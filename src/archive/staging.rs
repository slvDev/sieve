//! Private staging with bounded transfers and publication after manifest checks.
use super::{manifest::Archive, runtime::ArchiveConfig};
use eyre::{ensure, eyre, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::sync::watch;

pub(super) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let temp = path.with_extension("tmp");
    regular_or_missing(&temp)?;
    let mut file = File::create(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temp, path)?;
    File::open(path.parent().ok_or_else(|| eyre!("missing parent"))?)?.sync_all()?;
    Ok(())
}

fn regular_or_missing(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(m) => {
            ensure!(
                m.is_file(),
                "staging path is not a regular file: {}",
                path.display()
            );
            Ok(true)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}

fn directory(path: &Path) -> Result<()> {
    if path.exists() {
        ensure!(
            fs::symlink_metadata(path)?.is_dir(),
            "staging directory is a symlink or non-directory"
        );
    } else {
        fs::create_dir(path)?;
    }
    Ok(())
}

#[derive(Default, Serialize, Deserialize)]
struct TransferLedger {
    reserved_bytes: u64,
}

pub(super) struct Staging {
    pub root: PathBuf,
    _lock: File,
    config: ArchiveConfig,
    stop: watch::Receiver<bool>,
    legacy_layout: bool,
}

impl Staging {
    pub fn open(
        config: ArchiveConfig,
        identity: &[u8],
        manifest: &[u8],
        stop: watch::Receiver<bool>,
    ) -> Result<Self> {
        if !config.staging_dir.exists() {
            fs::create_dir_all(&config.staging_dir)?;
        }
        directory(&config.staging_dir)?;
        let root = fs::canonicalize(&config.staging_dir)?;
        let lock_path = root.join(".lock");
        regular_or_missing(&lock_path)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        lock.try_lock_exclusive()
            .map_err(|e| eyre!("archive staging is already in use: {e}"))?;
        let identity_path = root.join("identity.json");
        if regular_or_missing(&identity_path)? {
            ensure!(
                fs::read(&identity_path)? == identity,
                "staging belongs to a different archive job"
            );
        } else {
            // Only an interrupted identity write may precede the ownership marker.
            for entry in fs::read_dir(&root)? {
                let name = entry?.file_name();
                ensure!(
                    name == ".lock" || name == "identity.tmp",
                    "refusing to adopt a nonempty staging directory"
                );
            }
            atomic_write(&identity_path, identity)?;
        }
        let pinned = root.join("manifest.json");
        if regular_or_missing(&pinned)? {
            ensure!(fs::read(&pinned)? == manifest, "retained manifest differs");
        } else {
            atomic_write(&pinned, manifest)?;
        }
        let identity_value: serde_json::Value = serde_json::from_slice(identity)?;
        let legacy_layout = identity_value.get("group").is_some();
        Ok(Self {
            legacy_layout,
            root,
            _lock: lock,
            config,
            stop,
        })
    }

    pub fn check(&self, additional: u64) -> Result<()> {
        ensure!(
            !*self.stop.borrow(),
            "archive import cancelled; rerun to resume"
        );
        ensure!(
            fs2::available_space(&self.root)?
                >= self
                    .config
                    .min_free_bytes
                    .checked_add(additional)
                    .ok_or_else(|| eyre!("disk budget overflow"))?,
            "archive free-space reserve would be exceeded"
        );
        Ok(())
    }

    fn key<'a>(&self, archive: &'a Archive) -> &'a str {
        if self.legacy_layout {
            archive.component
        } else {
            archive
                .url
                .rsplit('/')
                .next()
                .unwrap_or(archive.component)
                .trim_end_matches(".tar.zst")
        }
    }

    pub fn component_dir(&self, archive: &Archive) -> PathBuf {
        self.root.join(self.key(archive))
    }

    /// Count actual files, including leftovers and metadata, without following links.
    pub fn usage(&self) -> Result<u64> {
        fn size(path: &Path) -> Result<u64> {
            let mut total = 0u64;
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let kind = entry.file_type()?;
                ensure!(!kind.is_symlink(), "symlink in archive staging");
                let bytes = if kind.is_dir() {
                    size(&entry.path())?
                } else {
                    entry.metadata()?.len()
                };
                total = total
                    .checked_add(bytes)
                    .ok_or_else(|| eyre!("staging usage overflow"))?;
            }
            Ok(total)
        }
        size(&self.root)
    }

    fn check_capacity(&self, additional: u64) -> Result<()> {
        self.check(additional)?;
        let required = self
            .usage()?
            .checked_add(additional)
            .and_then(|n| n.checked_add(self.config.min_free_bytes))
            .ok_or_else(|| eyre!("staging budget overflow"))?;
        ensure!(
            required <= self.config.max_staging_bytes,
            "actual archive staging would exceed max_staging_bytes"
        );
        Ok(())
    }

    pub fn verify(&self, archive: &Archive) -> Result<()> {
        verify_files(&self.component_dir(archive), archive)
    }

    pub fn stage(&self, archive: &Archive) -> Result<()> {
        self.check(0)?;
        let output = self.component_dir(archive);
        if output.exists() {
            self.verify(archive)?;
            let compressed = self.root.join(format!("{}.tar.zst", self.key(archive)));
            if regular_or_missing(&compressed)? {
                fs::remove_file(compressed)?;
            }
            return Ok(());
        }
        let partial = self.root.join(format!("{}.partial", self.key(archive)));
        Self::discard_partial(&partial, archive)?;
        let compressed = self.root.join(format!("{}.tar.zst", self.key(archive)));
        // No HTTP validator is persisted: every interrupted transfer restarts.
        if regular_or_missing(&compressed)? {
            fs::remove_file(&compressed)?;
        }
        let mut last = None;
        for attempt in 0..=self.config.retries {
            self.check_capacity(archive.compressed_bytes + archive.extracted_bytes)?;
            match self.download(archive, &compressed) {
                Ok(()) => {
                    last = None;
                    break;
                }
                Err(err) => {
                    last = Some(err);
                    if regular_or_missing(&compressed)? {
                        fs::remove_file(&compressed)?;
                    }
                    self.check(0)?;
                    if attempt < self.config.retries {
                        tracing::warn!(
                            component = archive.component,
                            attempt,
                            "archive download failed; restarting transfer"
                        );
                    }
                }
            }
        }
        if let Some(error) = last {
            return Err(error);
        }
        self.extract(archive, &compressed, &partial)?;
        self.report_extraction(archive)?;
        fs::rename(&partial, &output)?;
        File::open(&self.root)?.sync_all()?;
        fs::remove_file(compressed)?;
        tracing::info!(
            component = archive.component,
            "archive component verified and staged"
        );
        Ok(())
    }

    fn report_extraction(&self, archive: &Archive) -> Result<()> {
        tracing::info!(
            component = archive.component,
            staging_bytes = self.usage()?,
            "archive extracted and verified"
        );
        Ok(())
    }

    fn download(&self, archive: &Archive, path: &Path) -> Result<()> {
        let ledger_path = self.root.join("transfer.json");
        let mut ledger: TransferLedger = if regular_or_missing(&ledger_path)? {
            serde_json::from_slice(&fs::read(&ledger_path)?)?
        } else {
            TransferLedger::default()
        };
        ledger.reserved_bytes = ledger
            .reserved_bytes
            .checked_add(archive.compressed_bytes)
            .ok_or_else(|| eyre!("transfer count overflow"))?;
        ensure!(
            ledger.reserved_bytes <= self.config.max_download_bytes,
            "archive lifetime transfer budget exhausted; increase max_download_bytes to retry"
        );
        // Charge a full attempt before sending HTTP, including interrupted attempts.
        atomic_write(&ledger_path, &serde_json::to_vec(&ledger)?)?;
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(self.config.timeout_secs))
            .build()?;
        let mut response = client
            .get(&archive.url)
            .header(reqwest::header::ACCEPT_ENCODING, "identity")
            .send()?
            .error_for_status()?;
        ensure!(
            response.status() == reqwest::StatusCode::OK,
            "archive server did not return a complete response"
        );
        if let Some(length) = response.content_length() {
            ensure!(
                length == archive.compressed_bytes,
                "archive Content-Length mismatch"
            );
        }
        let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
        let mut remaining = archive.compressed_bytes;
        let mut buffer = vec![0u8; 65536];
        while remaining > 0 {
            self.check(buffer.len() as u64)?;
            let capacity = remaining.min(buffer.len() as u64) as usize;
            let n = response.read(&mut buffer[..capacity])?;
            ensure!(n != 0, "truncated archive download");
            file.write_all(&buffer[..n])?;
            remaining -= n as u64;
        }
        ensure!(
            response.read(&mut buffer[..1])? == 0,
            "archive exceeds manifest size"
        );
        file.sync_all()?;
        Ok(())
    }

    pub(super) fn extract(&self, archive: &Archive, source: &Path, partial: &Path) -> Result<()> {
        directory(partial)?;
        directory(&partial.join("static_files"))?;
        let mut decoder = zstd::Decoder::new(File::open(source)?)?;
        decoder.window_log_max(27)?;
        let limit = archive
            .extracted_bytes
            .checked_add(1_048_577)
            .ok_or_else(|| eyre!("extraction limit overflow"))?;
        let mut tar = tar::Archive::new(decoder.take(limit));
        let mut seen = BTreeSet::new();
        for entry in tar.entries()?.raw(true) {
            let mut entry = entry?;
            let path = entry.path()?.into_owned();
            if entry.header().entry_type().is_dir() {
                ensure!(
                    path == Path::new(".") || path == Path::new("static_files"),
                    "unexpected archive directory"
                );
                continue;
            }
            ensure!(
                entry.header().entry_type().is_file(),
                "archive links and special files are forbidden"
            );
            let expected = archive
                .files
                .iter()
                .find(|f| Path::new(&f.path) == path)
                .ok_or_else(|| eyre!("unexpected archive path: {}", path.display()))?;
            ensure!(
                seen.insert(expected.path.clone()) && entry.size() == expected.size,
                "duplicate or incorrectly sized archive file"
            );
            let mut output = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(partial.join(&path))?;
            let mut hash = blake3::Hasher::new();
            let mut buffer = vec![0u8; 65536];
            loop {
                self.check(buffer.len() as u64)?;
                let n = entry.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                hash.update(&buffer[..n]);
                output.write_all(&buffer[..n])?;
            }
            ensure!(
                hash.finalize().to_hex().as_str() == expected.blake3,
                "archive file checksum mismatch"
            );
            output.sync_all()?;
        }
        let mut decoder = tar.into_inner();
        let mut buffer = vec![0u8; 65536];
        while decoder.read(&mut buffer)? != 0 {
            self.check(0)?;
        }
        ensure!(
            decoder.limit() > 0 && seen.len() == archive.files.len(),
            "incomplete or oversized tar stream"
        );
        File::open(partial.join("static_files"))?.sync_all()?;
        File::open(partial)?.sync_all()?;
        Ok(())
    }

    fn discard_partial(partial: &Path, archive: &Archive) -> Result<()> {
        if !partial.exists() {
            return Ok(());
        }
        validate_tree(partial, archive)?;
        for expected in &archive.files {
            let path = partial.join(&expected.path);
            if regular_or_missing(&path)? {
                fs::remove_file(path)?;
            }
        }
        let files = partial.join("static_files");
        if files.exists() {
            fs::remove_dir(files)?;
        }
        fs::remove_dir(partial)?;
        Ok(())
    }

    pub fn cleanup(&self, archive: &Archive) -> Result<()> {
        ensure!(
            archive.component != "headers",
            "header evidence must be retained"
        );
        let root = self.component_dir(archive);
        if root.exists() {
            validate_tree(&root, archive)?;
            // Missing files are expected after an interrupted cleanup. Changed
            // files are never removed, even inside this job's private directory.
            for file in &archive.files {
                let path = root.join(&file.path);
                if regular_or_missing(&path)? {
                    verify_file(&path, file.size, &file.blake3)?;
                    fs::remove_file(path)?;
                }
            }
            let files = root.join("static_files");
            if files.exists() {
                fs::remove_dir(files)?;
            }
            fs::remove_dir(root)?;
        }
        Self::discard_partial(
            &self.root.join(format!("{}.partial", self.key(archive))),
            archive,
        )?;
        let compressed = self.root.join(format!("{}.tar.zst", self.key(archive)));
        if regular_or_missing(&compressed)? {
            fs::remove_file(compressed)?;
        }
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
}

fn validate_tree(root: &Path, archive: &Archive) -> Result<()> {
    ensure!(
        fs::symlink_metadata(root)?.is_dir(),
        "staging root is not a directory"
    );
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        ensure!(
            entry.file_name() == "static_files" && entry.file_type()?.is_dir(),
            "unexpected staging entry"
        );
        for child in fs::read_dir(entry.path())? {
            let child = child?;
            ensure!(
                child.file_type()?.is_file()
                    && archive
                        .files
                        .iter()
                        .any(|f| root.join(&f.path) == child.path()),
                "unexpected staging file"
            );
        }
    }
    Ok(())
}

fn verify_file(path: &Path, size: u64, hash: &str) -> Result<()> {
    ensure!(regular_or_missing(path)?, "missing staged file");
    let mut input = File::open(path)?;
    ensure!(input.metadata()?.len() == size, "staged file size mismatch");
    let mut digest = blake3::Hasher::new();
    std::io::copy(&mut input, &mut digest)?;
    ensure!(
        digest.finalize().to_hex().as_str() == hash,
        "staged file checksum mismatch"
    );
    Ok(())
}

fn verify_files(root: &Path, archive: &Archive) -> Result<()> {
    validate_tree(root, archive)?;
    for file in &archive.files {
        verify_file(&root.join(&file.path), file.size, &file.blake3)?;
    }
    Ok(())
}
