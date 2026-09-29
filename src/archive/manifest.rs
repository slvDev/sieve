//! Manifest and path checks adapted from Shinode's snapshot/manifest.rs (MIT).
//! Only metadata is accepted here; payload compatibility is checked by the future reader.

use eyre::{ensure, eyre, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub(super) const PRODUCER: &str = "2.5.2-dev (76a8261)";
pub(super) const COMPONENTS: [&str; 3] = ["headers", "transactions", "receipts"];

#[derive(Debug, Deserialize)]
pub(super) struct Manifest {
    pub block: u64,
    chain_id: u64,
    storage_version: u64,
    pub reth_version: String,
    base_url: String,
    components: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Component {
    pub blocks_per_file: u64,
    total_blocks: u64,
    chunk_files: Vec<String>,
    chunk_sizes: Vec<u64>,
    chunk_decompressed_sizes: Vec<u64>,
    chunk_output_files: Vec<Vec<OutputFile>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct OutputFile {
    pub path: String,
    pub size: u64,
    pub blake3: String,
}

#[derive(Debug, Serialize)]
pub(super) struct Archive {
    pub component: &'static str,
    pub url: String,
    pub compressed_bytes: u64,
    pub extracted_bytes: u64,
    pub files: Vec<OutputFile>,
}

impl Manifest {
    pub fn parse(raw: &[u8], digest: &str) -> Result<Self> {
        ensure!(
            raw.len() as u64 <= super::MAX_MANIFEST_BYTES,
            "manifest exceeds 16 MiB"
        );
        ensure!(
            format!("{:x}", Sha256::digest(raw)) == digest,
            "manifest SHA-256 mismatch"
        );
        let manifest: Self = serde_json::from_slice(raw)?;
        ensure!(
            manifest.chain_id == 8453 && manifest.storage_version == 2,
            "expected Base mainnet V2 snapshot"
        );
        ensure!(
            manifest.reth_version == PRODUCER,
            "unsupported snapshot producer {}; expected {PRODUCER}",
            manifest.reth_version
        );
        // Every persisted Sieve block number must fit PostgreSQL BIGINT.
        ensure!(
            manifest.block < i64::MAX as u64,
            "unsupported snapshot height"
        );
        let base = reqwest::Url::parse(&manifest.base_url)?;
        ensure!(
            base.scheme() == "https"
                && base.has_host()
                && base.username().is_empty()
                && base.password().is_none()
                && base.query().is_none()
                && base.fragment().is_none(),
            "snapshot base URL must use HTTPS without credentials/query/fragment"
        );
        Ok(manifest)
    }

    pub fn component(&self, name: &str) -> Result<Component> {
        let component: Component = serde_json::from_value(
            self.components
                .get(name)
                .ok_or_else(|| eyre!("missing {name} component"))?
                .clone(),
        )?;
        let total = self.block + 1;
        ensure!(
            component.blocks_per_file > 0 && component.total_blocks == total,
            "{name}: pruned history or invalid chunk width"
        );
        let count = usize::try_from(total.div_ceil(component.blocks_per_file))?;
        ensure!(
            [
                component.chunk_files.len(),
                component.chunk_sizes.len(),
                component.chunk_decompressed_sizes.len(),
                component.chunk_output_files.len()
            ]
            .iter()
            .all(|len| *len == count),
            "{name}: incomplete chunk metadata"
        );
        Ok(component)
    }

    pub fn archive(
        &self,
        name: &'static str,
        component: &Component,
        index: usize,
    ) -> Result<Archive> {
        let [start, end] = component.range(index)?;
        let stem = format!("{name}-{start}-{end}");
        let path = &component.chunk_files[index];
        let (parent, filename) = path
            .split_once('/')
            .ok_or_else(|| eyre!("invalid chunk path"))?;
        ensure!(
            (parent == "static_files"
                || (!parent.is_empty() && parent.bytes().all(|b| b.is_ascii_digit())))
                && filename == format!("{stem}.tar.zst"),
            "unexpected chunk path: {path}"
        );
        let files = component.chunk_output_files[index].clone();
        let prefix = format!("static_files/static_file_{name}_{start}_{end}");
        ensure!(files.len() == 3, "{stem}: expected data/conf/off files");
        for suffix in ["", ".conf", ".off"] {
            ensure!(
                files
                    .iter()
                    .filter(|f| f.path == format!("{prefix}{suffix}"))
                    .count()
                    == 1,
                "{stem}: invalid output paths"
            );
        }
        for file in &files {
            ensure!(
                file.blake3.len() == 64
                    && file
                        .blake3
                        .bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
                "{stem}: invalid BLAKE3 checksum"
            );
        }
        let extracted = sum(files.iter().map(|file| file.size))?;
        ensure!(
            extracted == component.chunk_decompressed_sizes[index]
                && component.chunk_sizes[index] > 0,
            "{stem}: inconsistent archive sizes"
        );
        Ok(Archive {
            component: name,
            url: format!("{}/{}", self.base_url.trim_end_matches('/'), path),
            compressed_bytes: component.chunk_sizes[index],
            extracted_bytes: extracted,
            files,
        })
    }
}

impl Component {
    pub fn range(&self, index: usize) -> Result<[u64; 2]> {
        let start = (index as u64)
            .checked_mul(self.blocks_per_file)
            .ok_or_else(|| eyre!("chunk range overflow"))?;
        let end = start
            .checked_add(self.blocks_per_file - 1)
            .ok_or_else(|| eyre!("chunk range overflow"))?;
        Ok([start, end])
    }
}

pub(super) fn sum(values: impl IntoIterator<Item = u64>) -> Result<u64> {
    values.into_iter().try_fold(0u64, |total, value| {
        total
            .checked_add(value)
            .ok_or_else(|| eyre!("archive size overflow"))
    })
}
