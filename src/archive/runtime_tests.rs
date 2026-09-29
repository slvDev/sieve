//! Real jars, tar streams, and PostgreSQL exercise the source adapter end to end.
#![expect(clippy::panic_in_result_fn, reason = "test assertions")]
use super::*;
use crate::sync::{
    ingestion_tests,
    validation::{AuthenticatedSegment, ValidatedPayload},
    BlockPayload,
};
use reth_codecs::Compact;
use reth_nippy_jar::{NippyJar, NippyJarWriter};
use reth_static_file_types::{SegmentHeader, SegmentRangeInclusive, StaticFileSegment};
use serde_json::{json, Value};
use std::{
    fs,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::sync::watch;

struct Fixture {
    root: PathBuf,
    config: ArchiveConfig,
    payloads: Vec<BlockPayload<BaseChain>>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn compact_bytes<T: Compact>(value: &T) -> Vec<u8> {
    let mut bytes = Vec::new();
    value.to_compact(&mut bytes);
    bytes
}

impl Fixture {
    fn new() -> Result<Self> {
        Self::with_width(5)
    }

    fn with_width(width: u64) -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "sieve-phase3-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root)?;
        let mut parent = B256::repeat_byte(3);
        let payloads = ingestion_tests::fixture()?
            .into_iter()
            .map(|p| {
                let mut h = p.header().clone();
                h.parent_hash = parent;
                h.gas_used = if p.receipts().is_empty() { 0 } else { 21_000 };
                parent = h.hash_slow();
                BlockPayload::new(h, p.body().clone(), p.receipts().to_vec())
            })
            .collect::<Vec<_>>();
        let mut components = serde_json::Map::new();
        for (name, kind) in [
            ("headers", StaticFileSegment::Headers),
            ("transactions", StaticFileSegment::Transactions),
            ("receipts", StaticFileSegment::Receipts),
        ] {
            components.insert(
                name.into(),
                fixture_component(&root, &payloads, name, kind, width)?,
            );
        }
        let raw = serde_json::to_vec(
            &json!({"block":104,"chain_id":8453,"storage_version":2,"reth_version":crate::archive::manifest::PRODUCER,"base_url":"https://fixture.invalid","components":components}),
        )?;
        fs::write(root.join("manifest.json"), &raw)?;
        let config = ArchiveConfig {
            manifest: root.join("manifest.json"),
            manifest_sha256: format!("{:x}", Sha256::digest(&raw)),
            end_block: 104,
            checkpoint_hash: parent,
            staging_dir: root.join("stage"),
            max_staging_bytes: 100_000_000,
            max_download_bytes: 100_000_000,
            min_free_bytes: 0,
            working_space_bytes: 1_000_000,
            retries: 0,
            timeout_secs: 2,
            max_transactions_per_block: 100,
        };
        Ok(Self {
            root,
            config,
            payloads,
        })
    }
    fn import(&self, start: u64) -> Result<PreparedImport> {
        PreparedImport::new(
            self.config.clone(),
            Path::new("."),
            ChainKind::Base,
            start,
            None,
            "fixture",
        )
    }
    fn stage(&self, import: &PreparedImport) -> Result<Staging> {
        let (_tx, rx) = watch::channel(false);
        let stage = Staging::open(
            self.config.clone(),
            import.identity.as_bytes(),
            &import.manifest,
            rx,
        )?;
        for archive in import.plan.groups.iter().flat_map(|g| &g.archives) {
            let target = stage.component_dir(archive);
            fs::create_dir_all(target.join("static_files"))?;
            for file in &archive.files {
                fs::copy(
                    self.root
                        .join("source")
                        .join(archive.component)
                        .join(&file.path),
                    target.join(&file.path),
                )?;
            }
        }
        Ok(stage)
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "constructs each fixture component and its manifest metadata"
)]
fn fixture_component(
    root: &Path,
    payloads: &[BlockPayload<BaseChain>],
    name: &str,
    kind: StaticFileSegment,
    width: u64,
) -> Result<Value> {
    let dir = root.join("source").join(name);
    fs::create_dir_all(dir.join("static_files"))?;
    let mut outputs = Vec::new();
    let mut sizes = Vec::new();
    let mut extracted = Vec::new();
    let mut tx = 50;
    for start in (100..=104).step_by(width as usize) {
        let end = start + width - 1;
        let relative = format!("static_files/static_file_{name}_{start}_{end}");
        let mut rows = vec![Vec::<Vec<u8>>::new(); kind.columns()];
        for p in payloads
            .iter()
            .filter(|p| (start..=end).contains(&p.header().number))
        {
            match kind {
                StaticFileSegment::Headers => {
                    rows[0].push(compact_bytes(p.header()));
                    rows[1].push(vec![0]);
                    rows[2].push(p.header().hash_slow().to_vec());
                }
                StaticFileSegment::Transactions => {
                    rows[0].extend(p.body().transactions.iter().map(compact_bytes));
                }
                StaticFileSegment::Receipts => {
                    rows[0].extend(p.receipts().iter().map(compact_bytes));
                }
                _ => return Err(eyre!("unexpected fixture segment")),
            }
        }
        let tx_range = (name != "headers" && !rows[0].is_empty())
            .then(|| SegmentRangeInclusive::new(tx, tx + rows[0].len() as u64 - 1));
        if name != "headers" {
            tx += rows[0].len() as u64;
        }
        let header = SegmentHeader::new(
            SegmentRangeInclusive::new(start, end),
            Some(SegmentRangeInclusive::new(start, end.min(104))),
            tx_range,
            kind,
        );
        let mut writer =
            NippyJarWriter::new(NippyJar::new(kind.columns(), &dir.join(&relative), header))?;
        let columns = rows
            .iter()
            .map(|col| {
                col.iter()
                    .map(|r| Ok::<&[u8], Box<dyn std::error::Error + Send + Sync>>(r.as_slice()))
            })
            .collect::<Vec<_>>();
        writer.append_rows(columns, rows[0].len() as u64)?;
        writer.commit()?;
        drop(writer);
        let files = ["", ".conf", ".off"].iter().map(|suffix| -> Result<Value> {
            let path = format!("{relative}{suffix}");
            let bytes = fs::read(dir.join(&path))?;
            Ok(json!({"path":path,"size":bytes.len(),"blake3":blake3::hash(&bytes).to_hex().as_str()}))
        }).collect::<Result<Vec<_>>>()?;
        let path = root.join(format!("{name}-{start}-{end}.tar.zst"));
        let encoder = zstd::Encoder::new(File::create(&path)?, 1)?;
        let mut tar = tar::Builder::new(encoder);
        for file in &files {
            let path = file["path"].as_str().ok_or_else(|| eyre!("path"))?;
            tar.append_path_with_name(dir.join(path), path)?;
        }
        tar.into_inner()?.finish()?;
        if width == 5 {
            fs::copy(&path, root.join(format!("{name}.tar.zst")))?;
        }
        sizes.push(fs::metadata(path)?.len());
        extracted.push(files.iter().filter_map(|v| v["size"].as_u64()).sum::<u64>());
        outputs.push(files);
    }
    let mut all_outputs = Vec::new();
    let mut all_sizes = Vec::new();
    let mut all_extracted = Vec::new();
    for i in 0..=104 / width {
        let (files, size, extract) = if i < 100 / width {
            (
                outputs[0]
                    .iter()
                    .cloned()
                    .map(|mut f| {
                        f["path"] = Value::String(f["path"].as_str().unwrap_or("").replace(
                            &format!("100_{}", 100 + width - 1),
                            &format!("{}_{}", i * width, i * width + width - 1),
                        ));
                        f
                    })
                    .collect(),
                sizes[0],
                extracted[0],
            )
        } else {
            let i = (i - 100 / width) as usize;
            (outputs[i].clone(), sizes[i], extracted[i])
        };
        all_outputs.push(files);
        all_sizes.push(size);
        all_extracted.push(extract);
    }
    Ok(json!({"blocks_per_file":width,"total_blocks":105,
        "chunk_files": (0..=104/width).map(|i| format!("static_files/{name}-{}-{}.tar.zst",i*width,i*width+width-1)).collect::<Vec<_>>(),
        "chunk_sizes":all_sizes,"chunk_decompressed_sizes":all_extracted,"chunk_output_files":all_outputs}))
}

#[test]
fn real_jars_decode_boundaries_and_authenticate_anchor() -> Result<()> {
    let fixture = Fixture::new()?;
    let import = fixture.import(101)?;
    let stage = fixture.stage(&import)?;
    let reader = HeaderReader::open(&stage, &import.plan.groups, import.evidence.clone())?;
    let segment = AuthenticatedSegment::archive(
        import.evidence.clone(),
        reader.headers(&import.evidence)?,
        101,
        104,
    )?;
    assert_eq!(segment.start(), 101);
    let mut seen = Vec::new();
    reader::scan(&stage, &import.plan.groups[0], 100, |payload| {
        seen.push(payload.header().hash_slow());
        Ok(())
    })?;
    assert_eq!(
        seen,
        fixture
            .payloads
            .iter()
            .map(|p| p.header().hash_slow())
            .collect::<Vec<_>>()
    );
    let mut wrong = import.evidence.clone();
    wrong.anchor_hash = B256::ZERO;
    assert!(
        AuthenticatedSegment::archive(wrong, reader.headers(&import.evidence)?, 101, 104).is_err()
    );
    assert!(reader::scan(&stage, &import.plan.groups[0], 0, |_| Ok(())).is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated SIEVE_PHASE2_DATABASE_URL; run serially"]
async fn resumed_archive_matches_uninterrupted_output_and_retains_evidence() -> Result<()> {
    let mut expected = None;
    for interrupted in [false, true] {
        let fixture = Fixture::new()?;
        let import = fixture.import(100)?;
        drop(fixture.stage(&import)?);
        let (ctx, _notifications, _live) = ingestion_tests::context(false).await?;
        if interrupted {
            db::archive_job::prepare(&ctx.db, &import.identity, &import.evidence, 100, 104).await?;
            let headers = fixture.payloads.iter().map(|p| Ok(p.header().clone()));
            let segment =
                AuthenticatedSegment::archive(import.evidence.clone(), headers, 100, 100)?;
            let pipeline = IngestionPipeline::start(ctx.clone(), segment).await?;
            let send = pipeline.sender();
            let p = &fixture.payloads[0];
            send.send(FetchItem::Payload(Box::new(
                ValidatedPayload::new(BlockPayload::new(
                    p.header().clone(),
                    p.body().clone(),
                    p.receipts().to_vec(),
                ))
                .map_err(|e| eyre!(e))?,
            )))
            .await
            .map_err(|_| eyre!("closed"))?;
            drop(send);
            pipeline.finish().await?;
            assert_eq!(
                db::archive_job::progress(&ctx.db).await?.map(|p| p.0),
                Some(100)
            );
            // Rebuild dynamic children just as a process restart does.
            ctx.config
                .replace_factory_children(std::collections::HashMap::new());
            db::load_factory_children(&ctx.db, &ctx.config, &ctx.factories).await?;
        }
        import.run(ctx.clone()).await?;
        let rows: Vec<(i64, Vec<u8>, String)> = sqlx::query_as(
            "SELECT block_number, block_hash, value FROM phase2_events ORDER BY block_number",
        )
        .fetch_all(ctx.db.pool())
        .await?;
        assert_eq!(rows.len(), 2);
        if let Some(expected) = &expected {
            assert_eq!(&rows, expected);
        } else {
            expected = Some(rows);
        }
        assert_eq!(
            ctx.db
                .last_checkpoint()
                .await?
                .map(crate::types::BlockNumber::as_u64),
            Some(104)
        );
        assert!(fixture.config.staging_dir.join("headers").exists());
        assert!(!fixture.config.staging_dir.join("transactions").exists());
        // Completed restart authenticates retained evidence and needs no payload files.
        fixture.import(100)?.run(ctx.clone()).await?;
        let cleaned: bool = sqlx::query_scalar("SELECT cleaned FROM _sieve_archive_job")
            .fetch_one(ctx.db.pool())
            .await?;
        assert!(cleaned);
        let mut conflict = fixture.import(100)?;
        conflict.identity.push(' ');
        assert!(conflict.run(ctx).await.is_err());
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated SIEVE_PHASE2_DATABASE_URL; run serially"]
async fn failed_archive_write_does_not_advance_job_and_retries_cleanly() -> Result<()> {
    let fixture = Fixture::new()?;
    let import = fixture.import(100)?;
    drop(fixture.stage(&import)?);
    let (successful, _, _) = ingestion_tests::context(false).await?;
    let (ctx, _, _) = ingestion_tests::context(true).await?;
    assert!(import.run(ctx.clone()).await.is_err());
    assert!(db::archive_job::progress(&ctx.db).await?.is_none());
    assert!(fixture.config.staging_dir.join("transactions").exists());
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM phase2_events")
        .fetch_one(ctx.db.pool())
        .await?;
    assert_eq!(rows, 0);
    fixture.import(100)?.run(successful.clone()).await?;
    assert_eq!(
        db::archive_job::progress(&successful.db)
            .await?
            .map(|p| p.0),
        Some(104)
    );
    Ok(())
}

#[test]
fn safe_extraction_detects_corruption_and_cleanup_preserves_changed_files() -> Result<()> {
    let fixture = Fixture::new()?;
    let import = fixture.import(100)?;
    let (_tx, rx) = watch::channel(false);
    let stage = Staging::open(
        fixture.config.clone(),
        import.identity.as_bytes(),
        &import.manifest,
        rx,
    )?;
    let archive = &import.plan.groups[0].archives[0];
    let source = fixture.root.join("headers.tar.zst");
    let partial = stage.root.join("headers.partial");
    stage.extract(archive, &source, &partial)?;
    fs::rename(partial, stage.component_dir(archive))?;
    stage.verify(archive)?;
    let mut bytes = fs::read(&source)?;
    bytes.truncate(bytes.len() / 2);
    fs::write(&source, bytes)?;
    assert!(stage
        .extract(archive, &source, &stage.root.join("corrupt.partial"))
        .is_err());
    drop(stage);
    // Seed the remaining owned components, then simulate a partially completed cleanup.
    let stage = fixture.stage(&import)?;
    let archive = &import.plan.groups[0].archives[1];
    let missing = stage.component_dir(archive).join(&archive.files[0].path);
    fs::remove_file(missing)?;
    let changed = stage.component_dir(archive).join(&archive.files[1].path);
    fs::write(&changed, b"externally modified")?;
    assert!(stage.cleanup(archive).is_err());
    assert_eq!(fs::read(&changed)?, b"externally modified");
    fs::copy(
        fixture
            .root
            .join("source/transactions")
            .join(&archive.files[1].path),
        &changed,
    )?;
    stage.cleanup(archive)?;
    stage.cleanup(archive)?;
    assert!(stage.root.join("headers").exists());
    Ok(())
}

#[test]
fn staging_refuses_adoption_symlinks_and_concurrent_jobs() -> Result<()> {
    let fixture = Fixture::new()?;
    let import = fixture.import(100)?;
    fs::create_dir_all(&fixture.config.staging_dir)?;
    fs::write(fixture.config.staging_dir.join("unowned"), b"keep")?;
    let (_tx, rx) = watch::channel(false);
    assert!(Staging::open(
        fixture.config.clone(),
        import.identity.as_bytes(),
        &import.manifest,
        rx.clone()
    )
    .is_err());
    fs::remove_file(fixture.config.staging_dir.join("unowned"))?;
    let stage = fixture.stage(&import)?;
    assert!(Staging::open(
        fixture.config.clone(),
        import.identity.as_bytes(),
        &import.manifest,
        rx
    )
    .is_err());
    #[cfg(unix)]
    {
        let archive = &import.plan.groups[0].archives[1];
        let path = stage.component_dir(archive).join(&archive.files[0].path);
        fs::remove_file(&path)?;
        std::os::unix::fs::symlink(fixture.root.join("manifest.json"), &path)?;
        assert!(stage.verify(archive).is_err());
        assert!(stage.cleanup(archive).is_err());
        assert!(fixture.root.join("manifest.json").exists());
    }
    Ok(())
}

#[test]
#[ignore = "requires localhost HTTP listener"]
fn interrupted_download_and_extraction_restart_with_durable_transfer_budget() -> Result<()> {
    use std::{
        io::{Read, Write},
        net::TcpListener,
    };
    let fixture = Fixture::new()?;
    let mut import = fixture.import(100)?;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    import.plan.groups[0].archives[0].url = format!("http://{}/headers", listener.local_addr()?);
    let bytes = fs::read(fixture.root.join("headers.tar.zst"))?;
    let size = bytes.len() as u64;
    let server = std::thread::spawn(move || -> Result<()> {
        for truncate in [true, false] {
            let (mut stream, _) = listener.accept()?;
            stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
            let mut request = [0; 4096];
            let _ = stream.read(&mut request)?;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                bytes.len()
            )?;
            stream.write_all(if truncate {
                &bytes[..bytes.len() / 2]
            } else {
                &bytes
            })?;
        }
        Ok(())
    });
    let (_tx, rx) = watch::channel(false);
    let mut config = fixture.config.clone();
    config.retries = 1;
    let stage = Staging::open(config, import.identity.as_bytes(), &import.manifest, rx)?;
    let archive = &import.plan.groups[0].archives[0];
    let partial = stage.root.join("headers.partial");
    fs::create_dir_all(partial.join("static_files"))?;
    fs::write(
        partial.join(&archive.files[0].path),
        b"interrupted extraction",
    )?;
    stage.stage(archive)?;
    server.join().map_err(|_| eyre!("HTTP fixture failed"))??;
    stage.verify(archive)?;
    let ledger: Value = serde_json::from_slice(&fs::read(stage.root.join("transfer.json"))?)?;
    assert_eq!(ledger["reserved_bytes"], 2 * size);
    assert!(!partial.exists());
    assert!(!stage.root.join("headers.tar.zst").exists());
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated SIEVE_PHASE2_DATABASE_URL; run serially"]
async fn non_genesis_start_does_not_index_decode_dependencies() -> Result<()> {
    let fixture = Fixture::new()?;
    let import = fixture.import(101)?;
    drop(fixture.stage(&import)?);
    let (mut ctx, _, _) = ingestion_tests::context(false).await?;
    ctx.factories = Arc::new(vec![]);
    ctx.config = Arc::new(crate::config::IndexConfig::new(vec![
        crate::config::ContractConfig::new(
            "Child",
            alloy_primitives::Address::repeat_byte(0x11),
            r#"[{"type":"event","name":"Ping","anonymous":false,"inputs":[{"name":"value","type":"uint256","indexed":false}]}]"#,
            &["Ping"],
        )?,
    ]));
    import.run(ctx.clone()).await?;
    let blocks: Vec<i64> =
        sqlx::query_scalar("SELECT block_number FROM phase2_events ORDER BY block_number")
            .fetch_all(ctx.db.pool())
            .await?;
    assert_eq!(blocks, vec![101]);
    assert!(ctx
        .db
        .get_block_hash(crate::types::BlockNumber::new(100))
        .await?
        .is_none());
    let url = std::env::var("SIEVE_PHASE2_DATABASE_URL")?;
    let lease = db::archive_job::writer_lease(&url).await?;
    assert!(db::archive_job::writer_lease(&url).await.is_err());
    sqlx::Connection::close(lease).await?;
    let lease = db::archive_job::writer_lease(&url).await?;
    sqlx::Connection::close(lease).await?;
    Ok(())
}

#[test]
fn indexing_fingerprint_is_stable_but_detects_abi_and_filter_changes() -> Result<()> {
    let fixture = Fixture::new()?;
    let abi_path = fixture.root.join("abi.json");
    fs::write(&abi_path, r#"[{"name":"Ping","type":"event","inputs":[]}]"#)?;
    let mut config: crate::toml_config::SieveConfig = serde_json::from_value(json!({
        "chain":"base", "contracts":[{"name":"Child","address":"0x1111111111111111111111111111111111111111","abi":"abi.json","start_block":100,
        "events":[{"name":"Ping","table":"pings","filter":{"a":["one"],"b":["two"]}}]}]
    }))?;
    let original = config_fingerprint(&config, &fixture.root)?;
    fs::write(&abi_path, r#"[{"inputs":[],"type":"event","name":"Ping"}]"#)?;
    assert_eq!(config_fingerprint(&config, &fixture.root)?, original);
    fs::write(
        &abi_path,
        r#"[{"inputs":[],"type":"event","name":"Other"}]"#,
    )?;
    assert_ne!(config_fingerprint(&config, &fixture.root)?, original);
    fs::write(&abi_path, r#"[{"name":"Ping","type":"event","inputs":[]}]"#)?;
    let filter = config.contracts[0].events[0]
        .filter
        .as_mut()
        .ok_or_else(|| eyre!("missing filter"))?;
    filter.insert("b".into(), vec!["changed".into()]);
    assert_ne!(config_fingerprint(&config, &fixture.root)?, original);
    Ok(())
}

/// Serve components on demand and assert that no earlier group's payload files
/// remain when the next payload request starts. A chosen component can fail to
/// simulate restart after a completed/cleaned group without test-only runtime hooks.
fn serve_groups(
    fixture: &Fixture,
    import: &mut PreparedImport,
    fail: Option<(u64, &'static str)>,
) -> Result<std::thread::JoinHandle<Result<Vec<String>>>> {
    use std::{
        io::{Read, Write},
        net::TcpListener,
    };
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?;
    let mut files = std::collections::HashMap::new();
    for group in &mut import.plan.groups {
        for archive in &mut group.archives {
            let key = format!(
                "{}-{}-{}.tar.zst",
                archive.component, group.archive_range[0], group.archive_range[1]
            );
            archive.url = format!("http://{address}/{key}");
            files.insert(
                key,
                (
                    fs::read(fixture.root.join(format!(
                        "{}-{}-{}.tar.zst",
                        archive.component, group.archive_range[0], group.archive_range[1]
                    )))?,
                    group.archive_range[0],
                    archive.component,
                ),
            );
        }
    }
    let stage_root = fixture.config.staging_dir.clone();
    let width = import.plan.groups[0].archive_range[1] - import.plan.groups[0].archive_range[0] + 1;
    Ok(std::thread::spawn(move || {
        let mut requests = Vec::new();
        let mut idle = std::time::Instant::now();
        loop {
            let (mut stream, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if idle.elapsed() > std::time::Duration::from_secs(2) {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            idle = std::time::Instant::now();
            stream.set_nonblocking(false)?;
            stream.set_read_timeout(Some(std::time::Duration::from_secs(2)))?;
            let mut buffer = [0u8; 4096];
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                let n = stream.read(&mut buffer)?;
                ensure!(
                    n > 0 && request.len() + n <= 16384,
                    "invalid fixture HTTP request"
                );
                request.extend_from_slice(&buffer[..n]);
            }
            let request = String::from_utf8_lossy(&request);
            let key = request
                .split_whitespace()
                .nth(1)
                .ok_or_else(|| eyre!("missing request path"))?
                .trim_start_matches('/');
            let (bytes, start, component) = files
                .get(key)
                .ok_or_else(|| eyre!("unplanned download {key}"))?;
            if *component != "headers" && *start > 100 {
                for previous in (100..*start).step_by(width as usize) {
                    for kind in ["transactions", "receipts"] {
                        ensure!(
                            !stage_root
                                .join(format!("{kind}-{previous}-{}", previous + width - 1))
                                .exists(),
                            "prior payload group still occupies staging"
                        );
                    }
                }
            }
            requests.push(key.to_owned());
            if fail == Some((*start, *component)) {
                stream.write_all(
                    b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )?;
                break;
            }
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                bytes.len()
            )?;
            stream.write_all(bytes)?;
        }
        Ok(requests)
    }))
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and localhost HTTP; run serially"]
async fn rolling_groups_resume_after_cleanup_and_match_uninterrupted_factory_output() -> Result<()>
{
    let mut expected = None;
    for interrupted in [false, true] {
        let fixture = Fixture::with_width(1)?;
        let (ctx, _, _) = ingestion_tests::context(false).await?;
        if interrupted {
            let mut first = fixture.import(100)?;
            let server = serve_groups(&fixture, &mut first, Some((101, "transactions")))?;
            assert!(first.run(ctx.clone()).await.is_err());
            server.join().map_err(|_| eyre!("server panicked"))??;
            assert_eq!(
                db::archive_job::progress(&ctx.db).await?.map(|p| p.0),
                Some(100)
            );
            let group = db::archive_job::group_progress(&ctx.db, 100)
                .await?
                .ok_or_else(|| eyre!("missing group"))?;
            assert!(group.completed && group.cleaned);
            assert_eq!(group.next_transaction, Some(51));
            ctx.config
                .replace_factory_children(std::collections::HashMap::new());
            db::load_factory_children(&ctx.db, &ctx.config, &ctx.factories).await?;
        }
        let mut import = fixture.import(100)?;
        let server = serve_groups(&fixture, &mut import, None)?;
        let result = import.run(ctx.clone()).await;
        let requests = server.join().map_err(|_| eyre!("server panicked"))??;
        result?;
        if interrupted {
            assert!(!requests
                .iter()
                .any(|r| r.starts_with("headers") || r.contains("-100-100")));
        }
        let rows: Vec<(i64, Vec<u8>, String)> = sqlx::query_as(
            "SELECT block_number, block_hash, value FROM phase2_events ORDER BY block_number",
        )
        .fetch_all(ctx.db.pool())
        .await?;
        assert_eq!(rows.len(), 2);
        if let Some(expected) = &expected {
            assert_eq!(&rows, expected);
        } else {
            expected = Some(rows);
        }
        let statuses: Vec<(i64, bool, bool, Option<i64>)> = sqlx::query_as("SELECT start_block, completed, cleaned, next_transaction FROM _sieve_archive_groups ORDER BY start_block").fetch_all(ctx.db.pool()).await?;
        assert_eq!(
            statuses,
            vec![
                (100, true, true, Some(51)),
                (101, true, true, Some(52)),
                (102, true, true, Some(52)),
                (103, true, true, Some(52)),
                (104, true, true, Some(52))
            ]
        );
        fixture.import(100)?.run(ctx).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and localhost HTTP; run serially"]
async fn rolling_partial_end_and_durable_transaction_continuity() -> Result<()> {
    let mut fixture = Fixture::with_width(2)?;
    fixture.config.end_block = 102;
    fixture.config.checkpoint_hash = fixture.payloads[2].header().hash_slow();
    let (ctx, _, _) = ingestion_tests::context(false).await?;
    let mut import = fixture.import(100)?;
    let server = serve_groups(&fixture, &mut import, None)?;
    import.run(ctx.clone()).await?;
    server.join().map_err(|_| eyre!("server panicked"))??;
    assert_eq!(
        db::archive_job::progress(&ctx.db).await?.map(|p| p.0),
        Some(102)
    );
    assert!(ctx
        .db
        .get_block_hash(crate::types::BlockNumber::new(103))
        .await?
        .is_none());
    let group = db::archive_job::group_progress(&ctx.db, 102)
        .await?
        .ok_or_else(|| eyre!("missing group"))?;
    assert_eq!(group.next_transaction, None); // partial group never claims a decoded next-group position
    let wrong = db::archive_job::GroupDescriptor {
        archive_range: [104, 105],
        index_range: [104, 104],
        available_end: 104,
        transaction_range: Some([99, 100]),
    };
    assert!(db::archive_job::prepare_group(&ctx.db, &wrong, Some(52))
        .await
        .is_err());
    Ok(())
}

#[test]
fn actual_staging_budget_refuses_transfer_before_any_request() -> Result<()> {
    let fixture = Fixture::with_width(2)?;
    let import = fixture.import(100)?;
    let (_tx, rx) = watch::channel(false);
    let mut config = fixture.config.clone();
    config.max_staging_bytes = 10_000;
    let stage = Staging::open(config, import.identity.as_bytes(), &import.manifest, rx)?;
    // A retained/leftover file also consumes the configured hard limit.
    File::create(stage.root.join("leftover"))?.set_len(10_000)?;
    let err = stage
        .stage(&import.plan.groups[0].archives[0])
        .err()
        .ok_or_else(|| eyre!("budget was not enforced"))?;
    assert!(err.to_string().contains("max_staging_bytes"));
    assert!(!stage.root.join("transfer.json").exists());
    Ok(())
}

#[test]
fn bounded_header_proofs_authenticate_resume_without_replaying_history() -> Result<()> {
    use alloy_consensus::Header;
    let mut parent = B256::repeat_byte(3);
    let headers = (100..8300)
        .map(|number| {
            let h = Header {
                number,
                parent_hash: parent,
                ..Default::default()
            };
            parent = h.hash_slow();
            h
        })
        .collect::<Vec<_>>();
    let evidence = ArchiveEvidence {
        first_block: 100,
        anchor_block: 8299,
        anchor_hash: parent,
        genesis_hash: ChainKind::Base.genesis_hash(),
        manifest_sha256: B256::repeat_byte(9),
    };
    let proof = AuthenticatedArchive::new(
        evidence.clone(),
        headers.iter().cloned().map(Ok),
        &std::collections::BTreeSet::new(),
    )?;
    assert_eq!(proof.segment_end(110, 8299)?, 8291);
    assert_eq!(proof.segment_end(8292, 8299)?, 8299);
    proof.segment(headers[10..8192].iter().cloned().map(Ok), 110, 8291)?;
    assert!(proof
        .segment(headers[8192..8199].iter().cloned().map(Ok), 8292, 8299)
        .is_err());
    let mut bad = headers[8192..].to_vec();
    bad[0].timestamp = 123;
    assert!(proof.segment(bad.into_iter().map(Ok), 8292, 8299).is_err());
    let mut wrong_anchor = evidence;
    wrong_anchor.anchor_hash = B256::ZERO;
    assert!(AuthenticatedArchive::new(
        wrong_anchor,
        headers.into_iter().map(Ok),
        &std::collections::BTreeSet::new()
    )
    .is_err());
    Ok(())
}
