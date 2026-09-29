use super::{
    plan::{self, Plan},
    reader::{self, HeaderReader},
    staging::Staging,
    PlanArgs, MAX_MANIFEST_BYTES,
};
use crate::{
    chain::{BaseChain, ChainKind},
    db,
    sync::{
        canonical::{self, FrontierStatus, CANONICAL_SEGMENT_BLOCKS},
        ingestion::{IngestionContext, IngestionPipeline},
        validation::{
            verify_archive_recovery, ArchiveEvidence, ArchiveRecoveryReader, AuthenticatedSegment,
        },
        FetchItem,
    },
};
use alloy_primitives::B256;
use eyre::{ensure, eyre, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::mpsc;

/// Explicit finite, one-group bootstrap. Paths are relative to the root config.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveConfig {
    pub manifest: PathBuf,
    pub manifest_sha256: String,
    pub end_block: u64,
    pub checkpoint_hash: B256,
    pub staging_dir: PathBuf,
    pub max_staging_bytes: u64,
    pub max_download_bytes: u64,
    #[serde(default = "default_reserve")]
    pub min_free_bytes: u64,
    #[serde(default = "default_working")]
    pub working_space_bytes: u64,
    #[serde(default = "default_retries")]
    pub retries: u32,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    #[serde(default = "default_transactions")]
    pub max_transactions_per_block: u32,
}
const fn default_reserve() -> u64 {
    5_368_709_120
}
const fn default_working() -> u64 {
    1_073_741_824
}
const fn default_retries() -> u32 {
    2
}
const fn default_timeout() -> u64 {
    600
}
const fn default_transactions() -> u32 {
    100_000
}

#[derive(Debug)]
pub struct PreparedImport {
    config: ArchiveConfig,
    plan: Plan,
    manifest: Vec<u8>,
    identity: String,
    evidence: ArchiveEvidence,
}

impl PreparedImport {
    /// Validate the entire job before connecting to PostgreSQL or downloading.
    pub fn new(
        mut config: ArchiveConfig,
        config_dir: &Path,
        chain: ChainKind,
        start: u64,
        cli_end: Option<u64>,
        fingerprint: &str,
    ) -> Result<Self> {
        ensure!(
            chain == ChainKind::Base,
            "archive import is supported only for Base"
        );
        ensure!(
            cli_end.is_none_or(|end| end == config.end_block),
            "--end-block must match archive.end_block (the trusted checkpoint)"
        );
        ensure!(
            config.timeout_secs > 0 && config.max_transactions_per_block > 0,
            "archive timeout and transaction limit must be positive"
        );
        config.manifest_sha256 =
            super::parse_sha256(&config.manifest_sha256).map_err(|e| eyre!(e))?;
        config.manifest = config_dir.join(&config.manifest);
        config.staging_dir = config_dir.join(&config.staging_dir);
        let file = File::open(&config.manifest)?;
        ensure!(
            file.metadata()?.is_file(),
            "manifest must be a regular file"
        );
        let mut manifest = Vec::new();
        file.take(MAX_MANIFEST_BYTES + 1)
            .read_to_end(&mut manifest)?;
        ensure!(
            manifest.len() as u64 <= MAX_MANIFEST_BYTES,
            "manifest exceeds 16 MiB"
        );
        let plan = plan::build(
            &manifest,
            &PlanArgs {
                manifest: config.manifest.clone(),
                manifest_sha256: config.manifest_sha256.clone(),
                start_block: start,
                end_block: config.end_block,
                checkpoint_hash: Some(config.checkpoint_hash),
                max_staging_bytes: Some(config.max_staging_bytes),
                working_space_bytes: config.working_space_bytes,
                min_free_bytes: config.min_free_bytes,
            },
        )?;
        ensure!(
            plan.groups.len() == 1,
            "phase 3 imports one aligned archive group; choose an end in the starting group"
        );
        ensure!(
            plan.resources.fits_staging_budget == Some(true),
            "archive group exceeds max_staging_bytes"
        );
        ensure!(
            plan.resources.total_download_bytes <= config.max_download_bytes,
            "archive group exceeds max_download_bytes"
        );
        let evidence = ArchiveEvidence {
            manifest_sha256: B256::from_slice(&Sha256::digest(&manifest)),
            genesis_hash: chain.genesis_hash(),
            first_block: plan.decode_dependency_range[0],
            anchor_block: config.end_block,
            anchor_hash: config.checkpoint_hash,
        };
        evidence.validate()?;
        let identity = serde_json::to_string(&serde_json::json!({
            "reader": "base-v2-aligned-compact-v1", "evidence": evidence,
            "range": plan.requested_range, "group": plan.groups[0].archive_range,
            "indexing_config_sha256": fingerprint,
        }))?;
        Ok(Self {
            config,
            plan,
            manifest,
            identity,
            evidence,
        })
    }

    pub async fn run(self, ctx: IngestionContext) -> Result<()> {
        let start = self.plan.requested_range[0];
        let end = self.plan.requested_range[1];
        db::archive_job::prepare(&ctx.db, &self.identity, &self.evidence, start, end).await?;
        let state = canonical::committed_frontier_status(&ctx.db).await?;
        let progress = db::archive_job::progress(&ctx.db).await?;
        let next = resume_block(&state, progress, start, &self.evidence)?;
        ensure!(
            next >= start && next <= end + 1,
            "configured archive range leaves a gap or precedes the database checkpoint"
        );
        let stop = ctx.stop_rx.clone();
        let staging_config = self.config.clone();
        let manifest = self.manifest;
        let identity = self.identity;
        let staging = Arc::new(
            tokio::task::spawn_blocking(move || {
                Staging::open(staging_config, identity.as_bytes(), &manifest, stop)
            })
            .await??,
        );
        let plan = Arc::new(self.plan);
        let stage = Arc::clone(&staging);
        let source_plan = Arc::clone(&plan);
        let evidence = self.evidence.clone();
        let reader = Arc::new(
            tokio::task::spawn_blocking(move || {
                stage.stage(&source_plan.groups[0].archives[0])?;
                HeaderReader::open(&stage.root, &source_plan.groups[0], evidence)
            })
            .await??,
        );
        if let FrontierStatus::Archive(frontier) = state {
            let retained = Arc::clone(&reader);
            tokio::task::spawn_blocking(move || {
                verify_archive_recovery(&frontier, retained.as_ref())
            })
            .await??;
        }
        // Authenticate the anchor before transferring payload components.
        let retained = Arc::clone(&reader);
        let evidence = self.evidence.clone();
        tokio::task::spawn_blocking(move || {
            AuthenticatedSegment::archive(evidence.clone(), retained.headers(&evidence)?, end, end)
        })
        .await??;
        if next <= end {
            let stage = Arc::clone(&staging);
            let source_plan = Arc::clone(&plan);
            tokio::task::spawn_blocking(move || {
                for archive in &source_plan.groups[0].archives[1..] {
                    stage.stage(archive)?;
                }
                Ok::<_, eyre::Report>(())
            })
            .await??;
            self::ingest(
                &ctx,
                &staging,
                plan.clone(),
                reader,
                self.evidence,
                next,
                self.config.max_transactions_per_block,
            )
            .await?;
        }
        ensure!(
            db::archive_job::progress(&ctx.db)
                .await?
                .is_some_and(|(block, _)| block == end),
            "archive stopped before requested end; rerun to resume"
        );
        let stage = Arc::clone(&staging);
        tokio::task::spawn_blocking(move || {
            for archive in &plan.groups[0].archives[1..] {
                stage.cleanup(archive)?;
            }
            Ok::<_, eyre::Report>(())
        })
        .await??;
        db::archive_job::mark_cleaned(&ctx.db).await?;
        tracing::info!(
            end,
            "archive import complete; payload scratch released, header evidence retained"
        );
        Ok(())
    }
}

fn resume_block(
    state: &FrontierStatus,
    progress: Option<(u64, B256)>,
    start: u64,
    evidence: &ArchiveEvidence,
) -> Result<u64> {
    Ok(match state {
        FrontierStatus::Fresh => {
            ensure!(
                progress.is_none(),
                "archive journal exists without committed frontier"
            );
            start
        }
        FrontierStatus::Committed { checkpoint, .. } => {
            ensure!(
                progress.is_none(),
                "archive progress disagrees with frontier provenance"
            );
            checkpoint + 1
        }
        FrontierStatus::Archive(frontier) => {
            ensure!(
                progress == Some((frontier.block, frontier.hash)),
                "archive journal disagrees with committed frontier"
            );
            ensure!(
                frontier.evidence == *evidence,
                "archive evidence changed on resume"
            );
            frontier.block + 1
        }
    })
}

async fn ingest(
    ctx: &IngestionContext,
    staging: &Arc<Staging>,
    plan: Arc<Plan>,
    reader: Arc<HeaderReader>,
    evidence: ArchiveEvidence,
    next: u64,
    max_transactions: u32,
) -> Result<()> {
    let (send, mut receive) = mpsc::channel(16);
    let stage = Arc::clone(staging);
    let end = plan.requested_range[1];
    let producer = tokio::task::spawn_blocking(move || {
        reader::scan(&stage.root, &plan.groups[0], max_transactions, |payload| {
            stage.check(0)?;
            if payload.header().number >= next {
                send.blocking_send(payload)
                    .map_err(|_| eyre!("archive consumer stopped"))?;
            }
            Ok(())
        })
    });
    let result = async {
        let mut start = next;
        while start <= end {
            staging.check(0)?;
            let tip = end.min(start + CANONICAL_SEGMENT_BLOCKS - 1);
            let retained = Arc::clone(&reader);
            let proof = evidence.clone();
            let segment = tokio::task::spawn_blocking(move || {
                AuthenticatedSegment::archive(proof.clone(), retained.headers(&proof)?, start, tip)
            })
            .await??;
            let pipeline = IngestionPipeline::<BaseChain>::start(ctx.clone(), segment).await?;
            let sender = pipeline.sender();
            let sent = async {
                for number in start..=tip {
                    staging.check(0)?;
                    let payload = receive
                        .recv()
                        .await
                        .ok_or_else(|| eyre!("archive reader ended at block {number}"))?;
                    ensure!(
                        payload.header().number == number,
                        "archive reader produced out-of-order payload"
                    );
                    sender
                        .send(FetchItem::Payload(Box::new(payload)))
                        .await
                        .map_err(|_| eyre!("archive ingestion stopped"))?;
                }
                Ok::<_, eyre::Report>(())
            }
            .await;
            drop(sender);
            let finished = pipeline.finish().await;
            sent?;
            finished?;
            start = tip + 1;
        }
        Ok::<_, eyre::Report>(())
    }
    .await;
    drop(receive);
    let produced = producer.await?;
    // Preserve reconstruction errors instead of replacing them with channel EOF.
    produced?;
    result
}

/// Canonical JSON maps make hash-map iteration irrelevant. Include ABI contents
/// as well as filters/mappings so changing a file in place cannot alter a resume.
pub fn config_fingerprint(config: &crate::toml_config::SieveConfig, dir: &Path) -> Result<String> {
    let mut abis = Vec::new();
    for contract in &config.contracts {
        abis.push(serde_json::from_slice::<serde_json::Value>(
            &std::fs::read(dir.join(&contract.abi))?,
        )?);
        if let Some(abi) = contract.factory.as_ref().and_then(|f| f.abi.as_ref()) {
            abis.push(serde_json::from_slice::<serde_json::Value>(
                &std::fs::read(dir.join(abi))?,
            )?);
        }
    }
    let mut value = serde_json::json!({"contracts": config.contracts, "transfers": config.transfers, "streams": config.streams, "abis": abis});
    // Other dependencies enable serde_json/preserve_order. Sort recursively
    // rather than relying on the map implementation's default ordering.
    value.sort_all_objects();
    let bytes = serde_json::to_vec(&value)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
