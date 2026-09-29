//! A versioned, serializable contract between inspection and a future archive reader.

use super::{
    manifest::{sum, Archive, Manifest, COMPONENTS},
    PlanArgs,
};
use alloy_primitives::B256;
use eyre::{ensure, Result};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub(super) struct Plan {
    schema_version: u32,
    mode: &'static str,
    chain_id: u64,
    expected_genesis_hash: &'static str,
    manifest_sha256: String,
    producer: String,
    snapshot_height: u64,
    reader_contract: &'static str,
    pub requested_range: [u64; 2],
    pub decode_dependency_range: [u64; 2],
    dependency_reason: &'static str,
    pub groups: Vec<Group>,
    pub resources: Resources,
    pub trust: Trust,
    pub handoff: Handoff,
    limitations: [&'static str; 3],
}

/// Metadata needed to open aligned jars and reconstruct from a group boundary.
#[derive(Debug, Serialize)]
pub(super) struct Group {
    /// Full nominal range encoded in file names (tail may end earlier).
    pub archive_range: [u64; 2],
    pub available_range: [u64; 2],
    pub index_range: [u64; 2],
    pub decode_from: u64,
    pub archives: [Archive; 3],
    pub compressed_bytes: u64,
    pub extracted_bytes: u64,
}

#[derive(Debug, Serialize)]
pub(super) struct Resources {
    pub total_download_bytes: u64,
    pub total_extracted_bytes: u64,
    /// All extracted header jars retained as restart-verifiable evidence.
    pub retained_header_bytes: u64,
    pub largest_group_file_bytes: u64,
    pub peak_header_pass_file_bytes: u64,
    pub peak_payload_pass_file_bytes: u64,
    pub peak_staging_file_bytes: u64,
    pub working_space_bytes: u64,
    pub min_free_bytes: u64,
    pub required_staging_budget_bytes: u64,
    pub configured_staging_budget_bytes: Option<u64>,
    pub fits_staging_budget: Option<bool>,
    assumptions: [&'static str; 4],
}

#[derive(Debug, Serialize)]
pub(super) struct Trust {
    manifest_digest_verified: bool,
    pub anchor_block: u64,
    pub supplied_checkpoint_hash: Option<B256>,
    checkpoint_status: &'static str,
    pub required_before_import: Vec<&'static str>,
    evidence_strategy: &'static str,
}

#[derive(Debug, Serialize)]
pub(super) struct Handoff {
    pub candidate_archive_end: u64,
    pub candidate_p2p_start: u64,
    status: &'static str,
    required_before_transition: [&'static str; 3],
}

pub(super) fn build(raw: &[u8], args: &PlanArgs) -> Result<Plan> {
    let manifest = Manifest::parse(raw, &args.manifest_sha256)?;
    ensure!(
        args.start_block <= args.end_block,
        "requested end precedes start"
    );
    ensure!(
        args.end_block <= manifest.block,
        "requested range is outside snapshot"
    );
    let components = [
        manifest.component(COMPONENTS[0])?,
        manifest.component(COMPONENTS[1])?,
        manifest.component(COMPONENTS[2])?,
    ];
    let width = components[0].blocks_per_file;
    ensure!(
        components.iter().all(|c| c.blocks_per_file == width),
        "reader requires aligned component block ranges"
    );
    let mut groups = Vec::new();
    for index in
        usize::try_from(args.start_block / width)?..=usize::try_from(args.end_block / width)?
    {
        let archives = [
            manifest.archive(COMPONENTS[0], &components[0], index)?,
            manifest.archive(COMPONENTS[1], &components[1], index)?,
            manifest.archive(COMPONENTS[2], &components[2], index)?,
        ];
        let [start, end] = components[0].range(index)?;
        groups.push(Group {
            archive_range: [start, end],
            available_range: [start, end.min(manifest.block)],
            index_range: [start.max(args.start_block), end.min(args.end_block)],
            decode_from: start,
            compressed_bytes: sum(archives.iter().map(|c| c.compressed_bytes))?,
            extracted_bytes: sum(archives.iter().map(|c| c.extracted_bytes))?,
            archives,
        });
    }
    let resources = resources(&groups, args)?;
    let mut required_before_import = vec![
        "authenticate consecutive headers against the end checkpoint and existing database seam",
        "verify extracted file hashes and validate reconstructed Base payload commitments",
        "reconcile database checkpoint, indexing configuration, and factory coverage",
    ];
    if args.checkpoint_hash.is_none() {
        required_before_import.insert(
            0,
            "obtain an independently trusted end-block hash or canonical peer quorum",
        );
    }
    Ok(Plan {
        schema_version: 1,
        mode: "offline_metadata_only",
        chain_id: 8453,
        expected_genesis_hash: "0xf712aa9241cc24369b143cf6dce85f0902a9731e70d66818a3a5845b296c73dd",
        manifest_sha256: args.manifest_sha256.clone(),
        producer: manifest.reth_version,
        snapshot_height: manifest.block,
        reader_contract: "base-v2-aligned-compact-v1",
        requested_range: [args.start_block, args.end_block],
        decode_dependency_range: [args.start_block / width * width, args.end_block],
        dependency_reason: "reconstruct sequential transaction boundaries from first selected archive; do not index its earlier blocks",
        groups,
        resources,
        trust: Trust {
            manifest_digest_verified: true,
            anchor_block: args.end_block,
            supplied_checkpoint_hash: args.checkpoint_hash,
            checkpoint_status: if args.checkpoint_hash.is_some() { "supplied_not_verified" } else { "missing" },
            required_before_import,
            evidence_strategy: "retain selected header jars; verify their complete chain to the end checkpoint before payload commits; reverify on resume",
        },
        handoff: Handoff {
            candidate_archive_end: args.end_block,
            candidate_p2p_start: args.end_block + 1,
            status: "not_probed_no_availability_claim",
            required_before_transition: [
                "probe actual successor headers, bodies, and receipts with overlap; recheck after import",
                "authenticate P2P headers through Sieve quorum and match the committed parent hash",
                "stop on a history gap or conflict; use newer pinned coverage or a serving history peer",
            ],
        },
        limitations: [
            "metadata compatibility only; this command does not decode payloads, index data, or verify canonicality/finality",
            "real Shinode source evidence covers early pre-Canyon blocks; recent/fork payload compatibility needs separate validation",
            "no database/configuration inspection, free-disk probe, archive transfer, extraction, RPC request, or P2P connection",
        ],
    })
}

fn resources(groups: &[Group], args: &PlanArgs) -> Result<Resources> {
    let retained_headers = sum(groups.iter().map(|g| g.archives[0].extracted_bytes))?;
    // Headers are downloaded one at a time, extracted, and their archives released.
    // Retain all extracted jars for authentication/restart; this upper bound also
    // covers a large earlier header archive while fewer jars have been retained.
    let header_archive = groups
        .iter()
        .map(|g| g.archives[0].compressed_bytes)
        .max()
        .unwrap_or(0);
    let peak_headers = sum([retained_headers, header_archive])?;
    let mut largest_group = 0;
    let mut largest_payload = 0;
    for group in groups {
        largest_group = largest_group.max(sum([group.compressed_bytes, group.extracted_bytes])?);
        let payload = sum(group.archives[1..]
            .iter()
            .flat_map(|a| [a.compressed_bytes, a.extracted_bytes]))?;
        largest_payload = largest_payload.max(payload);
    }
    let peak_payload = sum([retained_headers, largest_payload])?;
    let peak_files = peak_headers.max(peak_payload);
    let required = sum([peak_files, args.working_space_bytes, args.min_free_bytes])?;
    Ok(Resources {
        total_download_bytes: sum(groups.iter().map(|g| g.compressed_bytes))?,
        total_extracted_bytes: sum(groups.iter().map(|g| g.extracted_bytes))?,
        retained_header_bytes: retained_headers,
        largest_group_file_bytes: largest_group,
        peak_header_pass_file_bytes: peak_headers,
        peak_payload_pass_file_bytes: peak_payload,
        peak_staging_file_bytes: peak_files,
        working_space_bytes: args.working_space_bytes,
        min_free_bytes: args.min_free_bytes,
        required_staging_budget_bytes: required,
        configured_staging_budget_bytes: args.max_staging_bytes,
        fits_staging_budget: args.max_staging_bytes.map(|budget| budget >= required),
        assumptions: [
            "one active payload group; no prefetch; reuse retained header jars without downloading them again",
            "compressed payload archives coexist with extracted files; retries replace partial files rather than duplicating them",
            "file payload estimate plus explicit working allowance and reserve; not a measured RSS or filesystem allocation bound",
            "PostgreSQL data, indices, WAL, build cache, and network retry traffic are additional and unestimated",
        ],
    })
}
