//! Synthetic manifest tests exercise selection, format rejection, and space accounting.

#![expect(clippy::panic_in_result_fn, reason = "test assertions")]

use super::{manifest::COMPONENTS, plan::build, PlanArgs};
use alloy_primitives::B256;
use clap::Parser;
use eyre::Result;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn fixture() -> Value {
    let mut components = serde_json::Map::new();
    for name in COMPONENTS {
        let files: Vec<Value> = (0..3)
            .map(|i| {
                let stem = format!("static_files/static_file_{name}_{}_{}", i * 10, i * 10 + 9);
                json!([
                    {"path": stem, "size": 80, "blake3": "ab".repeat(32)},
                    {"path": format!("{stem}.conf"), "size": 10, "blake3": "cd".repeat(32)},
                    {"path": format!("{stem}.off"), "size": 10, "blake3": "ef".repeat(32)}
                ])
            })
            .collect();
        components.insert(name.to_owned(), json!({
            "blocks_per_file": 10,
            "total_blocks": 25,
            "chunk_files": [format!("static_files/{name}-0-9.tar.zst"), format!("static_files/{name}-10-19.tar.zst"), format!("123/{name}-20-29.tar.zst")],
            "chunk_sizes": [50, 50, 50],
            "chunk_decompressed_sizes": [100, 100, 100],
            "chunk_output_files": files,
        }));
    }
    // Unneeded execution components must not be decoded or selected.
    components.insert("state".into(), json!({"unrelated_format": true}));
    json!({
        "block": 24, "chain_id": 8453, "storage_version": 2,
        "reth_version": super::manifest::PRODUCER,
        "base_url": "https://snapshots.example.test",
        "components": components,
    })
}

fn inputs(value: &Value, start: u64, end: u64) -> Result<(Vec<u8>, PlanArgs)> {
    let raw = serde_json::to_vec(value)?;
    let args = PlanArgs {
        manifest: "unused.json".into(),
        manifest_sha256: format!("{:x}", Sha256::digest(&raw)),
        start_block: start,
        end_block: end,
        checkpoint_hash: None,
        max_staging_bytes: None,
        working_space_bytes: 7,
        min_free_bytes: 11,
    };
    Ok((raw, args))
}

#[test]
fn non_genesis_selection_excludes_earlier_groups_and_clips_tail() -> Result<()> {
    let (raw, args) = inputs(&fixture(), 13, 24)?;
    let plan = build(&raw, &args)?;
    assert_eq!(plan.requested_range, [13, 24]);
    assert_eq!(plan.decode_dependency_range, [10, 24]);
    assert_eq!(plan.groups.len(), 2);
    assert_eq!(plan.groups[0].index_range, [13, 19]);
    assert_eq!(plan.groups[1].archive_range, [20, 29]);
    assert_eq!(plan.groups[1].available_range, [20, 24]);
    assert_eq!(plan.groups[1].index_range, [20, 24]);
    assert_eq!(plan.groups[1].decode_from, 20);
    assert!(plan.groups[1].archives[0]
        .url
        .ends_with("/123/headers-20-29.tar.zst"));
    assert_eq!(plan.handoff.candidate_p2p_start, 25);
    Ok(())
}

#[test]
fn single_block_and_genesis_ranges_are_inclusive() -> Result<()> {
    for number in [0, 9, 10, 24] {
        let (raw, args) = inputs(&fixture(), number, number)?;
        let plan = build(&raw, &args)?;
        assert_eq!(plan.groups.len(), 1);
        assert_eq!(plan.groups[0].index_range, [number, number]);
        assert_eq!(plan.decode_dependency_range, [number / 10 * 10, number]);
    }
    Ok(())
}

#[test]
fn rolling_budget_counts_retained_headers_and_one_payload_group() -> Result<()> {
    let (raw, mut args) = inputs(&fixture(), 0, 24)?;
    args.max_staging_bytes = Some(617);
    let plan = build(&raw, &args)?;
    let r = plan.resources;
    assert_eq!(r.total_download_bytes, 450);
    assert_eq!(r.total_extracted_bytes, 900);
    assert_eq!(r.retained_header_bytes, 300);
    assert_eq!(r.largest_group_file_bytes, 450);
    assert_eq!(r.peak_header_pass_file_bytes, 350);
    assert_eq!(r.peak_payload_pass_file_bytes, 600);
    assert_eq!(r.required_staging_budget_bytes, 618);
    assert_eq!(r.fits_staging_budget, Some(false));
    args.max_staging_bytes = Some(618);
    assert_eq!(
        build(&raw, &args)?.resources.fits_staging_budget,
        Some(true)
    );
    args.max_staging_bytes = None;
    assert_eq!(build(&raw, &args)?.resources.fits_staging_budget, None);
    Ok(())
}

#[test]
fn large_header_archive_can_dominate_peak_space() -> Result<()> {
    let mut manifest = fixture();
    manifest["components"]["headers"]["chunk_sizes"][0] = json!(1000);
    let (raw, args) = inputs(&manifest, 0, 24)?;
    let r = build(&raw, &args)?.resources;
    assert_eq!(r.peak_header_pass_file_bytes, 1300);
    assert_eq!(r.peak_staging_file_bytes, 1300);
    Ok(())
}

#[test]
fn checkpoint_is_only_a_supplied_input_and_handoff_is_unprobed() -> Result<()> {
    let (raw, mut args) = inputs(&fixture(), 13, 24)?;
    let missing = build(&raw, &args)?;
    assert!(missing.trust.supplied_checkpoint_hash.is_none());
    assert_eq!(missing.trust.required_before_import.len(), 4);
    args.checkpoint_hash = Some(B256::repeat_byte(1));
    let supplied = build(&raw, &args)?;
    assert_eq!(supplied.trust.anchor_block, 24);
    assert_eq!(supplied.trust.required_before_import.len(), 3);
    let json = serde_json::to_value(supplied)?;
    assert_eq!(json["trust"]["checkpoint_status"], "supplied_not_verified");
    assert_eq!(
        json["handoff"]["status"],
        "not_probed_no_availability_claim"
    );
    Ok(())
}

#[test]
fn rejects_wrong_digest_before_parsing_and_invalid_ranges() -> Result<()> {
    let (raw, mut args) = inputs(&fixture(), 0, 24)?;
    args.manifest_sha256 = "00".repeat(32);
    assert!(matches!(build(&raw, &args), Err(error) if error.to_string().contains("SHA-256")));
    for (start, end) in [(20, 10), (0, 25), (u64::MAX, u64::MAX)] {
        let (raw, args) = inputs(&fixture(), start, end)?;
        assert!(build(&raw, &args).is_err());
    }
    Ok(())
}

#[test]
fn rejects_incompatible_identity_producer_and_layout() -> Result<()> {
    let cases = [
        ("/chain_id", json!(1)),
        ("/storage_version", json!(1)),
        ("/reth_version", json!("2.5.3")),
        ("/block", json!(u64::MAX)),
        ("/components/receipts/total_blocks", json!(24)),
        ("/components/transactions/blocks_per_file", json!(9)),
        ("/components/headers/blocks_per_file", json!(0)),
        ("/components/receipts/chunk_sizes", json!([50])),
    ];
    for (path, value) in cases {
        let mut manifest = fixture();
        *manifest
            .pointer_mut(path)
            .ok_or_else(|| eyre::eyre!("fixture path {path}"))? = value;
        let (raw, args) = inputs(&manifest, 13, 24)?;
        assert!(build(&raw, &args).is_err(), "accepted {path}");
    }
    let mut manifest = fixture();
    manifest["components"]
        .as_object_mut()
        .ok_or_else(|| eyre::eyre!("components"))?
        .remove("receipts");
    let (raw, args) = inputs(&manifest, 0, 24)?;
    assert!(build(&raw, &args).is_err());
    Ok(())
}

#[test]
fn rejects_unsafe_paths_invalid_hashes_and_inconsistent_sizes() -> Result<()> {
    let cases = [
        ("/base_url", json!("http://snapshots.example.test")),
        (
            "/base_url",
            json!("https://user:pass@snapshots.example.test"),
        ),
        (
            "/base_url",
            json!("https://snapshots.example.test?token=secret"),
        ),
        (
            "/base_url",
            json!("https://snapshots.example.test/#fragment"),
        ),
        (
            "/components/headers/chunk_files/0",
            json!("../headers-0-9.tar.zst"),
        ),
        (
            "/components/headers/chunk_files/0",
            json!("https://elsewhere.test/headers-0-9.tar.zst"),
        ),
        (
            "/components/headers/chunk_output_files/0/0/path",
            json!("../escape"),
        ),
        (
            "/components/headers/chunk_output_files/0/1/path",
            json!("static_files/static_file_headers_0_9"),
        ),
        (
            "/components/headers/chunk_output_files/0/0/blake3",
            json!("ab"),
        ),
        ("/components/headers/chunk_sizes/0", json!(0)),
        ("/components/headers/chunk_decompressed_sizes/0", json!(101)),
        (
            "/components/headers/chunk_output_files/0/0/size",
            json!(u64::MAX),
        ),
    ];
    for (path, value) in cases {
        let mut manifest = fixture();
        *manifest
            .pointer_mut(path)
            .ok_or_else(|| eyre::eyre!("fixture path {path}"))? = value;
        let (raw, args) = inputs(&manifest, 0, 24)?;
        assert!(build(&raw, &args).is_err(), "accepted {path}");
    }
    Ok(())
}

#[test]
fn rejects_resource_arithmetic_overflow() -> Result<()> {
    let (raw, mut args) = inputs(&fixture(), 0, 24)?;
    args.min_free_bytes = u64::MAX;
    assert!(build(&raw, &args).is_err());
    let mut manifest = fixture();
    manifest["components"]["headers"]["chunk_sizes"][0] = json!(u64::MAX);
    let (raw, args) = inputs(&manifest, 0, 24)?;
    assert!(build(&raw, &args).is_err());
    Ok(())
}

#[test]
fn cli_requires_range_and_pin_and_accepts_no_config() -> Result<()> {
    let digest = "AB".repeat(32);
    let base = [
        "sieve",
        "archive-plan",
        "--manifest",
        "input.json",
        "--manifest-sha256",
        digest.as_str(),
    ];
    assert!(crate::cli::Cli::try_parse_from(base).is_err());
    let cli = crate::cli::Cli::try_parse_from(base.into_iter().chain([
        "--start-block",
        "13",
        "--end-block",
        "24",
    ]))?;
    let Some(crate::cli::Command::ArchivePlan(args)) = cli.command else {
        eyre::bail!("wrong command");
    };
    assert_eq!(args.manifest_sha256, digest.to_ascii_lowercase());
    assert_eq!(args.start_block, 13);
    assert!(super::parse_sha256("bad").is_err());
    assert!(super::parse_sha256(&format!("0x{digest}")).is_err());
    Ok(())
}

#[test]
fn current_producer_declares_height_but_retains_inclusive_coverage() -> Result<()> {
    let mut value = fixture();
    value["reth_version"] = super::manifest::CURRENT_PRODUCER.into();
    for name in COMPONENTS {
        value["components"][name]["total_blocks"] = 24.into();
    }
    let (raw, args) = inputs(&value, 13, 24)?;
    let plan = build(&raw, &args)?;
    assert_eq!(plan.groups[1].available_range, [20, 24]);
    assert_eq!(plan.requested_range, [13, 24]);
    value["components"]["receipts"]["total_blocks"] = 23.into();
    let (raw, args) = inputs(&value, 13, 24)?;
    assert!(build(&raw, &args).is_err());
    Ok(())
}
