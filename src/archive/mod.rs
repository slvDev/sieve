//! Offline planning and explicitly configured Base V2 archive ingestion.

#[cfg(test)]
mod acceptance_tests;
pub mod handoff;
mod manifest;
mod plan;
mod reader;
mod runtime;
mod staging;
pub use runtime::{config_fingerprint, ArchiveConfig, PreparedImport};

#[cfg(test)]
mod tests;

use alloy_primitives::B256;
use clap::Args;
use eyre::{ensure, Result, WrapErr};
use std::{fs::File, io::Read, path::PathBuf};

use plan::Plan;

const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;

/// Explicit inputs for offline planning, independent of indexer configuration.
#[derive(Debug, Args)]
pub struct PlanArgs {
    /// Local Base V2 manifest JSON. No network requests are made.
    #[arg(long)]
    pub manifest: PathBuf,
    /// Independently obtained SHA-256 of the exact manifest bytes.
    #[arg(long, value_parser = parse_sha256)]
    pub manifest_sha256: String,
    /// First block to index (earlier blocks in its archive are decode dependencies).
    #[arg(long)]
    pub start_block: u64,
    /// Last block to index, inclusive. Must be covered by the manifest.
    #[arg(long)]
    pub end_block: u64,
    /// Independently trusted hash at --end-block; recorded, not verified by planning.
    #[arg(long)]
    pub checkpoint_hash: Option<B256>,
    /// Maximum planned staging bytes, including the reserve. Omit to only estimate.
    #[arg(long)]
    pub max_staging_bytes: Option<u64>,
    /// Filesystem/job-metadata working allowance added to source file sizes.
    #[arg(long, default_value_t = 1_073_741_824)]
    pub working_space_bytes: u64,
    /// Free-space reserve added to staging requirements (PostgreSQL is separate).
    #[arg(long, default_value_t = 5_368_709_120)]
    pub min_free_bytes: u64,
}

fn parse_sha256(value: &str) -> std::result::Result<String, String> {
    if value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(value.to_ascii_lowercase())
    } else {
        Err("expected 64 hexadecimal SHA-256 digits (without 0x)".into())
    }
}

/// Inspect a bounded local manifest without opening any other resource.
fn inspect(args: &PlanArgs) -> Result<Plan> {
    let file = File::open(&args.manifest).wrap_err("cannot open archive manifest")?;
    ensure!(
        file.metadata()?.is_file(),
        "manifest must be a regular file"
    );
    let mut raw = Vec::new();
    file.take(MAX_MANIFEST_BYTES + 1).read_to_end(&mut raw)?;
    ensure!(
        raw.len() as u64 <= MAX_MANIFEST_BYTES,
        "manifest exceeds 16 MiB"
    );
    plan::build(&raw, args)
}

/// Print a metadata-only plan. A failed budget check is reported in JSON, so users
/// can inspect requirements without changing limits or transferring data.
pub fn print_plan(args: &PlanArgs) -> Result<()> {
    use std::io::Write;
    let plan = inspect(args)?;
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer_pretty(&mut stdout, &plan)?;
    writeln!(stdout)?;
    Ok(())
}
