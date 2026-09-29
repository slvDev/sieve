//! Durable archive provenance, kept separately from the current frontier so a
//! later peer-quorum commit cannot erase the archive trust boundary.
use super::Database;
use crate::sync::validation::{ArchiveEvidence, ArchiveFrontier};
use crate::types::BlockNumber;
use alloy_primitives::B256;
use eyre::{ensure, Result};
use sqlx::{Postgres, Transaction};

pub const ARCHIVE_FRONTIER_DDL: &str = "CREATE TABLE IF NOT EXISTS _sieve_archive_frontier (
    id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    block_number BIGINT NOT NULL,
    block_hash BYTEA NOT NULL,
    evidence TEXT NOT NULL
)";

pub async fn archive_frontier(db: &Database) -> Result<Option<ArchiveFrontier>> {
    let row: Option<(i64, Vec<u8>, String)> = sqlx::query_as(
        "SELECT block_number, block_hash, evidence FROM _sieve_archive_frontier WHERE id = 1",
    )
    .fetch_optional(db.pool())
    .await?;
    row.map(|(block, hash, evidence)| {
        let evidence: ArchiveEvidence = serde_json::from_str(&evidence)?;
        evidence.validate()?;
        let block = u64::try_from(block)?;
        ensure!(
            block >= evidence.first_block && block <= evidence.anchor_block,
            "archive frontier outside evidence range"
        );
        Ok(ArchiveFrontier {
            block,
            hash: B256::try_from(hash.as_slice())
                .map_err(|_| eyre::eyre!("invalid archive frontier hash"))?,
            evidence,
        })
    })
    .transpose()
}

pub async fn frontier_source(db: &Database) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT source FROM _sieve_canonical WHERE id = 1")
            .fetch_optional(db.pool())
            .await?,
    )
}

/// Called only by the shared writer after authenticated, contiguous processing.
/// Indexed rows, factory coverage, checkpoint, and provenance share a transaction.
pub async fn advance_archive_frontier(
    tx: &mut Transaction<'_, Postgres>,
    block: u64,
    hash: B256,
    evidence: &ArchiveEvidence,
) -> Result<()> {
    evidence.validate()?;
    ensure!(
        block >= evidence.first_block && block <= evidence.anchor_block,
        "archive commit outside authenticated range"
    );
    let prior: Option<(i64, String)> = sqlx::query_as(
        "SELECT block_number, evidence FROM _sieve_archive_frontier WHERE id = 1 FOR UPDATE",
    )
    .fetch_optional(&mut **tx)
    .await?;
    if let Some((previous, serialized)) = prior {
        let previous_evidence: ArchiveEvidence = serde_json::from_str(&serialized)?;
        ensure!(
            previous_evidence == *evidence && block >= u64::try_from(previous)?,
            "archive source changed or frontier regressed; explicit reconciliation required"
        );
    }
    sqlx::query("INSERT INTO _sieve_archive_frontier (id, block_number, block_hash, evidence) VALUES (1, $1, $2, $3) ON CONFLICT (id) DO UPDATE SET block_number = EXCLUDED.block_number, block_hash = EXCLUDED.block_hash, evidence = EXCLUDED.evidence")
        .bind(block as i64).bind(hash.as_slice()).bind(serde_json::to_string(evidence)?)
        .execute(&mut **tx).await?;
    super::advance_verified_frontier(tx, BlockNumber::new(block), &hash).await?;
    sqlx::query("UPDATE _sieve_canonical SET source = 'archive_checkpoint' WHERE id = 1")
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub async fn ensure_rollback_above_archive(
    tx: &mut Transaction<'_, Postgres>,
    target: BlockNumber,
) -> Result<()> {
    let boundary: Option<i64> =
        sqlx::query_scalar("SELECT block_number FROM _sieve_archive_frontier WHERE id = 1")
            .fetch_optional(&mut **tx)
            .await?;
    if let Some(boundary) = boundary {
        ensure!(
            target.as_u64() >= u64::try_from(boundary)?,
            "rollback crosses trusted archive boundary at {boundary}; explicit recovery required"
        );
    }
    Ok(())
}
