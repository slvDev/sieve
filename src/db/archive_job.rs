//! Archive progress is advanced in the same transaction as indexed output.
use super::Database;
use crate::sync::validation::ArchiveEvidence;
use alloy_primitives::B256;
use eyre::{ensure, Result};
use sqlx::{Connection, PgConnection, Postgres, Transaction};

pub const DDL: &str = "CREATE TABLE IF NOT EXISTS _sieve_archive_job (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    identity TEXT NOT NULL,
    evidence TEXT NOT NULL,
    start_block BIGINT NOT NULL,
    end_block BIGINT NOT NULL,
    committed_block BIGINT,
    committed_hash BYTEA,
    cleaned BOOLEAN NOT NULL DEFAULT FALSE
)";

/// A dedicated session owns the lease; dropping it closes the connection rather
/// than returning a session-level advisory lock to the connection pool.
pub async fn writer_lease(url: &str) -> Result<PgConnection> {
    let mut connection = PgConnection::connect(url).await?;
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(1936287094, 1)")
        .fetch_one(&mut connection)
        .await?;
    ensure!(acquired, "another Sieve writer owns this database");
    Ok(connection)
}

pub async fn prepare(
    db: &Database,
    identity: &str,
    evidence: &ArchiveEvidence,
    start: u64,
    end: u64,
) -> Result<()> {
    sqlx::query("INSERT INTO _sieve_archive_job (id, identity, evidence, start_block, end_block) VALUES (1, $1, $2, $3, $4) ON CONFLICT (id) DO NOTHING")
        .bind(identity).bind(serde_json::to_string(evidence)?).bind(start as i64).bind(end as i64).execute(db.pool()).await?;
    let stored: String = sqlx::query_scalar("SELECT identity FROM _sieve_archive_job WHERE id = 1")
        .fetch_one(db.pool())
        .await?;
    ensure!(
        stored == identity,
        "archive job/configuration changed; restore the original job or use a fresh database"
    );
    Ok(())
}

pub async fn advance(
    tx: &mut Transaction<'_, Postgres>,
    block: u64,
    hash: B256,
    evidence: &ArchiveEvidence,
) -> Result<()> {
    let row: Option<(String, i64, i64)> = sqlx::query_as(
        "SELECT evidence, start_block, end_block FROM _sieve_archive_job WHERE id = 1 FOR UPDATE",
    )
    .fetch_optional(&mut **tx)
    .await?;
    if let Some((stored, start, end)) = row {
        ensure!(
            serde_json::from_str::<ArchiveEvidence>(&stored)? == *evidence
                && (start..=end).contains(&(block as i64)),
            "archive commit differs from durable job"
        );
        sqlx::query(
            "UPDATE _sieve_archive_job SET committed_block = $1, committed_hash = $2 WHERE id = 1",
        )
        .bind(block as i64)
        .bind(hash.as_slice())
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub async fn progress(db: &Database) -> Result<Option<(u64, B256)>> {
    let row: Option<(i64, Vec<u8>)> = sqlx::query_as("SELECT committed_block, committed_hash FROM _sieve_archive_job WHERE id = 1 AND committed_block IS NOT NULL").fetch_optional(db.pool()).await?;
    row.map(|(block, hash)| {
        Ok((
            u64::try_from(block)?,
            B256::try_from(hash.as_slice()).map_err(|_| eyre::eyre!("invalid archive job hash"))?,
        ))
    })
    .transpose()
}

pub async fn mark_cleaned(db: &Database) -> Result<()> {
    let result = sqlx::query(
        "UPDATE _sieve_archive_job SET cleaned = TRUE WHERE id = 1 AND committed_block = end_block",
    )
    .execute(db.pool())
    .await?;
    ensure!(
        result.rows_affected() == 1,
        "cannot clean incomplete archive job"
    );
    Ok(())
}
