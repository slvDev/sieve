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
    let initial = if db.has_indexed_state().await? {
        db.last_checkpoint()
            .await?
            .map_or(start, |block| start.max(block.as_u64() + 1))
    } else {
        start
    };
    sqlx::query("UPDATE _sieve_archive_job SET initial_start = CASE WHEN committed_block IS NULL THEN $1 ELSE start_block END WHERE id = 1 AND initial_start IS NULL")
        .bind(i64::try_from(initial)?).execute(db.pool()).await?;
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
        // The producer validates full-group row consumption before submitting
        // its last block, so completion is durable with that block's output.
        sqlx::query("UPDATE _sieve_archive_groups SET completed = ($1 = end_block) WHERE start_block <= $1 AND end_block >= $1")
            .bind(block as i64).execute(&mut **tx).await?;
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

pub const GROUPS_DDL: &str = "CREATE TABLE IF NOT EXISTS _sieve_archive_groups (
    start_block BIGINT PRIMARY KEY,
    end_block BIGINT NOT NULL,
    descriptor TEXT NOT NULL,
    next_transaction BIGINT,
    completed BOOLEAN NOT NULL DEFAULT FALSE,
    cleaned BOOLEAN NOT NULL DEFAULT FALSE
)";

/// Source boundaries are stable metadata, not permission to skip indexed blocks.
#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct GroupDescriptor {
    pub archive_range: [u64; 2],
    pub index_range: [u64; 2],
    pub available_end: u64,
    /// Half-open source transaction range; None for a transaction-free group.
    pub transaction_range: Option<[u64; 2]>,
}

pub struct GroupProgress {
    pub descriptor: GroupDescriptor,
    pub next_transaction: Option<u64>,
    pub completed: bool,
    pub cleaned: bool,
}

pub async fn group_progress(db: &Database, start: u64) -> Result<Option<GroupProgress>> {
    let row: Option<(String, Option<i64>, bool, bool)> = sqlx::query_as("SELECT descriptor, next_transaction, completed, cleaned FROM _sieve_archive_groups WHERE start_block = $1")
        .bind(i64::try_from(start)?).fetch_optional(db.pool()).await?;
    row.map(|(descriptor, next, completed, cleaned)| {
        Ok(GroupProgress {
            descriptor: serde_json::from_str(&descriptor)?,
            next_transaction: next.map(u64::try_from).transpose()?,
            completed,
            cleaned,
        })
    })
    .transpose()
}

pub async fn prepare_group(
    db: &Database,
    descriptor: &GroupDescriptor,
    previous_transaction: Option<u64>,
) -> Result<()> {
    let next = match descriptor.transaction_range {
        Some([first, next]) => {
            ensure!(
                first < next && previous_transaction.is_none_or(|previous| previous == first),
                "transaction-number discontinuity between archive groups"
            );
            Some(next)
        }
        None => previous_transaction,
    };
    let start = i64::try_from(descriptor.index_range[0])?;
    let end = i64::try_from(descriptor.index_range[1])?;
    let serialized = serde_json::to_string(descriptor)?;
    let next = if descriptor.index_range[1] == descriptor.available_end {
        next.map(i64::try_from).transpose()?
    } else {
        None
    };
    sqlx::query("INSERT INTO _sieve_archive_groups (start_block, end_block, descriptor, next_transaction) VALUES ($1, $2, $3, $4) ON CONFLICT (start_block) DO NOTHING")
        .bind(start).bind(end).bind(&serialized).bind(next).execute(db.pool()).await?;
    let stored: (String, Option<i64>) = sqlx::query_as(
        "SELECT descriptor, next_transaction FROM _sieve_archive_groups WHERE start_block = $1",
    )
    .bind(start)
    .fetch_one(db.pool())
    .await?;
    ensure!(
        stored == (serialized, next),
        "archive group decoding position changed on resume"
    );
    Ok(())
}

pub async fn mark_group_cleaned(db: &Database, start: u64) -> Result<()> {
    let result = sqlx::query(
        "UPDATE _sieve_archive_groups SET cleaned = TRUE WHERE start_block = $1 AND completed",
    )
    .bind(i64::try_from(start)?)
    .execute(db.pool())
    .await?;
    ensure!(
        result.rows_affected() == 1,
        "cannot release an incomplete archive group"
    );
    Ok(())
}

pub async fn initial_start(db: &Database) -> Result<u64> {
    let value: i64 =
        sqlx::query_scalar("SELECT initial_start FROM _sieve_archive_job WHERE id = 1")
            .fetch_one(db.pool())
            .await?;
    Ok(u64::try_from(value)?)
}
