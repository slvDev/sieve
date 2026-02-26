//! Database layer — PostgreSQL storage for indexed events.
//!
//! Uses `sqlx` with async connection pooling and embedded migrations.
//!
//! Internal tables:
//! - `_sieve_checkpoints`: track which blocks have been indexed
//!
//! Transaction model: one Postgres transaction per block, so all handler
//! INSERTs + checkpoint UPDATE are committed atomically.

use eyre::WrapErr;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Transaction};
use tracing::info;

/// PostgreSQL database wrapper.
#[derive(Debug)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Connect to PostgreSQL and run embedded migrations.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection or migrations fail.
    pub async fn connect(url: &str) -> eyre::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .wrap_err("failed to connect to database")?;

        sqlx::migrate!()
            .run(&pool)
            .await
            .wrap_err("failed to run migrations")?;

        info!("database connected and migrations applied");
        Ok(Self { pool })
    }

    /// Read the last checkpoint block number.
    ///
    /// Returns `None` if the checkpoint is 0 (no blocks indexed yet).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn last_checkpoint(&self) -> eyre::Result<Option<u64>> {
        let row: (i64,) =
            sqlx::query_as("SELECT block_number FROM _sieve_checkpoints WHERE id = 1")
                .fetch_one(&self.pool)
                .await
                .wrap_err("failed to read checkpoint")?;

        if row.0 == 0 {
            Ok(None)
        } else {
            Ok(Some(row.0 as u64))
        }
    }

    /// Begin a new database transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if starting the transaction fails.
    pub async fn begin(&self) -> eyre::Result<Transaction<'_, Postgres>> {
        self.pool
            .begin()
            .await
            .wrap_err("failed to begin transaction")
    }

    /// Expose the pool for integration tests.
    #[cfg(test)]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Update the checkpoint block number within an existing transaction.
///
/// # Errors
///
/// Returns an error if the UPDATE query fails.
pub async fn update_checkpoint(
    tx: &mut Transaction<'_, Postgres>,
    block_number: u64,
) -> eyre::Result<()> {
    sqlx::query(
        "UPDATE _sieve_checkpoints SET block_number = $1, updated_at = NOW() WHERE id = 1",
    )
    .bind(block_number as i64)
    .execute(&mut **tx)
    .await
    .wrap_err("failed to update checkpoint")?;
    Ok(())
}

/// Roll back indexed data to a given block number within an existing transaction.
///
/// Deletes user table rows above `block_number` and resets the checkpoint.
/// Used for reorg handling.
///
/// # Errors
///
/// Returns an error if any DELETE/UPDATE query fails.
#[expect(dead_code, reason = "used in Phase 5 reorg handling")]
pub async fn rollback_to(
    tx: &mut Transaction<'_, Postgres>,
    block_number: u64,
) -> eyre::Result<()> {
    sqlx::query("DELETE FROM usdc_transfers WHERE block_number > $1")
        .bind(block_number as i64)
        .execute(&mut **tx)
        .await
        .wrap_err("failed to rollback usdc_transfers")?;

    update_checkpoint(tx, block_number).await?;
    Ok(())
}

#[cfg(test)]
#[expect(clippy::panic_in_result_fn, reason = "assertions in tests are idiomatic")]
mod tests {
    use super::*;

    async fn test_db() -> eyre::Result<Database> {
        let url = std::env::var("DATABASE_URL")
            .wrap_err("DATABASE_URL not set")?;
        Database::connect(&url).await
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn checkpoint_roundtrip() -> eyre::Result<()> {
        let db = test_db().await?;

        // Reset checkpoint to 0 for a clean test
        sqlx::query("UPDATE _sieve_checkpoints SET block_number = 0 WHERE id = 1")
            .execute(db.pool())
            .await
            .wrap_err("reset failed")?;

        // Should be None when block_number is 0
        let checkpoint = db.last_checkpoint().await?;
        assert!(checkpoint.is_none());

        // Update checkpoint
        let mut tx = db.begin().await?;
        update_checkpoint(&mut tx, 21_000_100).await?;
        tx.commit()
            .await
            .wrap_err("commit failed")?;

        // Should now return the block number
        let checkpoint = db.last_checkpoint().await?;
        assert_eq!(checkpoint, Some(21_000_100));

        // Clean up
        sqlx::query("UPDATE _sieve_checkpoints SET block_number = 0 WHERE id = 1")
            .execute(db.pool())
            .await
            .wrap_err("cleanup failed")?;

        Ok(())
    }
}
