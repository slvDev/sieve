//! Database layer — PostgreSQL storage for indexed events.
//!
//! Uses `sqlx` with async connection pooling and embedded migrations.
//!
//! Internal tables:
//! - `_sieve_checkpoints`: track which blocks have been indexed
//!
//! Transaction model: one Postgres transaction per block, so all handler
//! INSERTs + checkpoint UPDATE are committed atomically.

use alloy_primitives::B256;
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

    /// Read the stored block hash for a given block number.
    ///
    /// Returns `None` if no hash is stored for that block.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get_block_hash(&self, block_number: u64) -> eyre::Result<Option<B256>> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as(
            "SELECT block_hash FROM _sieve_block_hashes WHERE block_number = $1",
        )
        .bind(block_number as i64)
        .fetch_optional(&self.pool)
        .await
        .wrap_err("failed to read block hash")?;

        match row {
            Some((bytes,)) => {
                let hash = B256::try_from(bytes.as_slice())
                    .map_err(|_| eyre::eyre!("invalid block hash length in DB"))?;
                Ok(Some(hash))
            }
            None => Ok(None),
        }
    }

    /// Expose the pool for integration tests.
    #[cfg(test)]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Update the checkpoint block number within an existing transaction.
///
/// Uses `GREATEST` so the checkpoint never moves backward during normal sync.
///
/// # Errors
///
/// Returns an error if the UPDATE query fails.
pub async fn update_checkpoint(
    tx: &mut Transaction<'_, Postgres>,
    block_number: u64,
) -> eyre::Result<()> {
    sqlx::query(
        "UPDATE _sieve_checkpoints SET block_number = GREATEST(block_number, $1), updated_at = NOW() WHERE id = 1",
    )
    .bind(block_number as i64)
    .execute(&mut **tx)
    .await
    .wrap_err("failed to update checkpoint")?;
    Ok(())
}

/// Store a block hash for reorg detection within an existing transaction.
///
/// Uses `ON CONFLICT DO UPDATE` so re-indexing after a reorg overwrites
/// the old (now-stale) hash.
///
/// # Errors
///
/// Returns an error if the INSERT/UPDATE query fails.
pub async fn store_block_hash(
    tx: &mut Transaction<'_, Postgres>,
    block_number: u64,
    block_hash: &[u8],
) -> eyre::Result<()> {
    sqlx::query(
        "INSERT INTO _sieve_block_hashes (block_number, block_hash) VALUES ($1, $2) \
         ON CONFLICT (block_number) DO UPDATE SET block_hash = EXCLUDED.block_hash",
    )
    .bind(block_number as i64)
    .bind(block_hash)
    .execute(&mut **tx)
    .await
    .wrap_err("failed to store block hash")?;
    Ok(())
}

/// Roll back internal sieve tables (block hashes, checkpoint) to a given block number.
///
/// Handlers roll back their own tables via [`HandlerRegistry::rollback_all`].
/// This function handles sieve-internal state only.
///
/// # Errors
///
/// Returns an error if any DELETE/UPDATE query fails.
pub async fn rollback_to(
    tx: &mut Transaction<'_, Postgres>,
    block_number: u64,
) -> eyre::Result<()> {
    sqlx::query("DELETE FROM _sieve_block_hashes WHERE block_number > $1")
        .bind(block_number as i64)
        .execute(&mut **tx)
        .await
        .wrap_err("failed to rollback block hashes")?;

    // Unconditional SET — rollback explicitly lowers the checkpoint
    sqlx::query(
        "UPDATE _sieve_checkpoints SET block_number = $1, updated_at = NOW() WHERE id = 1",
    )
    .bind(block_number as i64)
    .execute(&mut **tx)
    .await
    .wrap_err("failed to reset checkpoint after rollback")?;

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

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn block_hash_roundtrip() -> eyre::Result<()> {
        let db = test_db().await?;

        let hash_a = alloy_primitives::B256::repeat_byte(0xAA);
        let hash_b = alloy_primitives::B256::repeat_byte(0xBB);

        // Store a hash and read it back
        let mut tx = db.begin().await?;
        store_block_hash(&mut tx, 99_999, hash_a.as_slice()).await?;
        tx.commit().await.wrap_err("commit failed")?;

        let stored = db.get_block_hash(99_999).await?;
        assert_eq!(stored, Some(hash_a));

        // Overwrite with a different hash (simulates reorg re-indexing)
        let mut tx = db.begin().await?;
        store_block_hash(&mut tx, 99_999, hash_b.as_slice()).await?;
        tx.commit().await.wrap_err("commit failed")?;

        let stored = db.get_block_hash(99_999).await?;
        assert_eq!(stored, Some(hash_b));

        // Clean up
        sqlx::query("DELETE FROM _sieve_block_hashes WHERE block_number = $1")
            .bind(99_999i64)
            .execute(db.pool())
            .await
            .wrap_err("cleanup failed")?;

        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn rollback_deletes_hashes() -> eyre::Result<()> {
        let db = test_db().await?;

        // Store hashes for blocks 100-110
        for block in 100..=110u64 {
            let hash = alloy_primitives::B256::repeat_byte(block as u8);
            let mut tx = db.begin().await?;
            store_block_hash(&mut tx, block, hash.as_slice()).await?;
            update_checkpoint(&mut tx, block).await?;
            tx.commit().await.wrap_err("commit failed")?;
        }

        // Rollback to 105
        let mut tx = db.begin().await?;
        rollback_to(&mut tx, 105).await?;
        tx.commit().await.wrap_err("commit failed")?;

        // Blocks 100-105 should still have hashes
        for block in 100..=105u64 {
            let stored = db.get_block_hash(block).await?;
            assert!(stored.is_some(), "block {block} hash should exist");
        }

        // Blocks 106-110 should be gone
        for block in 106..=110u64 {
            let stored = db.get_block_hash(block).await?;
            assert!(stored.is_none(), "block {block} hash should be deleted");
        }

        // Checkpoint should be 105
        let checkpoint = db.last_checkpoint().await?;
        assert_eq!(checkpoint, Some(105));

        // Clean up
        sqlx::query("DELETE FROM _sieve_block_hashes WHERE block_number BETWEEN 100 AND 110")
            .execute(db.pool())
            .await
            .wrap_err("cleanup failed")?;
        sqlx::query("UPDATE _sieve_checkpoints SET block_number = 0 WHERE id = 1")
            .execute(db.pool())
            .await
            .wrap_err("cleanup failed")?;

        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn checkpoint_does_not_decrease() -> eyre::Result<()> {
        let db = test_db().await?;

        // Reset checkpoint to 0
        sqlx::query("UPDATE _sieve_checkpoints SET block_number = 0 WHERE id = 1")
            .execute(db.pool())
            .await
            .wrap_err("reset failed")?;

        // Set checkpoint to 100
        let mut tx = db.begin().await?;
        update_checkpoint(&mut tx, 100).await?;
        tx.commit().await.wrap_err("commit failed")?;

        // Try to set checkpoint to 50 (should be ignored by GREATEST)
        let mut tx = db.begin().await?;
        update_checkpoint(&mut tx, 50).await?;
        tx.commit().await.wrap_err("commit failed")?;

        // Checkpoint should still be 100
        let checkpoint = db.last_checkpoint().await?;
        assert_eq!(checkpoint, Some(100));

        // Set checkpoint to 200 (should advance)
        let mut tx = db.begin().await?;
        update_checkpoint(&mut tx, 200).await?;
        tx.commit().await.wrap_err("commit failed")?;

        let checkpoint = db.last_checkpoint().await?;
        assert_eq!(checkpoint, Some(200));

        // Clean up
        sqlx::query("UPDATE _sieve_checkpoints SET block_number = 0 WHERE id = 1")
            .execute(db.pool())
            .await
            .wrap_err("cleanup failed")?;

        Ok(())
    }
}
