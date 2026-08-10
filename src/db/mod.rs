//! Database layer — PostgreSQL storage for indexed events.
//!
//! Uses `sqlx` with async connection pooling. Internal tables are created
//! at runtime via `create_internal_tables()`, same as user tables.
//!
//! Internal tables:
//! - `_sieve_checkpoints`: track which blocks have been indexed
//! - `_sieve_block_hashes`: store block hashes for reorg detection
//! - `_sieve_factory_children`: persist dynamically discovered factory children
//!
//! Transaction model: one Postgres transaction per block, so all handler
//! INSERTs + checkpoint UPDATE are committed atomically.

use crate::config::IndexConfig;
use crate::toml_config::{ResolvedCall, ResolvedEvent, ResolvedTransfer};
use crate::types::BlockNumber;
use alloy_primitives::{Address, B256};
use eyre::WrapErr;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::HashMap;
use tracing::info;

/// PostgreSQL database wrapper.
#[derive(Debug)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Connect to PostgreSQL.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails.
    pub async fn connect(url: &str) -> eyre::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query("SET synchronous_commit = off")
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(url)
            .await
            .wrap_err("failed to connect to database")?;

        info!("database connected");
        Ok(Self { pool })
    }

    /// Read the last checkpoint block number.
    ///
    /// Returns `None` if the checkpoint is 0 (no blocks indexed yet).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn last_checkpoint(&self) -> eyre::Result<Option<BlockNumber>> {
        let row: (i64,) =
            sqlx::query_as("SELECT block_number FROM _sieve_checkpoints WHERE id = 1")
                .fetch_one(&self.pool)
                .await
                .wrap_err("failed to read checkpoint")?;

        if row.0 == 0 {
            Ok(None)
        } else {
            Ok(Some(BlockNumber::new(row.0 as u64)))
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
    pub async fn get_block_hash(&self, block_number: BlockNumber) -> eyre::Result<Option<B256>> {
        let row: Option<(Vec<u8>,)> =
            sqlx::query_as("SELECT block_hash FROM _sieve_block_hashes WHERE block_number = $1")
                .bind(block_number.as_u64() as i64)
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

    /// Expose the connection pool.
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
    block_number: BlockNumber,
) -> eyre::Result<()> {
    sqlx::query(
        "UPDATE _sieve_checkpoints SET block_number = GREATEST(block_number, $1), updated_at = NOW() WHERE id = 1",
    )
    .bind(block_number.as_u64() as i64)
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
#[cfg(test)]
pub async fn store_block_hash(
    tx: &mut Transaction<'_, Postgres>,
    block_number: BlockNumber,
    block_hash: &[u8],
) -> eyre::Result<()> {
    sqlx::query(
        "INSERT INTO _sieve_block_hashes (block_number, block_hash) VALUES ($1, $2) \
         ON CONFLICT (block_number) DO UPDATE SET block_hash = EXCLUDED.block_hash",
    )
    .bind(block_number.as_u64() as i64)
    .bind(block_hash)
    .execute(&mut **tx)
    .await
    .wrap_err("failed to store block hash")?;
    Ok(())
}

/// Store multiple block hashes in a single UNNEST query.
///
/// # Errors
///
/// Returns an error if the batch INSERT fails.
pub async fn store_block_hashes_batch(
    tx: &mut Transaction<'_, Postgres>,
    block_numbers: &[i64],
    block_hashes: &[Vec<u8>],
) -> eyre::Result<()> {
    if block_numbers.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO _sieve_block_hashes (block_number, block_hash) \
         SELECT * FROM UNNEST($1::BIGINT[], $2::BYTEA[]) \
         ON CONFLICT (block_number) DO UPDATE SET block_hash = EXCLUDED.block_hash",
    )
    .bind(block_numbers)
    .bind(block_hashes)
    .execute(&mut **tx)
    .await
    .wrap_err("failed to store block hashes")?;
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
    block_number: BlockNumber,
) -> eyre::Result<()> {
    sqlx::query("DELETE FROM _sieve_block_hashes WHERE block_number > $1")
        .bind(block_number.as_u64() as i64)
        .execute(&mut **tx)
        .await
        .wrap_err("failed to rollback block hashes")?;

    // Unconditional SET — rollback explicitly lowers the checkpoint
    sqlx::query("UPDATE _sieve_checkpoints SET block_number = $1, updated_at = NOW() WHERE id = 1")
        .bind(block_number.as_u64() as i64)
        .execute(&mut **tx)
        .await
        .wrap_err("failed to reset checkpoint after rollback")?;

    Ok(())
}

/// Load persisted factory children from the database into the config.
///
/// Called at startup to restore dynamically discovered child contracts.
/// Returns the number of children loaded.
///
/// # Errors
///
/// Returns an error if the query fails.
pub async fn load_factory_children(db: &Database, config: &IndexConfig) -> eyre::Result<u64> {
    let rows = sqlx::query("SELECT factory_name, child_address FROM _sieve_factory_children")
        .fetch_all(db.pool())
        .await
        .wrap_err("failed to load factory children")?;

    let mut count = 0u64;
    for row in &rows {
        let factory_name: &str = row.try_get("factory_name")?;
        let child_bytes: Vec<u8> = row.try_get("child_address")?;

        if register_persisted_child(config, factory_name, &child_bytes) {
            count = count.saturating_add(1);
        }
    }

    if count > 0 {
        info!(count, "loaded factory children from database");
    }
    Ok(count)
}

/// Discard all in-memory factory children and reload the committed set.
///
/// Processing workers register children speculatively before any payload
/// validation, so after a rejected batch the in-memory map may contain
/// entries from blocks that were never committed — including blocks that
/// were still queued behind the failed batch. Rebuilding from the database
/// restores exactly the committed state.
///
/// The replacement is atomic: the committed set is built off to the side
/// and only swapped in once fully loaded. If the query fails, the current
/// map is left untouched — callers must treat that as fatal, since the
/// speculative state has not been discarded.
///
/// Callers must ensure no processing workers are running concurrently,
/// otherwise a worker could re-register a speculative child after the
/// rebuild.
///
/// # Errors
///
/// Returns an error if the reload query fails.
pub async fn rebuild_factory_children(db: &Database, config: &IndexConfig) -> eyre::Result<u64> {
    let rows = sqlx::query("SELECT factory_name, child_address FROM _sieve_factory_children")
        .fetch_all(db.pool())
        .await
        .wrap_err("failed to load factory children for rebuild")?;

    let mut committed: HashMap<Address, usize> = HashMap::new();
    for row in &rows {
        let factory_name: &str = row.try_get("factory_name")?;
        let child_bytes: Vec<u8> = row.try_get("child_address")?;
        if let Some((address, contract_idx)) =
            parse_persisted_child(config, factory_name, &child_bytes)
        {
            committed.insert(address, contract_idx);
        }
    }

    let count = committed.len() as u64;
    config.replace_factory_children(committed);
    Ok(count)
}

/// Validate and register a single persisted factory child.
///
/// Returns `true` if the child was successfully registered.
fn register_persisted_child(config: &IndexConfig, factory_name: &str, child_bytes: &[u8]) -> bool {
    parse_persisted_child(config, factory_name, child_bytes)
        .is_some_and(|(address, contract_idx)| config.register_factory_child(address, contract_idx))
}

/// Validate a persisted factory-child row, resolving its contract index.
///
/// Returns `None` (with a warning) for malformed addresses or rows that
/// reference a contract missing from the current config.
fn parse_persisted_child(
    config: &IndexConfig,
    factory_name: &str,
    child_bytes: &[u8],
) -> Option<(Address, usize)> {
    if child_bytes.len() != 20 {
        tracing::warn!(
            factory = factory_name,
            len = child_bytes.len(),
            "invalid child address length in DB, skipping"
        );
        return None;
    }

    let child_address = Address::from_slice(child_bytes);

    let Some(contract_idx) = config.contracts.iter().position(|c| c.name == factory_name) else {
        tracing::warn!(
            factory = factory_name,
            "factory child references unknown contract, skipping"
        );
        return None;
    };

    Some((child_address, contract_idx))
}

/// Persist a newly discovered factory child in the database.
///
/// # Errors
///
/// Returns an error if the INSERT fails.
pub async fn store_factory_child(
    tx: &mut Transaction<'_, Postgres>,
    factory_name: &str,
    child_address: &Address,
    block_number: u64,
) -> eyre::Result<()> {
    sqlx::query(
        "INSERT INTO _sieve_factory_children (factory_name, child_address, block_number) \
         VALUES ($1, $2, $3) ON CONFLICT (child_address) DO NOTHING",
    )
    .bind(factory_name)
    .bind(child_address.as_slice())
    .bind(block_number as i64)
    .execute(&mut **tx)
    .await
    .wrap_err("failed to store factory child")?;
    Ok(())
}

/// Roll back factory children discovered after the given block number.
///
/// Returns the addresses that were removed (for unregistering from config).
///
/// # Errors
///
/// Returns an error if the query fails.
pub async fn rollback_factory_children(
    tx: &mut Transaction<'_, Postgres>,
    block_number: BlockNumber,
    config: &IndexConfig,
) -> eyre::Result<Vec<Address>> {
    let rows = sqlx::query(
        "DELETE FROM _sieve_factory_children WHERE block_number > $1 RETURNING child_address",
    )
    .bind(block_number.as_u64() as i64)
    .fetch_all(&mut **tx)
    .await
    .wrap_err("failed to rollback factory children")?;

    let mut removed = Vec::with_capacity(rows.len());
    for row in &rows {
        let child_bytes: Vec<u8> = row.try_get("child_address")?;
        if child_bytes.len() == 20 {
            let addr = Address::from_slice(&child_bytes);
            config.unregister_factory_child(&addr);
            removed.push(addr);
        }
    }

    if !removed.is_empty() {
        info!(count = removed.len(), "rolled back factory children");
    }
    Ok(removed)
}

/// Drop all sieve tables (user + internal) for a fresh start.
///
/// Drops user tables first, then internal tables. Also removes the legacy
/// `_sqlx_migrations` table if it exists from before the migration removal.
///
/// # Errors
///
/// Returns an error if any DROP statement fails.
pub async fn drop_all_tables(
    db: &Database,
    events: &[ResolvedEvent],
    transfers: &[ResolvedTransfer],
    calls: &[ResolvedCall],
) -> eyre::Result<()> {
    // User tables
    for event in events {
        let sql = format!("DROP TABLE IF EXISTS {} CASCADE", event.table_name);
        sqlx::raw_sql(&sql)
            .execute(db.pool())
            .await
            .wrap_err_with(|| format!("failed to drop table '{}'", event.table_name))?;
    }
    for transfer in transfers {
        let sql = format!("DROP TABLE IF EXISTS {} CASCADE", transfer.table_name);
        sqlx::raw_sql(&sql)
            .execute(db.pool())
            .await
            .wrap_err_with(|| format!("failed to drop table '{}'", transfer.table_name))?;
    }
    for call in calls {
        let sql = format!("DROP TABLE IF EXISTS {} CASCADE", call.table_name);
        sqlx::raw_sql(&sql)
            .execute(db.pool())
            .await
            .wrap_err_with(|| format!("failed to drop table '{}'", call.table_name))?;
    }

    // Internal tables
    for table in [
        "_sieve_factory_children",
        "_sieve_block_hashes",
        "_sieve_checkpoints",
        "_sieve_chain",
        "_sqlx_migrations",
    ] {
        let sql = format!("DROP TABLE IF EXISTS {table} CASCADE");
        sqlx::raw_sql(&sql)
            .execute(db.pool())
            .await
            .wrap_err_with(|| format!("failed to drop table '{table}'"))?;
    }

    info!("all tables dropped (--fresh)");
    Ok(())
}

/// DDL for the `_sieve_checkpoints` table.
pub const CHECKPOINTS_DDL: &str = "\
CREATE TABLE IF NOT EXISTS _sieve_checkpoints (
    id SMALLINT PRIMARY KEY DEFAULT 1,
    block_number BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)";

/// DDL for the `_sieve_block_hashes` table.
pub const BLOCK_HASHES_DDL: &str = "\
CREATE TABLE IF NOT EXISTS _sieve_block_hashes (
    block_number BIGINT PRIMARY KEY,
    block_hash BYTEA NOT NULL
)";

/// DDL for the `_sieve_factory_children` table.
pub const FACTORY_CHILDREN_DDL: &str = "\
CREATE TABLE IF NOT EXISTS _sieve_factory_children (
    id BIGSERIAL PRIMARY KEY,
    factory_name TEXT NOT NULL,
    child_address BYTEA NOT NULL,
    block_number BIGINT NOT NULL,
    UNIQUE (child_address)
)";

/// DDL for the `_sieve_chain` table (single-row chain identity).
pub const CHAIN_IDENTITY_DDL: &str = "\
CREATE TABLE IF NOT EXISTS _sieve_chain (
    id SMALLINT PRIMARY KEY DEFAULT 1,
    chain TEXT NOT NULL,
    genesis_hash BYTEA NOT NULL
)";

/// Create sieve-internal tables at runtime.
///
/// Uses `CREATE TABLE IF NOT EXISTS` so it is safe to call on every startup.
/// This replaces the old `sqlx::migrate!()` approach — all DDL is now runtime.
///
/// # Errors
///
/// Returns an error if any DDL statement fails.
pub async fn create_internal_tables(db: &Database) -> eyre::Result<()> {
    let checkpoints_sql = format!(
        "{CHECKPOINTS_DDL};\n\
         INSERT INTO _sieve_checkpoints (id, block_number) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;"
    );
    sqlx::raw_sql(&checkpoints_sql)
        .execute(db.pool())
        .await
        .wrap_err("failed to create _sieve_checkpoints")?;

    sqlx::raw_sql(&format!("{BLOCK_HASHES_DDL};"))
        .execute(db.pool())
        .await
        .wrap_err("failed to create _sieve_block_hashes")?;

    sqlx::raw_sql(&format!("{FACTORY_CHILDREN_DDL};"))
        .execute(db.pool())
        .await
        .wrap_err("failed to create _sieve_factory_children")?;

    sqlx::raw_sql(&format!("{CHAIN_IDENTITY_DDL};"))
        .execute(db.pool())
        .await
        .wrap_err("failed to create _sieve_chain")?;

    info!("internal tables ready");
    Ok(())
}

/// Chain that databases created before chain tracking are assumed to hold.
///
/// Sieve only supported Ethereum mainnet before the `_sieve_chain` table
/// existed, so a database with prior sieve state but no identity row must
/// be mainnet.
const LEGACY_CHAIN: &str = "mainnet";

/// Check whether the database holds sieve state from earlier runs:
/// an advanced checkpoint or any stored block hashes. (The checkpoint and
/// block hashes are written atomically with indexed data, so a zero
/// checkpoint with no hashes means nothing was ever indexed.)
async fn has_prior_sieve_state(db: &Database) -> eyre::Result<bool> {
    let checkpoint: Option<i64> =
        sqlx::query_scalar("SELECT block_number FROM _sieve_checkpoints WHERE id = 1")
            .fetch_optional(db.pool())
            .await
            .wrap_err("failed to read checkpoint for chain identity")?;
    if checkpoint.unwrap_or(0) > 0 {
        return Ok(true);
    }
    let has_hashes: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM _sieve_block_hashes)")
        .fetch_one(db.pool())
        .await
        .wrap_err("failed to check block hashes for chain identity")?;
    Ok(has_hashes)
}

/// Bind this database to a chain identity, or verify an existing binding.
///
/// The first run stores `(chain, genesis_hash)`; every later run must match
/// it. This prevents mixing indexed state from different chains when the
/// same database URL is reused with a changed `chain` config.
///
/// A database that predates chain tracking (has sieve state but no identity
/// row) is treated as [`LEGACY_CHAIN`]: it binds normally under a mainnet
/// config and is refused for any other chain.
///
/// # Errors
///
/// Returns an error if the stored identity differs from the given one, if
/// a legacy database is reused for a non-mainnet chain, or on query failure.
pub async fn ensure_chain_identity(db: &Database, chain: &str, genesis: B256) -> eyre::Result<()> {
    let row = sqlx::query("SELECT chain, genesis_hash FROM _sieve_chain WHERE id = 1")
        .fetch_optional(db.pool())
        .await
        .wrap_err("failed to read chain identity")?;

    let Some(row) = row else {
        if chain != LEGACY_CHAIN && has_prior_sieve_state(db).await? {
            return Err(eyre::eyre!(
                "this database contains sieve state from before chain tracking; such databases \
                 are Ethereum {LEGACY_CHAIN}, but the config selects chain \"{chain}\"; use a \
                 separate database or run `sieve reset` to wipe it"
            ));
        }
        sqlx::query("INSERT INTO _sieve_chain (id, chain, genesis_hash) VALUES (1, $1, $2)")
            .bind(chain)
            .bind(genesis.as_slice())
            .execute(db.pool())
            .await
            .wrap_err("failed to store chain identity")?;
        info!(chain, genesis = %genesis, "bound database to chain");
        return Ok(());
    };

    let stored_chain: String = row.try_get("chain")?;
    let stored_genesis: Vec<u8> = row.try_get("genesis_hash")?;

    if stored_chain != chain || stored_genesis != genesis.as_slice() {
        let stored_genesis_hex = alloy_primitives::hex::encode(&stored_genesis);
        return Err(eyre::eyre!(
            "database is bound to chain \"{stored_chain}\" (genesis 0x{stored_genesis_hex}) but \
             config selects chain \"{chain}\" (genesis {genesis}); use a separate database or \
             run `sieve reset` to wipe it"
        ));
    }
    Ok(())
}

/// Create user-defined tables from resolved TOML config.
///
/// Runs `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`
/// for each resolved event. Safe to run repeatedly (idempotent DDL).
///
/// # Errors
///
/// Returns an error if any DDL statement fails.
pub async fn create_user_tables(db: &Database, events: &[ResolvedEvent]) -> eyre::Result<()> {
    for event in events {
        sqlx::raw_sql(&event.create_table_sql)
            .execute(db.pool())
            .await
            .wrap_err_with(|| format!("failed to create table '{}'", event.table_name))?;

        for index_sql in &event.create_indexes_sql {
            sqlx::raw_sql(index_sql)
                .execute(db.pool())
                .await
                .wrap_err_with(|| {
                    format!("failed to create index for table '{}'", event.table_name)
                })?;
        }

        info!(table = %event.table_name, "created user table");
    }
    Ok(())
}

/// Create native transfer tables from resolved TOML config.
///
/// Runs `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`
/// for each resolved transfer. Safe to run repeatedly (idempotent DDL).
///
/// # Errors
///
/// Returns an error if any DDL statement fails.
pub async fn create_transfer_tables(
    db: &Database,
    transfers: &[ResolvedTransfer],
) -> eyre::Result<()> {
    for transfer in transfers {
        sqlx::raw_sql(&transfer.create_table_sql)
            .execute(db.pool())
            .await
            .wrap_err_with(|| format!("failed to create table '{}'", transfer.table_name))?;

        for index_sql in &transfer.create_indexes_sql {
            sqlx::raw_sql(index_sql)
                .execute(db.pool())
                .await
                .wrap_err_with(|| {
                    format!("failed to create index for table '{}'", transfer.table_name)
                })?;
        }

        info!(table = %transfer.table_name, "created transfer table");
    }
    Ok(())
}

/// Create function call tables from resolved TOML config.
///
/// Runs `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`
/// for each resolved call. Safe to run repeatedly (idempotent DDL).
///
/// # Errors
///
/// Returns an error if any DDL statement fails.
pub async fn create_call_tables(db: &Database, calls: &[ResolvedCall]) -> eyre::Result<()> {
    for call in calls {
        sqlx::raw_sql(&call.create_table_sql)
            .execute(db.pool())
            .await
            .wrap_err_with(|| format!("failed to create table '{}'", call.table_name))?;

        for index_sql in &call.create_indexes_sql {
            sqlx::raw_sql(index_sql)
                .execute(db.pool())
                .await
                .wrap_err_with(|| {
                    format!("failed to create index for table '{}'", call.table_name)
                })?;
        }

        info!(table = %call.table_name, "created call table");
    }
    Ok(())
}

#[cfg(test)]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions in tests are idiomatic"
)]
mod tests {
    use super::*;

    async fn test_db() -> eyre::Result<Database> {
        let url = std::env::var("DATABASE_URL").wrap_err("DATABASE_URL not set")?;
        let db = Database::connect(&url).await?;
        create_internal_tables(&db).await?;
        Ok(db)
    }

    /// Reset all state the chain-identity logic reads: identity row,
    /// checkpoint, block hashes.
    ///
    /// NOTE: like the other DB tests here, the chain-identity tests mutate
    /// shared single-row tables — run them with `--test-threads=1`.
    async fn reset_identity_state(db: &Database) -> eyre::Result<()> {
        sqlx::query("DELETE FROM _sieve_chain")
            .execute(db.pool())
            .await
            .wrap_err("reset chain failed")?;
        sqlx::query("UPDATE _sieve_checkpoints SET block_number = 0 WHERE id = 1")
            .execute(db.pool())
            .await
            .wrap_err("reset checkpoint failed")?;
        sqlx::query("DELETE FROM _sieve_block_hashes")
            .execute(db.pool())
            .await
            .wrap_err("reset hashes failed")?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn chain_identity_fresh_db_binds_and_verifies() -> eyre::Result<()> {
        let db = test_db().await?;
        reset_identity_state(&db).await?;

        let base_genesis = alloy_primitives::B256::repeat_byte(0x0B);
        let main_genesis = alloy_primitives::B256::repeat_byte(0x0E);

        // A fresh database binds to base (no legacy state present).
        ensure_chain_identity(&db, "base", base_genesis).await?;

        // Matching identity passes on subsequent runs.
        ensure_chain_identity(&db, "base", base_genesis).await?;

        // A different chain is rejected.
        let result = ensure_chain_identity(&db, "mainnet", main_genesis).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("bound to chain"));

        // Same chain but different genesis is rejected too.
        let result = ensure_chain_identity(&db, "base", main_genesis).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("bound to chain"));

        reset_identity_state(&db).await
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn chain_identity_legacy_db_is_mainnet() -> eyre::Result<()> {
        let db = test_db().await?;
        reset_identity_state(&db).await?;

        // Simulate a pre-chain-tracking database: indexed state exists
        // (advanced checkpoint) but no identity row.
        sqlx::query("UPDATE _sieve_checkpoints SET block_number = 21000000 WHERE id = 1")
            .execute(db.pool())
            .await
            .wrap_err("simulate legacy failed")?;

        // Rebinding a legacy database to base must be refused.
        let result =
            ensure_chain_identity(&db, "base", alloy_primitives::B256::repeat_byte(0x0B)).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("before chain tracking"));

        // Under a mainnet config the legacy database binds and verifies.
        let main_genesis = alloy_primitives::B256::repeat_byte(0x0E);
        ensure_chain_identity(&db, "mainnet", main_genesis).await?;
        ensure_chain_identity(&db, "mainnet", main_genesis).await?;

        reset_identity_state(&db).await
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn chain_identity_legacy_detection_via_block_hashes() -> eyre::Result<()> {
        let db = test_db().await?;
        reset_identity_state(&db).await?;

        // Legacy state can also be just stored block hashes (checkpoint 0).
        let mut tx = db.begin().await?;
        store_block_hash(
            &mut tx,
            BlockNumber::new(77),
            alloy_primitives::B256::repeat_byte(0x77).as_slice(),
        )
        .await?;
        tx.commit().await.wrap_err("commit failed")?;

        let result =
            ensure_chain_identity(&db, "base", alloy_primitives::B256::repeat_byte(0x0B)).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("before chain tracking"));

        reset_identity_state(&db).await
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
        update_checkpoint(&mut tx, BlockNumber::new(21_000_100)).await?;
        tx.commit().await.wrap_err("commit failed")?;

        // Should now return the block number
        let checkpoint = db.last_checkpoint().await?;
        assert_eq!(checkpoint, Some(BlockNumber::new(21_000_100)));

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
        store_block_hash(&mut tx, BlockNumber::new(99_999), hash_a.as_slice()).await?;
        tx.commit().await.wrap_err("commit failed")?;

        let stored = db.get_block_hash(BlockNumber::new(99_999)).await?;
        assert_eq!(stored, Some(hash_a));

        // Overwrite with a different hash (simulates reorg re-indexing)
        let mut tx = db.begin().await?;
        store_block_hash(&mut tx, BlockNumber::new(99_999), hash_b.as_slice()).await?;
        tx.commit().await.wrap_err("commit failed")?;

        let stored = db.get_block_hash(BlockNumber::new(99_999)).await?;
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
            store_block_hash(&mut tx, BlockNumber::new(block), hash.as_slice()).await?;
            update_checkpoint(&mut tx, BlockNumber::new(block)).await?;
            tx.commit().await.wrap_err("commit failed")?;
        }

        // Rollback to 105
        let mut tx = db.begin().await?;
        rollback_to(&mut tx, BlockNumber::new(105)).await?;
        tx.commit().await.wrap_err("commit failed")?;

        // Blocks 100-105 should still have hashes
        for block in 100..=105u64 {
            let stored = db.get_block_hash(BlockNumber::new(block)).await?;
            assert!(stored.is_some(), "block {block} hash should exist");
        }

        // Blocks 106-110 should be gone
        for block in 106..=110u64 {
            let stored = db.get_block_hash(BlockNumber::new(block)).await?;
            assert!(stored.is_none(), "block {block} hash should be deleted");
        }

        // Checkpoint should be 105
        let checkpoint = db.last_checkpoint().await?;
        assert_eq!(checkpoint, Some(BlockNumber::new(105)));

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
        update_checkpoint(&mut tx, BlockNumber::new(100)).await?;
        tx.commit().await.wrap_err("commit failed")?;

        // Try to set checkpoint to 50 (should be ignored by GREATEST)
        let mut tx = db.begin().await?;
        update_checkpoint(&mut tx, BlockNumber::new(50)).await?;
        tx.commit().await.wrap_err("commit failed")?;

        // Checkpoint should still be 100
        let checkpoint = db.last_checkpoint().await?;
        assert_eq!(checkpoint, Some(BlockNumber::new(100)));

        // Set checkpoint to 200 (should advance)
        let mut tx = db.begin().await?;
        update_checkpoint(&mut tx, BlockNumber::new(200)).await?;
        tx.commit().await.wrap_err("commit failed")?;

        let checkpoint = db.last_checkpoint().await?;
        assert_eq!(checkpoint, Some(BlockNumber::new(200)));

        // Clean up
        sqlx::query("UPDATE _sieve_checkpoints SET block_number = 0 WHERE id = 1")
            .execute(db.pool())
            .await
            .wrap_err("cleanup failed")?;

        Ok(())
    }
}
