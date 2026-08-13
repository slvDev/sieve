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
use crate::toml_config::{ResolvedCall, ResolvedEvent, ResolvedFactory, ResolvedTransfer};
use crate::types::BlockNumber;
use alloy_primitives::{Address, B256};
use eyre::WrapErr;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::HashMap;
use tracing::{info, warn};

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

/// Read the stored parent hash for a block, if known.
///
/// Returns `None` when the row is absent or was written by a version that
/// predates parent tracking.
pub async fn get_block_parent_hash(
    db: &Database,
    block_number: BlockNumber,
) -> eyre::Result<Option<B256>> {
    let row: Option<(Option<Vec<u8>>,)> =
        sqlx::query_as("SELECT parent_hash FROM _sieve_block_hashes WHERE block_number = $1")
            .bind(block_number.as_u64() as i64)
            .fetch_optional(db.pool())
            .await
            .wrap_err("failed to read block parent hash")?;

    match row {
        Some((Some(bytes),)) => {
            let hash = B256::try_from(bytes.as_slice())
                .map_err(|_| eyre::eyre!("invalid parent hash length in DB"))?;
            Ok(Some(hash))
        }
        _ => Ok(None),
    }
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
    parent_hashes: &[Vec<u8>],
) -> eyre::Result<()> {
    if block_numbers.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO _sieve_block_hashes (block_number, block_hash, parent_hash) \
         SELECT * FROM UNNEST($1::BIGINT[], $2::BYTEA[], $3::BYTEA[]) \
         ON CONFLICT (block_number) DO UPDATE \
         SET block_hash = EXCLUDED.block_hash, parent_hash = EXCLUDED.parent_hash",
    )
    .bind(block_numbers)
    .bind(block_hashes)
    .bind(parent_hashes)
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

    // Factory coverage may never claim blocks the checkpoint does not.
    // Clamp every row — lowering inactive factories' coverage too is
    // always conservative.
    sqlx::query("UPDATE _sieve_factories SET covered_through = LEAST(covered_through, $1)")
        .bind(block_number.as_u64() as i64)
        .execute(&mut **tx)
        .await
        .wrap_err("failed to clamp factory coverage after rollback")?;

    // Move the verified frontier in lockstep with the checkpoint. Update
    // to the stored hash of the rollback target, or clear the marker when
    // the target has no stored hash. NEVER inserts — a legacy database
    // (no marker) must not gain one through a rollback, so it still fails
    // startup verification.
    let target_hash: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT block_hash FROM _sieve_block_hashes WHERE block_number = $1")
            .bind(block_number.as_u64() as i64)
            .fetch_optional(&mut **tx)
            .await
            .wrap_err("failed to read rollback target hash")?;
    match target_hash {
        Some(hash) => {
            sqlx::query(
                "UPDATE _sieve_canonical SET verified_through = $1, verified_hash = $2 \
                 WHERE id = 1",
            )
            .bind(block_number.as_u64() as i64)
            .bind(hash)
            .execute(&mut **tx)
            .await
            .wrap_err("failed to move verified frontier after rollback")?;
        }
        None => {
            sqlx::query("DELETE FROM _sieve_canonical WHERE id = 1")
                .execute(&mut **tx)
                .await
                .wrap_err("failed to clear verified frontier after rollback")?;
        }
    }

    Ok(())
}

/// Load persisted factory children from the database into the config.
///
/// Called at startup to restore dynamically discovered child contracts.
/// Every row is resolved through its FULL factory identity (address,
/// creation selector, child contract name) against the currently
/// configured factories — a display name alone must never route children.
/// Returns the number of children loaded.
///
/// # Errors
///
/// Returns an error if the query fails or a row's identity does not
/// match a configured factory.
pub async fn load_factory_children(
    db: &Database,
    config: &IndexConfig,
    factories: &[ResolvedFactory],
) -> eyre::Result<u64> {
    let rows = sqlx::query(
        "SELECT factory_name, factory_address, creation_selector, child_address \
         FROM _sieve_factory_children",
    )
    .fetch_all(db.pool())
    .await
    .wrap_err("failed to load factory children")?;

    let mut count = 0u64;
    for row in &rows {
        let (address, contract_idx) = parse_persisted_child(config, factories, row)?;
        if config.register_factory_child(address, contract_idx) {
            count = count.saturating_add(1);
        }
    }

    if count > 0 {
        info!(count, "loaded factory children from database");
    }
    Ok(count)
}

/// Validate that every configured factory's history is actually covered
/// by this database, and record coverage rows for new factories.
///
/// Coverage is keyed by factory identity — address, creation-event
/// selector, and child contract name — not display name, and tracks
/// `covered_through`: the highest block indexed while the factory was
/// active. Startup refuses any factory whose creation events could have
/// been skipped:
///
/// - an identity new to a database already indexed to or past the
///   factory's `start_block` (resume continues one past the checkpoint,
///   so "to" is a gap too),
/// - a known factory whose `start_block` changed in either direction
///   (immutable: lowering leaves earlier creation events unscanned,
///   raising breaks reorg rediscovery below the new start),
/// - a known factory whose extraction rule changed — the full creation
///   event layout (names, types, `indexed` flags) plus selected
///   parameter, not just the selector (which hashes types only),
/// - a known factory that was inactive while the checkpoint advanced
///   into its range (removed and later re-added) — that range is missing
///   both creation events AND events from already-known children, so it
///   cannot be asserted away,
/// - a sync that would begin past the factory's next uncovered block
///   (e.g. a `--start-block` override jumping over the factory's range),
///   which would otherwise let `covered_through` certify skipped blocks.
///
/// `configured_start` is the block sync will begin at before checkpoint
/// resume (CLI override or config minimum).
///
/// With `assume_coverage` (`--assume-factory-coverage`), ONLY the
/// missing-record refusal is downgraded to a recorded assertion: the
/// factory is marked covered through the current checkpoint. This is the
/// explicit upgrade path for databases created before coverage tracking,
/// where the operator asserts the factory was configured continuously
/// since its `start_block`. Every other refusal stands regardless of the
/// flag.
///
/// # Errors
///
/// Returns an error on a coverage gap or query failure.
pub async fn ensure_factory_coverage(
    db: &Database,
    factories: &[ResolvedFactory],
    configured_start: u64,
    assume_coverage: bool,
) -> eyre::Result<()> {
    if factories.is_empty() {
        return Ok(());
    }
    let checkpoint_opt = db.last_checkpoint().await?;
    let checkpoint = checkpoint_opt.map_or(0, BlockNumber::as_u64);
    let prior_state = has_prior_sieve_state(db).await?;
    // Mirror of `resolve_effective_start`: the first block this run will
    // actually process.
    let sync_start = match checkpoint_opt {
        Some(cp) if cp.as_u64() >= configured_start => cp.as_u64().saturating_add(1),
        _ => configured_start,
    };

    for factory in factories {
        check_factory_coverage(
            db,
            factory,
            checkpoint,
            prior_state,
            sync_start,
            assume_coverage,
        )
        .await?;
    }
    Ok(())
}

/// Refuse a sync that would begin past the factory's next uncovered block.
///
/// `covered_through` only ever advances to the committed checkpoint, so if
/// the run starts beyond `max(start_block, covered_through + 1)` the
/// skipped range would be certified as covered without ever being scanned.
fn check_sync_start(
    factory: &ResolvedFactory,
    covered_through: u64,
    sync_start: u64,
) -> eyre::Result<()> {
    let required_next = factory.start_block.max(covered_through.saturating_add(1));
    if sync_start > required_next {
        return Err(eyre::eyre!(
            "factory \"{}\": sync would begin at block {sync_start}, skipping blocks \
             {required_next}..={} that the factory has not covered; creation events there \
             would never be scanned — lower --start-block (or the configured start_blocks) so \
             sync begins at or before block {required_next}, or use a fresh database",
            factory.child_contract_name,
            sync_start.saturating_sub(1),
        ));
    }
    Ok(())
}

/// Validate one factory against its stored coverage row, inserting the
/// row when the identity is new. See [`ensure_factory_coverage`].
async fn check_factory_coverage(
    db: &Database,
    factory: &ResolvedFactory,
    checkpoint: u64,
    prior_state: bool,
    sync_start: u64,
    assume_coverage: bool,
) -> eyre::Result<()> {
    let row: Option<(String, i64, i64)> = sqlx::query_as(
        "SELECT extraction_fingerprint, start_block, covered_through FROM _sieve_factories \
         WHERE factory_address = $1 AND creation_selector = $2 AND child_contract_name = $3",
    )
    .bind(factory.factory_address.as_slice())
    .bind(factory.creation_selector.as_slice())
    .bind(&factory.child_contract_name)
    .fetch_optional(db.pool())
    .await
    .wrap_err("failed to read factory coverage")?;

    let Some((stored_fingerprint, stored_start, covered_through)) = row else {
        // A new row starts covered through the checkpoint.
        check_sync_start(factory, checkpoint, sync_start)?;
        return register_new_factory(db, factory, checkpoint, prior_state, assume_coverage).await;
    };

    let fingerprint = factory.extraction_fingerprint();
    if stored_fingerprint != fingerprint {
        return Err(eyre::eyre!(
            "factory \"{}\": the child-extraction rule changed from \"{stored_fingerprint}\" \
             to \"{fingerprint}\"; children persisted under the old rule may be wrong and \
             past blocks were scanned with different extraction — use a fresh database or run \
             `sieve reset`",
            factory.child_contract_name,
        ));
    }
    if (factory.start_block as i64) != stored_start {
        return Err(eyre::eyre!(
            "factory \"{}\": start_block changed from {stored_start} to {}; start_block is \
             immutable for an indexed factory — lowering would leave earlier creation events \
             unscanned, and raising would stop reorg recovery from rediscovering children \
             created below the new start — use a fresh database or run `sieve reset`",
            factory.child_contract_name,
            factory.start_block,
        ));
    }
    let covered_through = u64::try_from(covered_through).unwrap_or(0);
    if checkpoint > covered_through && checkpoint >= factory.start_block {
        let gap_start = factory.start_block.max(covered_through.saturating_add(1));
        return Err(eyre::eyre!(
            "factory \"{}\": this database advanced to block {checkpoint} while the factory \
             was not active (coverage ends at block {covered_through}); blocks \
             {gap_start}..={checkpoint} are missing both creation events and events from \
             already-known children, and cannot be backfilled — use a fresh database or run \
             `sieve reset`",
            factory.child_contract_name,
        ));
    }
    check_sync_start(factory, covered_through, sync_start)
}

/// Insert the coverage row for a factory identity seen for the first time.
///
/// Refused when the database has already indexed to or into the factory's
/// range, unless the operator asserts coverage with
/// `--assume-factory-coverage`. The row always carries the CURRENT
/// config's identity fields — there is no stored state to overwrite.
async fn register_new_factory(
    db: &Database,
    factory: &ResolvedFactory,
    checkpoint: u64,
    prior_state: bool,
    assume_coverage: bool,
) -> eyre::Result<()> {
    if prior_state && checkpoint >= factory.start_block {
        if !assume_coverage {
            return Err(eyre::eyre!(
                "factory \"{}\" (factory {}, event \"{}\") has no coverage record, but this \
                 database is already indexed to block {checkpoint} and the factory starts at \
                 block {}; creation events in blocks {}..={checkpoint} were never scanned, so \
                 existing children would be silently missing — use a fresh database, run \
                 `sieve reset`, or rerun once with --assume-factory-coverage if this factory \
                 was in fact indexed continuously (e.g. a database from before coverage \
                 tracking)",
                factory.child_contract_name,
                factory.factory_address,
                factory.creation_event.name,
                factory.start_block,
                factory.start_block,
            ));
        }
        warn!(
            factory = %factory.child_contract_name,
            covered_through = checkpoint,
            "recording assumed factory coverage (--assume-factory-coverage)"
        );
        // The same assertion binds this factory's pre-identity-tracking
        // children (NULL identity columns) to the asserted identity, so
        // they load under it from now on.
        let stamped = sqlx::query(
            "UPDATE _sieve_factory_children SET factory_address = $2, creation_selector = $3 \
             WHERE factory_name = $1 AND factory_address IS NULL",
        )
        .bind(&factory.child_contract_name)
        .bind(factory.factory_address.as_slice())
        .bind(factory.creation_selector.as_slice())
        .execute(db.pool())
        .await
        .wrap_err("failed to bind legacy factory children")?;
        if stamped.rows_affected() > 0 {
            warn!(
                factory = %factory.child_contract_name,
                children = stamped.rows_affected(),
                "bound legacy factory children to the asserted identity"
            );
        }
    }
    sqlx::query(
        "INSERT INTO _sieve_factories (factory_address, creation_selector, \
         child_contract_name, extraction_fingerprint, start_block, covered_through) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(factory.factory_address.as_slice())
    .bind(factory.creation_selector.as_slice())
    .bind(&factory.child_contract_name)
    .bind(factory.extraction_fingerprint())
    .bind(factory.start_block as i64)
    .bind(checkpoint as i64)
    .execute(db.pool())
    .await
    .wrap_err("failed to record factory coverage")?;
    Ok(())
}

/// Bind-ready identity keys for the configured factories, precomputed
/// once so every batch commit can advance coverage without re-deriving
/// them.
#[derive(Debug, Default)]
pub struct FactoryCoverageKeys {
    addresses: Vec<Vec<u8>>,
    selectors: Vec<Vec<u8>>,
    names: Vec<String>,
}

impl FactoryCoverageKeys {
    /// Build identity keys from the resolved factory configs.
    #[must_use]
    pub fn new(factories: &[ResolvedFactory]) -> Self {
        Self {
            addresses: factories
                .iter()
                .map(|f| f.factory_address.as_slice().to_vec())
                .collect(),
            selectors: factories
                .iter()
                .map(|f| f.creation_selector.as_slice().to_vec())
                .collect(),
            names: factories
                .iter()
                .map(|f| f.child_contract_name.clone())
                .collect(),
        }
    }

    /// Whether no factories are configured.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.addresses.is_empty()
    }
}

/// Advance `covered_through` for the given factory identities.
///
/// Must run inside the same transaction as [`update_checkpoint`]: a
/// checkpoint advance while a factory is active has to advance its
/// coverage atomically, otherwise a later restart would misread the
/// factory as having been inactive over those blocks. Rows not named in
/// `keys` (factories removed from the config) deliberately stay behind —
/// that widening gap is what makes a later re-add refusable. Uses
/// `GREATEST` (like the checkpoint) so it never moves backward here;
/// rollbacks lower it via [`rollback_to`].
///
/// # Errors
///
/// Returns an error if the UPDATE query fails.
pub async fn advance_factory_coverage(
    tx: &mut Transaction<'_, Postgres>,
    keys: &FactoryCoverageKeys,
    block_number: BlockNumber,
) -> eyre::Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "UPDATE _sieve_factories AS f SET covered_through = GREATEST(f.covered_through, $4) \
         FROM UNNEST($1::BYTEA[], $2::BYTEA[], $3::TEXT[]) \
         AS k(factory_address, creation_selector, child_contract_name) \
         WHERE f.factory_address = k.factory_address \
           AND f.creation_selector = k.creation_selector \
           AND f.child_contract_name = k.child_contract_name",
    )
    .bind(&keys.addresses)
    .bind(&keys.selectors)
    .bind(&keys.names)
    .bind(block_number.as_u64() as i64)
    .execute(&mut **tx)
    .await
    .wrap_err("failed to advance factory coverage")?;
    Ok(())
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
pub async fn rebuild_factory_children(
    db: &Database,
    config: &IndexConfig,
    factories: &[ResolvedFactory],
) -> eyre::Result<u64> {
    let rows = sqlx::query(
        "SELECT factory_name, factory_address, creation_selector, child_address \
         FROM _sieve_factory_children",
    )
    .fetch_all(db.pool())
    .await
    .wrap_err("failed to load factory children for rebuild")?;

    let mut committed: HashMap<Address, usize> = HashMap::new();
    for row in &rows {
        let (address, contract_idx) = parse_persisted_child(config, factories, row)?;
        committed.insert(address, contract_idx);
    }

    let count = committed.len() as u64;
    config.replace_factory_children(committed);
    Ok(count)
}

/// Validate a persisted factory-child row, resolving its contract index
/// through the FULL factory identity.
///
/// A row is only accepted when a currently configured factory matches its
/// stored `(factory_address, creation_selector, child_contract_name)`
/// exactly. Everything else is FATAL — silently skipping rows would drop
/// persisted children while the checkpoint stays advanced, and routing by
/// name alone could hand a different factory's children to the wrong
/// ABI/handlers:
///
/// - no configured factory with the row's name: the factory was removed
///   or renamed — migrate or delete the rows;
/// - identity mismatch: the name was reused for a DIFFERENT factory —
///   the children belong to the old identity and must not be loaded;
/// - missing identity columns: rows from before identity tracking —
///   rerun once with `--assume-factory-coverage` to bind them.
///
/// # Errors
///
/// Returns an error for malformed rows or any identity mismatch.
fn parse_persisted_child(
    config: &IndexConfig,
    factories: &[ResolvedFactory],
    row: &sqlx::postgres::PgRow,
) -> eyre::Result<(Address, usize)> {
    let factory_name: &str = row.try_get("factory_name")?;
    let stored_address: Option<Vec<u8>> = row.try_get("factory_address")?;
    let stored_selector: Option<Vec<u8>> = row.try_get("creation_selector")?;
    let child_bytes: Vec<u8> = row.try_get("child_address")?;

    if child_bytes.len() != 20 {
        return Err(eyre::eyre!(
            "corrupt child address ({} bytes) for factory \"{factory_name}\" in \
             _sieve_factory_children",
            child_bytes.len()
        ));
    }
    let child_address = Address::from_slice(&child_bytes);

    let Some(factory) = factories
        .iter()
        .find(|f| f.child_contract_name == factory_name)
    else {
        return Err(eyre::eyre!(
            "persisted factory children reference factory \"{factory_name}\", which is not in \
             the current config; if the contract was renamed, migrate the rows \
             (UPDATE _sieve_factory_children SET factory_name = '<new>' WHERE factory_name = \
             '{factory_name}'); if it was removed intentionally, delete them \
             (DELETE FROM _sieve_factory_children WHERE factory_name = '{factory_name}')"
        ));
    };

    match (stored_address, stored_selector) {
        (Some(addr), Some(sel))
            if addr == factory.factory_address.as_slice()
                && sel == factory.creation_selector.as_slice() => {}
        (Some(_), Some(_)) => {
            return Err(eyre::eyre!(
                "persisted children of \"{factory_name}\" were discovered by a DIFFERENT \
                 factory identity than the one now configured (factory {}, event \"{}\"); \
                 loading them would route another factory's children through this ABI and \
                 handlers — use a fresh database or run `sieve reset`",
                factory.factory_address,
                factory.creation_event.name,
            ));
        }
        _ => {
            return Err(eyre::eyre!(
                "persisted children of \"{factory_name}\" predate factory identity tracking; \
                 rerun once with --assume-factory-coverage to bind them to the configured \
                 factory (only if this factory was configured continuously), or use a fresh \
                 database"
            ));
        }
    }

    let Some(contract_idx) = config.contracts.iter().position(|c| c.name == factory_name) else {
        return Err(eyre::eyre!(
            "factory \"{factory_name}\" has no matching contract entry in the current config"
        ));
    };

    Ok((child_address, contract_idx))
}

/// Persist a newly discovered factory child in the database, bound to
/// the full identity of the factory that discovered it.
///
/// # Errors
///
/// Returns an error if the INSERT fails.
pub async fn store_factory_child(
    tx: &mut Transaction<'_, Postgres>,
    discovery: &crate::filter::FactoryDiscovery,
) -> eyre::Result<()> {
    sqlx::query(
        "INSERT INTO _sieve_factory_children \
         (factory_name, factory_address, creation_selector, child_address, block_number) \
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT (child_address) DO NOTHING",
    )
    .bind(&discovery.child_contract_name)
    .bind(discovery.factory_address.as_slice())
    .bind(discovery.creation_selector.as_slice())
    .bind(discovery.child_address.as_slice())
    .bind(discovery.block_number as i64)
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
    configured_chain: &str,
    events: &[ResolvedEvent],
    transfers: &[ResolvedTransfer],
    calls: &[ResolvedCall],
) -> eyre::Result<()> {
    // Every Sieve-owned table: the persisted registry (covers tables from
    // earlier configs, e.g. before a chain switch) plus the tables named
    // in the current config. Databases that predate the registry cannot
    // enumerate their old tables, so a chain-SWITCH reset on them would
    // silently leave the old chain's tables behind — refuse it. The source
    // chain comes from the identity row when present (Base support predates
    // table tracking), else prior state implies legacy mainnet.
    let registry = owned_tables(db).await?;
    if registry.is_none() {
        let source_chain = match stored_chain_tolerant(db).await? {
            Some(chain) => Some(chain),
            None => has_prior_sieve_state_tolerant(db)
                .await?
                .then(|| LEGACY_CHAIN.to_owned()),
        };
        if let Some(source) = source_chain {
            if source != configured_chain {
                return Err(eyre::eyre!(
                    "this database predates table tracking and holds chain \"{source}\" state; \
                     a reset cannot locate all of its old tables, so switching it to chain \
                     \"{configured_chain}\" could mix chain data — use a fresh database instead"
                ));
            }
        }
    }
    let mut tables: std::collections::BTreeSet<String> =
        registry.unwrap_or_default().into_iter().collect();
    tables.extend(events.iter().map(|e| e.table_name.clone()));
    tables.extend(transfers.iter().map(|t| t.table_name.clone()));
    tables.extend(calls.iter().map(|c| c.table_name.clone()));

    for table in &tables {
        let sql = format!("DROP TABLE IF EXISTS {table} CASCADE");
        sqlx::raw_sql(&sql)
            .execute(db.pool())
            .await
            .wrap_err_with(|| format!("failed to drop table '{table}'"))?;
    }

    // Internal tables
    for table in [
        "_sieve_factory_children",
        "_sieve_block_hashes",
        "_sieve_checkpoints",
        "_sieve_chain",
        "_sieve_tables",
        "_sieve_factories",
        "_sieve_canonical",
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
///
/// `parent_hash` enables cross-batch adjacency verification; it is
/// nullable only for rows written by versions that predate the column.
pub const BLOCK_HASHES_DDL: &str = "\
CREATE TABLE IF NOT EXISTS _sieve_block_hashes (
    block_number BIGINT PRIMARY KEY,
    block_hash BYTEA NOT NULL,
    parent_hash BYTEA
)";

/// DDL for the `_sieve_factory_children` table.
///
/// `factory_address` and `creation_selector` bind each child to the full
/// identity of the factory that discovered it; they are nullable only for
/// rows written by versions that predate identity tracking (bound once
/// via `--assume-factory-coverage`).
pub const FACTORY_CHILDREN_DDL: &str = "\
CREATE TABLE IF NOT EXISTS _sieve_factory_children (
    id BIGSERIAL PRIMARY KEY,
    factory_name TEXT NOT NULL,
    factory_address BYTEA,
    creation_selector BYTEA,
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

/// DDL for the `_sieve_factories` coverage registry.
///
/// One row per factory identity `(factory_address, creation_selector,
/// child_contract_name)` — the on-chain facts that determine which
/// children get discovered, not the display name. `covered_through` is
/// the highest block this database has indexed WITH the factory active;
/// it advances atomically with every checkpoint update while the factory
/// is configured and is clamped by rollbacks. Any range the checkpoint
/// crossed without the factory active is a coverage gap — creation events
/// there were never scanned — and startup refuses the factory instead of
/// silently missing children.
pub const FACTORY_COVERAGE_DDL: &str = "\
CREATE TABLE IF NOT EXISTS _sieve_factories (
    factory_address BYTEA NOT NULL,
    creation_selector BYTEA NOT NULL,
    child_contract_name TEXT NOT NULL,
    extraction_fingerprint TEXT NOT NULL,
    start_block BIGINT NOT NULL,
    covered_through BIGINT NOT NULL,
    PRIMARY KEY (factory_address, creation_selector, child_contract_name)
)";

/// DDL for the `_sieve_tables` registry of Sieve-owned user tables.
///
/// `sieve reset` / `--fresh` must drop every table Sieve ever created,
/// not just the ones named in the currently loaded config — otherwise a
/// chain switch could leave stale tables behind and later mix chain data.
pub const OWNED_TABLES_DDL: &str = "\
CREATE TABLE IF NOT EXISTS _sieve_tables (
    table_name TEXT PRIMARY KEY
)";

/// DDL for the `_sieve_canonical` verified-frontier marker (single row).
///
/// Records the highest block whose committed hash was verified against a
/// peer quorum. Startup verification writes it after confirming the
/// checkpoint hash is canonical; it makes "this state was quorum-verified"
/// an explicit, inspectable fact rather than an implicit assumption.
pub const CANONICAL_FRONTIER_DDL: &str = "\
CREATE TABLE IF NOT EXISTS _sieve_canonical (
    id SMALLINT PRIMARY KEY DEFAULT 1,
    verified_through BIGINT NOT NULL,
    verified_hash BYTEA NOT NULL
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

    // Older databases predate the parent_hash column — add it in place.
    sqlx::raw_sql("ALTER TABLE _sieve_block_hashes ADD COLUMN IF NOT EXISTS parent_hash BYTEA")
        .execute(db.pool())
        .await
        .wrap_err("failed to add parent_hash column")?;

    sqlx::raw_sql(&format!("{FACTORY_CHILDREN_DDL};"))
        .execute(db.pool())
        .await
        .wrap_err("failed to create _sieve_factory_children")?;

    // Older databases predate factory identity tracking — add the
    // identity columns in place (rows stay NULL until bound explicitly).
    sqlx::raw_sql(
        "ALTER TABLE _sieve_factory_children ADD COLUMN IF NOT EXISTS factory_address BYTEA;\n\
         ALTER TABLE _sieve_factory_children ADD COLUMN IF NOT EXISTS creation_selector BYTEA",
    )
    .execute(db.pool())
    .await
    .wrap_err("failed to add factory identity columns")?;

    sqlx::raw_sql(&format!("{CHAIN_IDENTITY_DDL};"))
        .execute(db.pool())
        .await
        .wrap_err("failed to create _sieve_chain")?;

    sqlx::raw_sql(&format!("{OWNED_TABLES_DDL};"))
        .execute(db.pool())
        .await
        .wrap_err("failed to create _sieve_tables")?;

    sqlx::raw_sql(&format!("{FACTORY_COVERAGE_DDL};"))
        .execute(db.pool())
        .await
        .wrap_err("failed to create _sieve_factories")?;

    sqlx::raw_sql(&format!("{CANONICAL_FRONTIER_DDL};"))
        .execute(db.pool())
        .await
        .wrap_err("failed to create _sieve_canonical")?;

    info!("internal tables ready");
    Ok(())
}

/// Record a user table as Sieve-owned so future resets can drop it even
/// when it is no longer part of the loaded config.
async fn register_owned_table(db: &Database, table_name: &str) -> eyre::Result<()> {
    sqlx::query("INSERT INTO _sieve_tables (table_name) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(table_name)
        .execute(db.pool())
        .await
        .wrap_err_with(|| format!("failed to register owned table '{table_name}'"))?;
    Ok(())
}

/// Read all Sieve-owned table names recorded by previous runs.
///
/// Returns `None` when the registry does not exist yet (databases created
/// before table tracking) — callers must treat such databases as having an
/// unknown table set.
async fn owned_tables(db: &Database) -> eyre::Result<Option<Vec<String>>> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass('_sieve_tables') IS NOT NULL")
        .fetch_one(db.pool())
        .await
        .wrap_err("failed to check _sieve_tables existence")?;
    if !exists {
        return Ok(None);
    }
    let rows: Vec<(String,)> = sqlx::query_as("SELECT table_name FROM _sieve_tables")
        .fetch_all(db.pool())
        .await
        .wrap_err("failed to read owned tables")?;
    Ok(Some(rows.into_iter().map(|(name,)| name).collect()))
}

/// Read the bound chain from `_sieve_chain`, tolerating the table not
/// existing yet (the reset path runs before any DDL).
async fn stored_chain_tolerant(db: &Database) -> eyre::Result<Option<String>> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass('_sieve_chain') IS NOT NULL")
        .fetch_one(db.pool())
        .await
        .wrap_err("failed to check _sieve_chain existence")?;
    if !exists {
        return Ok(None);
    }
    let chain: Option<String> = sqlx::query_scalar("SELECT chain FROM _sieve_chain WHERE id = 1")
        .fetch_optional(db.pool())
        .await
        .wrap_err("failed to read stored chain")?;
    Ok(chain)
}

/// Like [`has_prior_sieve_state`], but tolerates the internal tables not
/// existing yet (the reset path runs before any DDL).
async fn has_prior_sieve_state_tolerant(db: &Database) -> eyre::Result<bool> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass('_sieve_checkpoints') IS NOT NULL")
        .fetch_one(db.pool())
        .await
        .wrap_err("failed to check _sieve_checkpoints existence")?;
    if !exists {
        return Ok(false);
    }
    has_prior_sieve_state(db).await
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

impl Database {
    /// Whether this database already holds indexed state (an advanced
    /// checkpoint or any stored block hashes). A fresh database returns
    /// `false` — the canonical stage requires no left seam for its first
    /// block; an existing database requires one.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn has_indexed_state(&self) -> eyre::Result<bool> {
        has_prior_sieve_state(self).await
    }

    /// Read the quorum-verified frontier `(block, hash)`, if recorded.
    ///
    /// Its PRESENCE is the trust boundary: a database Sieve built through
    /// the canonical stage always has it (advanced atomically with every
    /// commit); a database indexed before canonical verification does not,
    /// and startup refuses such state.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn verified_frontier(&self) -> eyre::Result<Option<(u64, B256)>> {
        let row: Option<(i64, Vec<u8>)> = sqlx::query_as(
            "SELECT verified_through, verified_hash FROM _sieve_canonical WHERE id = 1",
        )
        .fetch_optional(self.pool())
        .await
        .wrap_err("failed to read verified frontier")?;
        match row {
            Some((block, bytes)) => {
                let hash = B256::try_from(bytes.as_slice())
                    .map_err(|_| eyre::eyre!("invalid verified frontier hash length"))?;
                Ok(Some((block.max(0) as u64, hash)))
            }
            None => Ok(None),
        }
    }
}

/// Advance the quorum-verified frontier within a commit transaction.
///
/// Called in the SAME transaction as [`update_checkpoint`], so the marker
/// and the checkpoint always move together: a committed segment is
/// verified through its highest committed block, atomically. Upserts, so
/// the first commit on a fresh database establishes the marker.
///
/// # Errors
///
/// Returns an error if the write fails.
pub async fn advance_verified_frontier(
    tx: &mut Transaction<'_, Postgres>,
    block: BlockNumber,
    hash: &B256,
) -> eyre::Result<()> {
    sqlx::query(
        "INSERT INTO _sieve_canonical (id, verified_through, verified_hash) \
         VALUES (1, $1, $2) \
         ON CONFLICT (id) DO UPDATE SET verified_through = EXCLUDED.verified_through, \
         verified_hash = EXCLUDED.verified_hash",
    )
    .bind(block.as_u64() as i64)
    .bind(hash.as_slice())
    .execute(&mut **tx)
    .await
    .wrap_err("failed to advance verified frontier")?;
    Ok(())
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
        register_owned_table(db, &event.table_name).await?;
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
        register_owned_table(db, &transfer.table_name).await?;
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
        register_owned_table(db, &call.table_name).await?;
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

    /// Build a minimal `ResolvedFactory` for coverage tests.
    fn test_factory(
        name: &str,
        start_block: u64,
    ) -> eyre::Result<crate::toml_config::ResolvedFactory> {
        test_factory_at(0xFA, name, start_block)
    }

    /// Like [`test_factory`] but with a chosen factory address byte.
    fn test_factory_at(
        addr_byte: u8,
        name: &str,
        start_block: u64,
    ) -> eyre::Result<crate::toml_config::ResolvedFactory> {
        let abi_json = r#"[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"pool","type":"address"}],"name":"PoolCreated","type":"event"}]"#;
        let abi: alloy_json_abi::JsonAbi =
            serde_json::from_str(abi_json).map_err(|e| eyre::eyre!("abi parse: {e}"))?;
        let event = abi
            .events
            .get("PoolCreated")
            .and_then(|v| v.first())
            .ok_or_else(|| eyre::eyre!("no event"))?;
        Ok(crate::toml_config::ResolvedFactory {
            factory_address: Address::repeat_byte(addr_byte),
            creation_selector: event.selector(),
            creation_event: event.clone(),
            child_address_param: "pool".to_owned(),
            child_contract_name: name.to_owned(),
            start_block,
        })
    }

    /// Build a `FactoryDiscovery` as the scanner would for this factory.
    fn test_discovery(
        factory: &crate::toml_config::ResolvedFactory,
        child: Address,
        block: u64,
    ) -> crate::filter::FactoryDiscovery {
        crate::filter::FactoryDiscovery {
            child_contract_name: factory.child_contract_name.clone(),
            factory_address: factory.factory_address,
            creation_selector: factory.creation_selector,
            child_address: child,
            block_number: block,
        }
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn factory_children_persist_restart_and_rollback() -> eyre::Result<()> {
        let db = test_db().await?;
        reset_identity_state(&db).await?;
        sqlx::query("DELETE FROM _sieve_factory_children")
            .execute(db.pool())
            .await
            .wrap_err("clear children failed")?;

        let config = crate::config::usdc_transfer_config()?;
        let factory = test_factory("USDC", 0)?;
        let child = Address::repeat_byte(0xC1);

        // Persist a child discovered at block 500, bound to the identity.
        let mut tx = db.begin().await?;
        store_factory_child(&mut tx, &test_discovery(&factory, child, 500)).await?;
        tx.commit().await.wrap_err("commit failed")?;

        // Restart path: loading registers it in-memory.
        assert_eq!(
            load_factory_children(&db, &config, std::slice::from_ref(&factory)).await?,
            1
        );
        assert!(config.contract_for_address(&child).is_some());

        // A DIFFERENT factory identity with the same display name must
        // never inherit the persisted children.
        let impostor = test_factory_at(0xFB, "USDC", 0)?;
        let result = load_factory_children(&db, &config, std::slice::from_ref(&impostor)).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("DIFFERENT factory identity"));

        // Reorg rollback below the discovery removes and unregisters it.
        let mut tx = db.begin().await?;
        let removed = rollback_factory_children(&mut tx, BlockNumber::new(400), &config).await?;
        tx.commit().await.wrap_err("commit failed")?;
        assert_eq!(removed, vec![child]);
        assert!(config.contract_for_address(&child).is_none());
        assert_eq!(
            load_factory_children(&db, &config, std::slice::from_ref(&factory)).await?,
            0
        );

        reset_identity_state(&db).await
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn load_factory_children_unknown_name_is_fatal() -> eyre::Result<()> {
        let db = test_db().await?;
        sqlx::query("DELETE FROM _sieve_factory_children")
            .execute(db.pool())
            .await
            .wrap_err("clear children failed")?;

        let config = crate::config::usdc_transfer_config()?;
        let gone = test_factory("RenamedAway", 0)?;
        let mut tx = db.begin().await?;
        store_factory_child(
            &mut tx,
            &test_discovery(&gone, Address::repeat_byte(0xC2), 10),
        )
        .await?;
        tx.commit().await.wrap_err("commit failed")?;

        let current = test_factory("USDC", 0)?;
        let result = load_factory_children(&db, &config, std::slice::from_ref(&current)).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("RenamedAway"));

        sqlx::query("DELETE FROM _sieve_factory_children")
            .execute(db.pool())
            .await
            .wrap_err("cleanup failed")?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn legacy_children_bound_on_adoption() -> eyre::Result<()> {
        let db = test_db().await?;
        reset_coverage_state(&db).await?;
        sqlx::query("DELETE FROM _sieve_factory_children")
            .execute(db.pool())
            .await
            .wrap_err("clear children failed")?;

        let config = crate::config::usdc_transfer_config()?;
        let factory = test_factory("USDC", 0)?;

        // Simulate a pre-identity-tracking row: no identity columns.
        sqlx::query(
            "INSERT INTO _sieve_factory_children (factory_name, child_address, block_number) \
             VALUES ($1, $2, $3)",
        )
        .bind("USDC")
        .bind(Address::repeat_byte(0xC3).as_slice())
        .bind(100_i64)
        .execute(db.pool())
        .await
        .wrap_err("insert legacy child failed")?;

        // Unbound legacy rows refuse to load and point at the flag.
        let result = load_factory_children(&db, &config, std::slice::from_ref(&factory)).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("assume-factory-coverage"));

        // A real legacy database has an advanced checkpoint (that is what
        // forces the operator through the flag in the first place).
        sqlx::query("UPDATE _sieve_checkpoints SET block_number = 1000 WHERE id = 1")
            .execute(db.pool())
            .await
            .wrap_err("set checkpoint failed")?;

        // Adoption binds them to the asserted identity; loading then works.
        ensure_factory_coverage(&db, std::slice::from_ref(&factory), 0, true).await?;
        assert_eq!(
            load_factory_children(&db, &config, std::slice::from_ref(&factory)).await?,
            1
        );

        sqlx::query("DELETE FROM _sieve_factory_children")
            .execute(db.pool())
            .await
            .wrap_err("cleanup failed")?;
        reset_coverage_state(&db).await
    }

    /// Clear coverage rows and identity state before/after a coverage test.
    async fn reset_coverage_state(db: &Database) -> eyre::Result<()> {
        sqlx::query("DELETE FROM _sieve_factories")
            .execute(db.pool())
            .await
            .wrap_err("clear coverage failed")?;
        reset_identity_state(db).await
    }

    /// Advance checkpoint and factory coverage together, as a sync run does.
    async fn advance_covered(
        db: &Database,
        factories: &[crate::toml_config::ResolvedFactory],
        block: u64,
    ) -> eyre::Result<()> {
        let keys = FactoryCoverageKeys::new(factories);
        let mut tx = db.begin().await?;
        update_checkpoint(&mut tx, BlockNumber::new(block)).await?;
        advance_factory_coverage(&mut tx, &keys, BlockNumber::new(block)).await?;
        tx.commit().await.wrap_err("commit failed")
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn factory_coverage_new_factory_rules() -> eyre::Result<()> {
        let db = test_db().await?;
        reset_coverage_state(&db).await?;

        // Fresh database: factory registers fine.
        let factory = test_factory("Pool", 500)?;
        ensure_factory_coverage(&db, std::slice::from_ref(&factory), 0, false).await?;

        // Progress with the factory active, then a legitimate restart.
        advance_covered(&db, std::slice::from_ref(&factory), 1000).await?;
        ensure_factory_coverage(&db, std::slice::from_ref(&factory), 0, false).await?;

        // NEW factory whose start is below the checkpoint: refused.
        let late = test_factory("LatePool", 500)?;
        let result = ensure_factory_coverage(&db, std::slice::from_ref(&late), 0, false).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("never scanned"));

        // NEW factory starting exactly AT the checkpoint: refused too —
        // resume continues one past the checkpoint.
        let edge = test_factory("EdgePool", 1000)?;
        let result = ensure_factory_coverage(&db, std::slice::from_ref(&edge), 0, false).await;
        assert!(result.is_err());

        // NEW factory starting above the checkpoint: fine.
        let future = test_factory("FuturePool", 1001)?;
        ensure_factory_coverage(&db, std::slice::from_ref(&future), 0, false).await?;

        // Same name but a different factory address is a DIFFERENT
        // identity — the stored "Pool" row must not vouch for it.
        let impostor = test_factory_at(0xFB, "Pool", 500)?;
        let result = ensure_factory_coverage(&db, std::slice::from_ref(&impostor), 0, false).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("no coverage record"));

        // With the operator's assertion, the new identity is recorded as
        // covered and later startups pass normally.
        ensure_factory_coverage(&db, std::slice::from_ref(&impostor), 0, true).await?;
        ensure_factory_coverage(&db, std::slice::from_ref(&impostor), 0, false).await?;

        reset_coverage_state(&db).await
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn factory_coverage_known_factory_rules() -> eyre::Result<()> {
        let db = test_db().await?;
        reset_coverage_state(&db).await?;

        let factory = test_factory("Pool", 500)?;
        ensure_factory_coverage(&db, std::slice::from_ref(&factory), 0, false).await?;
        advance_covered(&db, std::slice::from_ref(&factory), 1000).await?;

        // start_block is immutable: lowering AND raising are both refused
        // (raising would break reorg rediscovery below the new start).
        let lowered = test_factory("Pool", 100)?;
        let result = ensure_factory_coverage(&db, std::slice::from_ref(&lowered), 0, false).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("immutable"));
        let raised = test_factory("Pool", 900)?;
        let result = ensure_factory_coverage(&db, std::slice::from_ref(&raised), 0, false).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("immutable"));

        // Changing the child-address parameter: refused (fingerprint).
        let mut reparam = test_factory("Pool", 500)?;
        reparam.child_address_param = "token0".to_owned();
        let result = ensure_factory_coverage(&db, std::slice::from_ref(&reparam), 0, false).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("extraction rule changed"));

        // Changing only a parameter's indexed status keeps the selector
        // AND the param name, but still changes extraction: refused.
        let mut reindexed = test_factory("Pool", 500)?;
        for input in &mut reindexed.creation_event.inputs {
            input.indexed = !input.indexed;
        }
        let result = ensure_factory_coverage(&db, std::slice::from_ref(&reindexed), 0, false).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("extraction rule changed"));

        // Checkpoint advancing while the factory was NOT active (removed
        // from config) leaves a gap: re-adding it is refused — the range
        // is missing child events too, so the flag cannot bless it.
        sqlx::query("UPDATE _sieve_checkpoints SET block_number = 2000 WHERE id = 1")
            .execute(db.pool())
            .await
            .wrap_err("set checkpoint failed")?;
        let result = ensure_factory_coverage(&db, std::slice::from_ref(&factory), 0, false).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("not active"));
        let result = ensure_factory_coverage(&db, std::slice::from_ref(&factory), 0, true).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("not active"));

        // The flag never blesses identity violations either.
        let result = ensure_factory_coverage(&db, std::slice::from_ref(&lowered), 0, true).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("immutable"));
        let result = ensure_factory_coverage(&db, std::slice::from_ref(&reparam), 0, true).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("extraction rule changed"));

        reset_coverage_state(&db).await
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn factory_coverage_sync_start_guard() -> eyre::Result<()> {
        let db = test_db().await?;
        reset_coverage_state(&db).await?;

        // Fresh database, but --start-block jumps past the factory's
        // start: the skipped range would be falsely certified — refused,
        // and no coverage row is written.
        let factory = test_factory("Pool", 500)?;
        let result = ensure_factory_coverage(&db, std::slice::from_ref(&factory), 800, false).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("skipping blocks"));
        let (rows,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM _sieve_factories")
            .fetch_one(db.pool())
            .await
            .wrap_err("count rows failed")?;
        assert_eq!(rows, 0);

        // Starting at or before the factory's start is fine.
        ensure_factory_coverage(&db, std::slice::from_ref(&factory), 500, false).await?;
        advance_covered(&db, std::slice::from_ref(&factory), 1000).await?;

        // A restart whose start override jumps past the covered frontier
        // (checkpoint 1000 → next uncovered 1001) is refused.
        let result =
            ensure_factory_coverage(&db, std::slice::from_ref(&factory), 1500, false).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("skipping blocks"));

        // Starting exactly at the covered frontier is fine.
        ensure_factory_coverage(&db, std::slice::from_ref(&factory), 1001, false).await?;

        reset_coverage_state(&db).await
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn factory_coverage_advance_and_rollback() -> eyre::Result<()> {
        let db = test_db().await?;
        reset_coverage_state(&db).await?;

        let pool = test_factory("Pool", 0)?;
        let other = test_factory_at(0xFB, "Other", 0)?;
        ensure_factory_coverage(&db, &[pool.clone(), other.clone()], 0, false).await?;

        // Advancing only Pool's keys must not touch Other's coverage.
        advance_covered(&db, std::slice::from_ref(&pool), 900).await?;
        assert_eq!(covered_through(&db, "Pool").await?, 900);
        assert_eq!(covered_through(&db, "Other").await?, 0);

        // Rollback clamps every row's coverage to the rollback target.
        let mut tx = db.begin().await?;
        rollback_to(&mut tx, BlockNumber::new(300)).await?;
        tx.commit().await.wrap_err("commit failed")?;
        assert_eq!(covered_through(&db, "Pool").await?, 300);
        assert_eq!(covered_through(&db, "Other").await?, 0);

        reset_coverage_state(&db).await
    }

    /// Read a factory's stored `covered_through` by child contract name.
    async fn covered_through(db: &Database, name: &str) -> eyre::Result<i64> {
        let (covered,): (i64,) = sqlx::query_as(
            "SELECT covered_through FROM _sieve_factories WHERE child_contract_name = $1",
        )
        .bind(name)
        .fetch_one(db.pool())
        .await
        .wrap_err("read covered_through failed")?;
        Ok(covered)
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn legacy_reset_refuses_chain_switch() -> eyre::Result<()> {
        let db = test_db().await?;
        reset_identity_state(&db).await?;

        // Simulate a registry-less database bound to base with state.
        sqlx::query("DROP TABLE IF EXISTS _sieve_tables")
            .execute(db.pool())
            .await
            .wrap_err("drop registry failed")?;
        sqlx::query("INSERT INTO _sieve_chain (id, chain, genesis_hash) VALUES (1, 'base', $1)")
            .bind(alloy_primitives::B256::repeat_byte(0x0B).as_slice())
            .execute(db.pool())
            .await
            .wrap_err("insert identity failed")?;
        sqlx::query("UPDATE _sieve_checkpoints SET block_number = 1000 WHERE id = 1")
            .execute(db.pool())
            .await
            .wrap_err("set checkpoint failed")?;

        // Registry-less base DB + mainnet destination → refused.
        let result = drop_all_tables(&db, "mainnet", &[], &[], &[]).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("chain \"base\" state"));

        // Same-chain reset is allowed.
        drop_all_tables(&db, "base", &[], &[], &[]).await?;

        // Recreate internal tables for the other tests and clean up.
        create_internal_tables(&db).await?;
        reset_identity_state(&db).await
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn legacy_reset_refuses_base_on_legacy_mainnet() -> eyre::Result<()> {
        let db = test_db().await?;
        reset_identity_state(&db).await?;

        // Registry-less DB, no identity row, but prior state → legacy mainnet.
        sqlx::query("DROP TABLE IF EXISTS _sieve_tables")
            .execute(db.pool())
            .await
            .wrap_err("drop registry failed")?;
        sqlx::query("UPDATE _sieve_checkpoints SET block_number = 1000 WHERE id = 1")
            .execute(db.pool())
            .await
            .wrap_err("set checkpoint failed")?;

        let result = drop_all_tables(&db, "base", &[], &[], &[]).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("chain \"mainnet\" state"));

        // Mainnet destination on legacy mainnet is allowed.
        drop_all_tables(&db, "mainnet", &[], &[], &[]).await?;

        create_internal_tables(&db).await?;
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
