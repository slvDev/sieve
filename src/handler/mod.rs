//! Handler system — user-defined event processing.
//!
//! Users implement the [`EventHandler`] trait to process decoded events.
//! Handlers receive decoded events and a database transaction,
//! allowing them to write to user-defined tables atomically.
//!
//! [`ConfigDrivenHandler`] is the TOML-driven implementation that generates
//! SQL dynamically from resolved event definitions.

use crate::decode::{DecodedEvent, DecodedParam};
use crate::toml_config::ResolvedEvent;
use alloy_dyn_abi::DynSolValue;
use alloy_primitives::Address;
use async_trait::async_trait;
use eyre::WrapErr;
use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use sqlx::{Postgres, Transaction};
use tracing::debug;

/// Trait for user-defined event handlers.
///
/// Each handler declares which contract/event combinations it handles
/// via [`matches`](EventHandler::matches), then processes matching events
/// in [`handle`](EventHandler::handle).
#[async_trait]
pub trait EventHandler: Send + Sync {
    /// Human-readable name for logging.
    fn name(&self) -> &str;

    /// Return `true` if this handler should process events from the given
    /// contract and event name.
    fn matches(&self, contract_name: &str, event_name: &str) -> bool;

    /// Process a decoded event, writing to the database within the provided
    /// transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if the database write fails.
    async fn handle(
        &self,
        event: &DecodedEvent,
        tx: &mut Transaction<'_, Postgres>,
    ) -> eyre::Result<()>;

    /// Roll back this handler's data above `block_number`.
    ///
    /// Called during reorg handling. Each handler is responsible for deleting
    /// its own rows so `rollback_to` doesn't need to hardcode table names.
    ///
    /// # Errors
    ///
    /// Returns an error if the DELETE query fails.
    async fn rollback(
        &self,
        block_number: u64,
        tx: &mut Transaction<'_, Postgres>,
    ) -> eyre::Result<()>;
}

/// Registry of event handlers. Dispatches decoded events to matching handlers.
pub struct HandlerRegistry {
    handlers: Vec<Box<dyn EventHandler>>,
}

impl std::fmt::Debug for HandlerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.handlers.iter().map(|h| h.name()).collect();
        f.debug_struct("HandlerRegistry")
            .field("handlers", &names)
            .finish()
    }
}

impl HandlerRegistry {
    /// Create a new registry with the given handlers.
    #[must_use]
    pub fn new(handlers: Vec<Box<dyn EventHandler>>) -> Self {
        Self { handlers }
    }

    /// Dispatch a decoded event to all matching handlers.
    ///
    /// Returns the number of handlers that processed the event.
    ///
    /// # Errors
    ///
    /// Returns an error if any matching handler fails.
    pub async fn dispatch(
        &self,
        event: &DecodedEvent,
        tx: &mut Transaction<'_, Postgres>,
    ) -> eyre::Result<u64> {
        let mut count = 0u64;
        for handler in &self.handlers {
            if handler.matches(&event.contract_name, &event.event_name) {
                handler.handle(event, tx).await?;
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    /// Roll back all handlers' data above `block_number`.
    ///
    /// # Errors
    ///
    /// Returns an error if any handler's rollback fails.
    pub async fn rollback_all(
        &self,
        block_number: u64,
        tx: &mut Transaction<'_, Postgres>,
    ) -> eyre::Result<()> {
        for handler in &self.handlers {
            handler.rollback(block_number, tx).await?;
        }
        Ok(())
    }

    /// Number of registered handlers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// Whether the registry has no handlers.
    #[must_use]
    #[expect(dead_code, reason = "required by convention alongside len()")]
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}

// ── Config-driven handler ─────────────────────────────────────────────

/// Generic event handler driven by TOML config.
///
/// Each instance handles one contract/event combination, using pre-built
/// SQL from [`ResolvedEvent`].
#[derive(Debug)]
pub struct ConfigDrivenHandler {
    resolved: ResolvedEvent,
    /// Pre-computed handler name for logging.
    handler_name: String,
}

impl ConfigDrivenHandler {
    /// Create a new config-driven handler for a resolved event.
    #[must_use]
    pub fn new(resolved: ResolvedEvent) -> Self {
        let handler_name = format!("{}:{}", resolved.contract_name, resolved.event_name);
        Self {
            resolved,
            handler_name,
        }
    }
}

#[async_trait]
impl EventHandler for ConfigDrivenHandler {
    fn name(&self) -> &str {
        &self.handler_name
    }

    fn matches(&self, contract_name: &str, event_name: &str) -> bool {
        self.resolved.contract_name == contract_name && self.resolved.event_name == event_name
    }

    async fn handle(
        &self,
        event: &DecodedEvent,
        tx: &mut Transaction<'_, Postgres>,
    ) -> eyre::Result<()> {
        debug!(
            handler = %self.handler_name,
            block = event.block_number,
            table = %self.resolved.table_name,
            "inserting event"
        );

        // Start building the query with pre-built INSERT SQL
        let mut query = sqlx::query(&self.resolved.insert_sql);

        // Bind standard columns: block_number, tx_hash, tx_index, log_index
        query = query
            .bind(event.block_number as i64)
            .bind(event.tx_hash.as_slice())
            .bind(event.tx_index as i32)
            .bind(event.log_index as i32);

        // Bind user-defined columns
        for col in &self.resolved.columns {
            let param = find_param(&event.indexed, &event.body, &col.param_name)
                .ok_or_else(|| {
                    eyre::eyre!(
                        "param '{}' not found in decoded event {}.{}",
                        col.param_name,
                        event.contract_name,
                        event.event_name
                    )
                })?;

            query = bind_dyn_value(query, &param.value, &col.pg_type)?;
        }

        query
            .execute(&mut **tx)
            .await
            .wrap_err_with(|| {
                format!(
                    "failed to insert into '{}' for {}.{}",
                    self.resolved.table_name, event.contract_name, event.event_name
                )
            })?;

        Ok(())
    }

    async fn rollback(
        &self,
        block_number: u64,
        tx: &mut Transaction<'_, Postgres>,
    ) -> eyre::Result<()> {
        sqlx::query(&self.resolved.rollback_sql)
            .bind(block_number as i64)
            .execute(&mut **tx)
            .await
            .wrap_err_with(|| format!("failed to rollback '{}'", self.resolved.table_name))?;
        Ok(())
    }
}

// ── DynSolValue binding ───────────────────────────────────────────────

/// Find a parameter by name in the indexed or body params.
#[must_use]
fn find_param<'a>(
    indexed: &'a [DecodedParam],
    body: &'a [DecodedParam],
    name: &str,
) -> Option<&'a DecodedParam> {
    indexed
        .iter()
        .chain(body.iter())
        .find(|p| p.name == name)
}

/// Bind a [`DynSolValue`] to a sqlx query based on the target Postgres type.
///
/// Returns the query with the value bound. All values are converted to owned
/// types to avoid lifetime issues. Unknown variants fall back to Debug
/// formatting as text.
fn bind_dyn_value<'q>(
    query: Query<'q, Postgres, PgArguments>,
    value: &DynSolValue,
    pg_type: &str,
) -> eyre::Result<Query<'q, Postgres, PgArguments>> {
    match value {
        DynSolValue::Address(addr) => {
            let checksum = Address::to_checksum(addr, None);
            Ok(query.bind(checksum))
        }
        DynSolValue::Bool(b) => Ok(query.bind(*b)),
        DynSolValue::Uint(val, bits) => {
            if pg_type == "bigint" && *bits <= 64 {
                let n: i64 = val.to::<u64>() as i64;
                Ok(query.bind(n))
            } else {
                // numeric — bind as string, Postgres casts
                Ok(query.bind(val.to_string()))
            }
        }
        DynSolValue::Int(val, bits) => {
            if pg_type == "bigint" && *bits <= 64 {
                let n: i64 = val.as_i64();
                Ok(query.bind(n))
            } else {
                Ok(query.bind(val.to_string()))
            }
        }
        DynSolValue::String(s) => Ok(query.bind(s.clone())),
        DynSolValue::Bytes(b) => Ok(query.bind(b.clone())),
        DynSolValue::FixedBytes(word, size) => {
            let bytes = word.as_slice()[..(*size)].to_vec();
            Ok(query.bind(bytes))
        }
        // Fallback: Debug format for complex types
        _ => {
            let text = format!("{value:?}");
            Ok(query.bind(text))
        }
    }
}

// ── USDC Transfer handler (test-only) ─────────────────────────────────

#[cfg(test)]
pub struct UsdcTransferHandler;

#[cfg(test)]
#[async_trait]
impl EventHandler for UsdcTransferHandler {
    #[expect(clippy::unnecessary_literal_bound, reason = "trait requires &str, not &'static str")]
    fn name(&self) -> &str {
        "UsdcTransferHandler"
    }

    fn matches(&self, contract_name: &str, event_name: &str) -> bool {
        contract_name == "USDC" && event_name == "Transfer"
    }

    async fn handle(
        &self,
        event: &DecodedEvent,
        tx: &mut Transaction<'_, Postgres>,
    ) -> eyre::Result<()> {
        let from = extract_address(&event.indexed, "from")?;
        let to = extract_address(&event.indexed, "to")?;
        let value = extract_uint(&event.body, "value")?;

        debug!(
            block = event.block_number,
            from = %from,
            to = %to,
            value = %value,
            "inserting USDC transfer"
        );

        sqlx::query(
            "INSERT INTO usdc_transfers (block_number, tx_hash, tx_index, log_index, from_address, to_address, value) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (block_number, tx_index, log_index) DO NOTHING",
        )
        .bind(event.block_number as i64)
        .bind(event.tx_hash.as_slice())
        .bind(event.tx_index as i32)
        .bind(event.log_index as i32)
        .bind(&from)
        .bind(&to)
        .bind(&value)
        .execute(&mut **tx)
        .await
        .wrap_err("failed to insert USDC transfer")?;

        Ok(())
    }

    async fn rollback(
        &self,
        block_number: u64,
        tx: &mut Transaction<'_, Postgres>,
    ) -> eyre::Result<()> {
        sqlx::query("DELETE FROM usdc_transfers WHERE block_number > $1")
            .bind(block_number as i64)
            .execute(&mut **tx)
            .await
            .wrap_err("failed to rollback usdc_transfers")?;
        Ok(())
    }
}

// ── DynSolValue helpers (test-only) ───────────────────────────────────

#[cfg(test)]
fn extract_address(
    params: &[DecodedParam],
    name: &str,
) -> eyre::Result<String> {
    for param in params {
        if param.name == name {
            if let DynSolValue::Address(addr) = &param.value {
                return Ok(Address::to_checksum(addr, None));
            }
            return Err(eyre::eyre!(
                "parameter '{name}' is not an address: {:?}",
                param.value
            ));
        }
    }
    Err(eyre::eyre!("parameter '{name}' not found"))
}

#[cfg(test)]
fn extract_uint(
    params: &[DecodedParam],
    name: &str,
) -> eyre::Result<String> {
    for param in params {
        if param.name == name {
            if let DynSolValue::Uint(val, _bits) = &param.value {
                return Ok(val.to_string());
            }
            return Err(eyre::eyre!(
                "parameter '{name}' is not a uint: {:?}",
                param.value
            ));
        }
    }
    Err(eyre::eyre!("parameter '{name}' not found"))
}

#[cfg(test)]
#[expect(clippy::panic_in_result_fn, reason = "assertions in tests are idiomatic")]
mod tests {
    use super::*;
    use alloy_dyn_abi::DynSolValue;
    use alloy_primitives::{address, B256, I256, U256};
    use crate::decode::{DecodedEvent, DecodedParam};

    fn make_test_event() -> DecodedEvent {
        let from = address!("1111111111111111111111111111111111111111");
        let to = address!("2222222222222222222222222222222222222222");

        DecodedEvent {
            event_name: "Transfer".to_string(),
            contract_name: "USDC".to_string(),
            indexed: vec![
                DecodedParam {
                    name: "from".to_string(),
                    solidity_type: "address".to_string(),
                    value: DynSolValue::Address(from),
                },
                DecodedParam {
                    name: "to".to_string(),
                    solidity_type: "address".to_string(),
                    value: DynSolValue::Address(to),
                },
            ],
            body: vec![DecodedParam {
                name: "value".to_string(),
                solidity_type: "uint256".to_string(),
                value: DynSolValue::Uint(U256::from(1_000_000u64), 256),
            }],
            block_number: 21_000_042,
            tx_hash: B256::repeat_byte(0xBB),
            tx_index: 5,
            log_index: 3,
        }
    }

    #[test]
    fn registry_dispatches_to_matching_handler() -> eyre::Result<()> {
        let handler = UsdcTransferHandler;
        assert!(handler.matches("USDC", "Transfer"));
        assert!(!handler.matches("USDC", "Approval"));
        assert!(!handler.matches("DAI", "Transfer"));
        Ok(())
    }

    #[test]
    fn extract_address_works() -> eyre::Result<()> {
        let event = make_test_event();
        let from = extract_address(&event.indexed, "from")?;
        assert!(from.starts_with("0x"));
        assert_eq!(from.len(), 42);
        Ok(())
    }

    #[test]
    fn extract_uint_works() -> eyre::Result<()> {
        let event = make_test_event();
        let value = extract_uint(&event.body, "value")?;
        assert_eq!(value, "1000000");
        Ok(())
    }

    #[test]
    fn extract_missing_param_errors() {
        let event = make_test_event();
        assert!(extract_address(&event.indexed, "nonexistent").is_err());
        assert!(extract_uint(&event.body, "nonexistent").is_err());
    }

    #[test]
    fn find_param_searches_indexed_and_body() {
        let event = make_test_event();
        assert!(find_param(&event.indexed, &event.body, "from").is_some());
        assert!(find_param(&event.indexed, &event.body, "value").is_some());
        assert!(find_param(&event.indexed, &event.body, "nonexistent").is_none());
    }

    #[test]
    fn config_driven_handler_matches() {
        use crate::toml_config::ResolvedEvent;

        let resolved = ResolvedEvent {
            event_name: "Transfer".to_string(),
            contract_name: "USDC".to_string(),
            table_name: "usdc_transfers".to_string(),
            columns: vec![],
            insert_sql: String::new(),
            create_table_sql: String::new(),
            create_indexes_sql: vec![],
            rollback_sql: String::new(),
        };

        let handler = ConfigDrivenHandler::new(resolved);
        assert!(handler.matches("USDC", "Transfer"));
        assert!(!handler.matches("USDC", "Approval"));
        assert!(!handler.matches("DAI", "Transfer"));
        assert_eq!(handler.name(), "USDC:Transfer");
    }

    #[test]
    fn bind_dyn_value_address() -> eyre::Result<()> {
        let addr = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let val = DynSolValue::Address(addr);
        let query = sqlx::query("SELECT $1");
        let _bound = bind_dyn_value(query, &val, "text")?;
        Ok(())
    }

    #[test]
    fn bind_dyn_value_bool() -> eyre::Result<()> {
        let val = DynSolValue::Bool(true);
        let query = sqlx::query("SELECT $1");
        let _bound = bind_dyn_value(query, &val, "boolean")?;
        Ok(())
    }

    #[test]
    fn bind_dyn_value_uint_small() -> eyre::Result<()> {
        let val = DynSolValue::Uint(U256::from(42u64), 64);
        let query = sqlx::query("SELECT $1");
        let _bound = bind_dyn_value(query, &val, "bigint")?;
        Ok(())
    }

    #[test]
    fn bind_dyn_value_uint_large() -> eyre::Result<()> {
        let val = DynSolValue::Uint(U256::from(1_000_000u64), 256);
        let query = sqlx::query("SELECT $1");
        let _bound = bind_dyn_value(query, &val, "numeric")?;
        Ok(())
    }

    #[test]
    fn bind_dyn_value_int() -> eyre::Result<()> {
        let val = DynSolValue::Int(I256::try_from(-42i64).unwrap_or_default(), 64);
        let query = sqlx::query("SELECT $1");
        let _bound = bind_dyn_value(query, &val, "bigint")?;
        Ok(())
    }

    #[test]
    fn bind_dyn_value_string() -> eyre::Result<()> {
        let val = DynSolValue::String("hello".to_string());
        let query = sqlx::query("SELECT $1");
        let _bound = bind_dyn_value(query, &val, "text")?;
        Ok(())
    }

    #[test]
    fn bind_dyn_value_bytes() -> eyre::Result<()> {
        let val = DynSolValue::Bytes(vec![1, 2, 3]);
        let query = sqlx::query("SELECT $1");
        let _bound = bind_dyn_value(query, &val, "bytea")?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn usdc_handler_inserts_row() -> eyre::Result<()> {
        let url = std::env::var("DATABASE_URL")
            .wrap_err("DATABASE_URL not set")?;
        let db = crate::db::Database::connect(&url).await?;

        let event = make_test_event();
        let handler = UsdcTransferHandler;

        let mut tx = db.begin().await?;
        handler.handle(&event, &mut tx).await?;
        tx.commit()
            .await
            .wrap_err("commit failed")?;

        // Verify the row was inserted
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM usdc_transfers WHERE block_number = $1 AND tx_index = $2 AND log_index = $3",
        )
        .bind(21_000_042i64)
        .bind(5i32)
        .bind(3i32)
        .fetch_one(db.pool())
        .await
        .wrap_err("query failed")?;

        assert_eq!(count.0, 1);

        // Clean up
        sqlx::query("DELETE FROM usdc_transfers WHERE block_number = $1")
            .bind(21_000_042i64)
            .execute(db.pool())
            .await
            .wrap_err("cleanup failed")?;

        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn usdc_handler_is_idempotent() -> eyre::Result<()> {
        let url = std::env::var("DATABASE_URL")
            .wrap_err("DATABASE_URL not set")?;
        let db = crate::db::Database::connect(&url).await?;

        // Clean up any leftover data from previous test runs
        sqlx::query("DELETE FROM usdc_transfers WHERE block_number = $1")
            .bind(21_000_042i64)
            .execute(db.pool())
            .await
            .wrap_err("pre-cleanup failed")?;

        let event = make_test_event();
        let handler = UsdcTransferHandler;

        // Insert first time
        let mut tx = db.begin().await?;
        handler.handle(&event, &mut tx).await?;
        tx.commit().await.wrap_err("first commit failed")?;

        // Insert same event again (should be a no-op due to ON CONFLICT DO NOTHING)
        let mut tx = db.begin().await?;
        handler.handle(&event, &mut tx).await?;
        tx.commit().await.wrap_err("second commit failed")?;

        // Verify only one row exists
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM usdc_transfers WHERE block_number = $1 AND tx_index = $2 AND log_index = $3",
        )
        .bind(21_000_042i64)
        .bind(5i32)
        .bind(3i32)
        .fetch_one(db.pool())
        .await
        .wrap_err("count query failed")?;

        assert_eq!(count.0, 1, "expected exactly 1 row after duplicate insert");

        // Clean up
        sqlx::query("DELETE FROM usdc_transfers WHERE block_number = $1")
            .bind(21_000_042i64)
            .execute(db.pool())
            .await
            .wrap_err("cleanup failed")?;

        Ok(())
    }
}
