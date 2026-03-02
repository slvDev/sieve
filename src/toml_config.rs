//! TOML configuration file parsing and resolution.
//!
//! Reads `sieve.toml`, parses contract/event definitions, resolves ABI files,
//! and produces [`IndexConfig`] + [`ResolvedEvent`] structs for the pipeline.

use crate::config::{ContractConfig, IndexConfig};
use alloy_json_abi::JsonAbi;
use alloy_primitives::{Address, B256};
use eyre::WrapErr;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tracing::info;

// ── TOML serde types ──────────────────────────────────────────────────

/// Top-level TOML config.
#[derive(Debug, Deserialize)]
pub struct SieveConfig {
    /// Optional database configuration.
    pub database: Option<DatabaseConfig>,
    /// Contracts to index.
    pub contracts: Vec<TomlContract>,
    /// Native ETH transfer definitions.
    #[serde(default)]
    pub transfers: Vec<TomlTransfer>,
    /// Stream/webhook notification sinks.
    #[serde(default)]
    pub streams: Vec<TomlStream>,
}

/// Database section.
#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    /// PostgreSQL connection URL.
    pub url: Option<String>,
}

/// A contract definition from TOML.
#[derive(Debug, Deserialize)]
pub struct TomlContract {
    /// Human-readable name (e.g. "USDC").
    pub name: String,
    /// Hex address with 0x prefix. Absent for factory-child contracts.
    pub address: Option<String>,
    /// Path to ABI JSON file (relative to config dir).
    pub abi: String,
    /// Block number to start indexing from. Factory provides it for children.
    pub start_block: Option<u64>,
    /// Events to index from this contract.
    pub events: Vec<TomlEvent>,
    /// Function calls to index from this contract.
    #[serde(default)]
    pub calls: Vec<TomlCall>,
    /// Optional factory config for dynamically-created contracts.
    pub factory: Option<TomlFactory>,
}

/// Factory section for a contract definition.
#[derive(Debug, Deserialize)]
pub struct TomlFactory {
    /// Factory contract address (hex with 0x prefix).
    pub address: String,
    /// Optional separate ABI for the factory (defaults to parent ABI).
    pub abi: Option<String>,
    /// Name of the creation event (e.g. "PoolCreated").
    pub event: String,
    /// ABI parameter name holding the child contract address (e.g. "pool").
    pub param: String,
    /// Block number to start scanning for factory events.
    pub start_block: u64,
}

/// An event definition from TOML.
#[derive(Debug, Deserialize)]
pub struct TomlEvent {
    /// Event name as it appears in the ABI (e.g. "Transfer").
    pub name: String,
    /// Postgres table name for this event's data.
    pub table: String,
    /// Optional context fields to enrich events with block/tx data.
    pub context: Option<Vec<String>>,
    /// Optional explicit column mappings. If omitted, auto-generated from ABI.
    pub columns: Option<Vec<TomlColumn>>,
    /// Optional indexed parameter filters. Keys are indexed param names from the
    /// ABI, values are lists of allowed hex values. All conditions are AND'd;
    /// each filter is OR'd internally.
    pub filter: Option<HashMap<String, Vec<String>>>,
}

/// An explicit column mapping from TOML.
#[derive(Debug, Deserialize)]
pub struct TomlColumn {
    /// ABI parameter name to map.
    pub param: String,
    /// Postgres column name (defaults to `param`).
    pub name: Option<String>,
    /// Postgres type (defaults to auto-mapping from Solidity type).
    #[serde(rename = "type")]
    pub pg_type: Option<String>,
}

/// A function call definition from TOML.
#[derive(Debug, Deserialize)]
pub struct TomlCall {
    /// Function name as it appears in the ABI (e.g. "exactInputSingle").
    pub name: String,
    /// Postgres table name for this call's data.
    pub table: String,
    /// Optional context fields to enrich calls with block/tx data.
    pub context: Option<Vec<String>>,
    /// Optional explicit column mappings. If omitted, auto-generated from ABI.
    pub columns: Option<Vec<TomlColumn>>,
}

/// A native ETH transfer definition from TOML.
#[derive(Debug, Deserialize)]
pub struct TomlTransfer {
    /// Human-readable name (e.g. "eth_transfers").
    pub name: String,
    /// Postgres table name.
    pub table: String,
    /// Block number to start indexing from.
    pub start_block: Option<u64>,
    /// Optional context fields to enrich transfers with block/tx data.
    pub context: Option<Vec<String>>,
    /// Optional address filters for from/to.
    pub filter: Option<TomlTransferFilter>,
}

/// Address filter for native ETH transfers.
#[derive(Debug, Deserialize)]
pub struct TomlTransferFilter {
    /// Only include transfers from these addresses.
    pub from: Option<Vec<String>>,
    /// Only include transfers to these addresses.
    pub to: Option<Vec<String>>,
}

/// A stream/webhook notification sink from TOML.
#[derive(Debug, Deserialize)]
pub struct TomlStream {
    /// Unique name for this stream.
    pub name: String,
    /// Stream type ("webhook" or "rabbitmq").
    #[serde(rename = "type")]
    pub stream_type: String,
    /// URL (webhook endpoint or AMQP connection string).
    pub url: Option<String>,
    /// Whether to send notifications during historical backfill. Defaults to true.
    #[serde(default = "default_true")]
    pub backfill: bool,
    /// AMQP exchange name (required for type = "rabbitmq").
    pub exchange: Option<String>,
    /// Routing key template for RabbitMQ. Supports `{table}` and `{event}` placeholders.
    /// Defaults to `"{table}.{event}"`.
    pub routing_key: Option<String>,
}

const fn default_true() -> bool {
    true
}

// ── Context fields ────────────────────────────────────────────────────

/// A context field that enriches events with block/transaction data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ContextField {
    /// Block timestamp (seconds since epoch).
    BlockTimestamp,
    /// Block hash.
    BlockHash,
    /// Transaction sender (recovered from signature).
    TxFrom,
    /// Transaction recipient (None for contract creation).
    TxTo,
    /// Transaction value in wei.
    TxValue,
    /// Effective gas price.
    TxGasPrice,
}

impl ContextField {
    /// Parse a context field name from TOML config.
    ///
    /// # Errors
    ///
    /// Returns an error if the name is not a recognized context field.
    fn from_name(name: &str) -> eyre::Result<Self> {
        match name {
            "block_timestamp" => Ok(Self::BlockTimestamp),
            "block_hash" => Ok(Self::BlockHash),
            "tx_from" => Ok(Self::TxFrom),
            "tx_to" => Ok(Self::TxTo),
            "tx_value" => Ok(Self::TxValue),
            "tx_gas_price" => Ok(Self::TxGasPrice),
            _ => Err(eyre::eyre!(
                "unknown context field '{name}'. Valid fields: \
                 block_timestamp, block_hash, tx_from, tx_to, tx_value, tx_gas_price"
            )),
        }
    }

    /// Postgres column name for this context field.
    #[must_use]
    pub const fn pg_column_name(self) -> &'static str {
        match self {
            Self::BlockTimestamp => "block_timestamp",
            Self::BlockHash => "block_hash",
            Self::TxFrom => "tx_from",
            Self::TxTo => "tx_to",
            Self::TxValue => "tx_value",
            Self::TxGasPrice => "tx_gas_price",
        }
    }

    /// Postgres type (with nullability suffix) for this context field.
    #[must_use]
    pub const fn pg_type(self) -> &'static str {
        match self {
            Self::BlockTimestamp | Self::TxGasPrice => "BIGINT NOT NULL",
            Self::BlockHash => "BYTEA NOT NULL",
            Self::TxFrom => "TEXT NOT NULL",
            Self::TxTo => "TEXT",
            Self::TxValue => "NUMERIC NOT NULL",
        }
    }
}

// ── Resolved types ────────────────────────────────────────────────────

/// A resolved column with all fields filled in.
#[derive(Debug, Clone)]
pub struct ResolvedColumn {
    /// Postgres column name.
    pub column_name: String,
    /// ABI parameter name.
    pub param_name: String,
    /// Postgres type (e.g. "text", "numeric", "bigint", "boolean", "bytea").
    pub pg_type: String,
}

/// Resolved output from TOML config resolution.
#[derive(Debug)]
pub struct ResolvedConfig {
    /// Filter/decode pipeline config.
    pub index_config: IndexConfig,
    /// Per-event SQL and column mappings.
    pub resolved_events: Vec<ResolvedEvent>,
    /// Factory definitions for dynamic contract discovery.
    pub factories: Vec<ResolvedFactory>,
    /// Native ETH transfer definitions.
    pub transfers: Vec<ResolvedTransfer>,
    /// Function call definitions.
    pub calls: Vec<ResolvedCall>,
    /// Stream/webhook notification sinks.
    pub streams: Vec<ResolvedStream>,
}

/// A fully resolved stream/webhook sink definition.
#[derive(Debug, Clone)]
pub struct ResolvedStream {
    /// Unique name for this stream.
    pub name: String,
    /// Stream type ("webhook" or "rabbitmq").
    pub stream_type: String,
    /// URL (webhook endpoint or AMQP connection string).
    pub url: String,
    /// Whether to send notifications during historical backfill.
    pub backfill: bool,
    /// AMQP exchange name (only for rabbitmq).
    pub exchange: Option<String>,
    /// Routing key template (only for rabbitmq).
    pub routing_key: Option<String>,
}

/// A resolved factory contract definition.
#[derive(Debug, Clone)]
pub struct ResolvedFactory {
    /// On-chain factory contract address.
    pub factory_address: Address,
    /// ABI event for contract creation.
    pub creation_event: alloy_json_abi::Event,
    /// Topic0 selector for the creation event.
    pub creation_selector: alloy_primitives::B256,
    /// Name of the ABI parameter holding the child address.
    pub child_address_param: String,
    /// Name of the child contract (for handler/table lookup).
    pub child_contract_name: String,
    /// Block number to start scanning for factory events.
    pub start_block: u64,
}

/// A fully resolved native ETH transfer definition.
#[derive(Debug, Clone)]
pub struct ResolvedTransfer {
    /// Human-readable name.
    pub name: String,
    /// Postgres table name.
    pub table_name: String,
    /// Context fields to enrich transfers with block/tx data.
    pub context_fields: Vec<ContextField>,
    /// Pre-built CREATE TABLE SQL.
    pub create_table_sql: String,
    /// Pre-built CREATE INDEX SQL statements.
    pub create_indexes_sql: Vec<String>,
    /// Pre-built INSERT SQL.
    pub insert_sql: String,
    /// Pre-built DELETE (rollback) SQL.
    pub rollback_sql: String,
    /// Filter: only include transfers from these addresses.
    pub filter_from: Vec<Address>,
    /// Filter: only include transfers to these addresses.
    pub filter_to: Vec<Address>,
}

/// A fully resolved function call definition with pre-built SQL.
#[derive(Debug, Clone)]
pub struct ResolvedCall {
    /// Function name from the ABI.
    pub function_name: String,
    /// Contract name.
    pub contract_name: String,
    /// Postgres table name.
    pub table_name: String,
    /// Context fields to enrich calls with block/tx data.
    pub context_fields: Vec<ContextField>,
    /// User-defined columns (mapped from ABI function inputs).
    pub columns: Vec<ResolvedColumn>,
    /// Pre-built INSERT SQL.
    pub insert_sql: String,
    /// Pre-built CREATE TABLE SQL.
    pub create_table_sql: String,
    /// Pre-built CREATE INDEX SQL statements.
    pub create_indexes_sql: Vec<String>,
    /// Pre-built DELETE (rollback) SQL.
    pub rollback_sql: String,
    /// Whether this call belongs to a factory-child contract.
    pub is_factory_child: bool,
}

/// A filter on a single indexed event parameter (topic).
///
/// At sync time, a log must have its `topic[topic_index]` value contained in
/// `values` to pass the filter. Multiple `TopicFilter`s for the same event
/// are AND'd (all must match).
#[derive(Debug, Clone)]
pub struct TopicFilter {
    /// Topic position: 1, 2, or 3 (topic0 is the event selector).
    pub topic_index: usize,
    /// Allowed topic values. The log must match at least one.
    pub values: HashSet<B256>,
}

/// A fully resolved event with pre-built SQL statements.
#[derive(Debug, Clone)]
pub struct ResolvedEvent {
    /// Event name from the ABI.
    pub event_name: String,
    /// Contract name.
    pub contract_name: String,
    /// Postgres table name.
    pub table_name: String,
    /// Context fields to enrich events with block/tx data.
    pub context_fields: Vec<ContextField>,
    /// User-defined columns (mapped from ABI params).
    pub columns: Vec<ResolvedColumn>,
    /// Pre-built INSERT SQL.
    pub insert_sql: String,
    /// Pre-built CREATE TABLE SQL.
    pub create_table_sql: String,
    /// Pre-built CREATE INDEX SQL statements.
    pub create_indexes_sql: Vec<String>,
    /// Pre-built DELETE (rollback) SQL.
    pub rollback_sql: String,
    /// Whether this event belongs to a factory-child contract.
    pub is_factory_child: bool,
    /// Indexed parameter filters for pre-decode filtering.
    pub topic_filters: Vec<TopicFilter>,
}

// ── Type mapping ──────────────────────────────────────────────────────

/// Map a Solidity type to a Postgres type.
#[must_use]
fn default_pg_type(solidity_type: &str) -> &'static str {
    match solidity_type {
        "bool" => "boolean",

        "address" | "string" => "text",

        // Small uints/ints → bigint
        "uint8" | "uint16" | "uint32" | "uint64" | "int8" | "int16" | "int32" | "int64" => {
            "bigint"
        }

        // Large uints/ints → numeric
        "uint128" | "uint256" | "uint96" | "uint112" | "uint160" | "uint192" | "uint224"
        | "int128" | "int256" | "int96" | "int112" | "int160" | "int192" | "int224" => "numeric",

        // bytes and bytesN → bytea
        s if s.starts_with("bytes") => "bytea",

        // uint/int variants we didn't explicitly list (e.g. uint40, uint48, etc.)
        s if s.starts_with("uint") || s.starts_with("int") => {
            let prefix_len = if s.starts_with("uint") { 4 } else { 3 };
            s[prefix_len..]
                .parse::<u32>()
                .ok()
                .map_or("text", |bits| {
                    if bits <= 64 {
                        "bigint"
                    } else {
                        "numeric"
                    }
                })
        }

        // Fallback
        _ => "text",
    }
}

// ── Identifier validation ─────────────────────────────────────────────

/// PostgreSQL reserved words that cannot be used as unquoted identifiers.
const SQL_RESERVED: &[&str] = &[
    "all", "alter", "and", "as", "between", "by", "case", "check", "column", "constraint",
    "create", "cross", "default", "delete", "distinct", "drop", "else", "end", "except", "exists",
    "false", "fetch", "for", "foreign", "from", "grant", "group", "having", "in", "index", "inner",
    "insert", "intersect", "into", "is", "join", "left", "like", "limit", "not", "null", "offset",
    "on", "or", "order", "outer", "primary", "references", "returning", "right", "select", "set",
    "table", "then", "true", "union", "unique", "update", "using", "values", "view", "when",
    "where", "with",
];

/// Validate that a SQL identifier (table or column name) is safe.
///
/// Allows lowercase ASCII letters, digits, and underscores. Must start
/// with a letter or underscore. Rejects everything else to prevent
/// SQL injection through user-provided config values.
///
/// # Errors
///
/// Returns an error if the identifier is empty or contains invalid characters.
fn validate_identifier(name: &str, kind: &str) -> eyre::Result<()> {
    if name.is_empty() {
        return Err(eyre::eyre!("{kind} name must not be empty"));
    }
    let first = name.as_bytes()[0];
    if !first.is_ascii_lowercase() && first != b'_' {
        return Err(eyre::eyre!(
            "{kind} name '{name}' must start with a lowercase letter or underscore"
        ));
    }
    if !name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_') {
        return Err(eyre::eyre!(
            "{kind} name '{name}' contains invalid characters (only a-z, 0-9, _ allowed)"
        ));
    }
    // Only reject reserved words for table names (user-chosen in TOML config).
    // Column names come from ABI parameters (e.g. "from" in Transfer events)
    // and are quoted in generated SQL, so they're safe.
    if kind == "table" && SQL_RESERVED.contains(&name) {
        return Err(eyre::eyre!("{kind} name '{name}' is a SQL reserved word"));
    }
    Ok(())
}

// ── SQL generators ────────────────────────────────────────────────────

/// Generate `CREATE TABLE IF NOT EXISTS` SQL for an event.
///
/// Callers must validate identifiers before calling this function.
/// For factory-child tables, adds a `contract_address` column and includes
/// it in the unique constraint.
#[must_use]
fn generate_create_table_sql(
    table: &str,
    context_fields: &[ContextField],
    columns: &[ResolvedColumn],
    is_factory_child: bool,
) -> String {
    use std::fmt::Write;

    let mut sql = format!(
        "CREATE TABLE IF NOT EXISTS {table} (\n  \
         id BIGSERIAL PRIMARY KEY,\n  \
         block_number BIGINT NOT NULL,\n  \
         tx_hash BYTEA NOT NULL,\n  \
         tx_index INTEGER NOT NULL,\n  \
         log_index INTEGER NOT NULL"
    );

    if is_factory_child {
        sql.push_str(",\n  contract_address TEXT NOT NULL");
    }

    // Context columns (between standard and user columns)
    for cf in context_fields {
        let _ = write!(sql, ",\n  {} {}", cf.pg_column_name(), cf.pg_type());
    }

    for col in columns {
        let _ = write!(sql, ",\n  {} {} NOT NULL", col.column_name, col.pg_type);
    }

    if is_factory_child {
        sql.push_str(",\n  UNIQUE (contract_address, block_number, tx_index, log_index)\n)");
    } else {
        sql.push_str(",\n  UNIQUE (block_number, tx_index, log_index)\n)");
    }

    sql
}

/// Generate `CREATE INDEX IF NOT EXISTS` SQL statements.
#[must_use]
fn generate_indexes_sql(table: &str) -> Vec<String> {
    vec![format!(
        "CREATE INDEX IF NOT EXISTS idx_{table}_block_number ON {table} (block_number)"
    )]
}

/// Write a SQL parameter placeholder, adding a `::numeric` cast when needed.
fn write_param(out: &mut String, idx: usize, needs_numeric_cast: bool) {
    use std::fmt::Write;
    if needs_numeric_cast {
        let _ = write!(out, "${idx}::numeric");
    } else {
        let _ = write!(out, "${idx}");
    }
}

/// Generate the INSERT SQL with positional parameters.
///
/// For factory-child tables, `contract_address` is inserted as `$5`
/// (shifting context and user column indices by 1).
#[must_use]
fn generate_insert_sql(
    table: &str,
    context_fields: &[ContextField],
    columns: &[ResolvedColumn],
    is_factory_child: bool,
) -> String {
    let mut col_names = String::from("block_number, tx_hash, tx_index, log_index");

    // Standard columns are $1-$4; factory children add contract_address as $5
    let mut param_idx = if is_factory_child {
        col_names.push_str(", contract_address");
        6
    } else {
        5
    };

    // Context columns
    for cf in context_fields {
        col_names.push_str(", ");
        col_names.push_str(cf.pg_column_name());
    }

    for col in columns {
        col_names.push_str(", ");
        col_names.push_str(&col.column_name);
    }

    let mut params = String::from("$1, $2, $3, $4");

    if is_factory_child {
        params.push_str(", $5");
    }

    for cf in context_fields {
        params.push_str(", ");
        write_param(&mut params, param_idx, matches!(cf, ContextField::TxValue));
        param_idx += 1;
    }

    for col in columns {
        params.push_str(", ");
        write_param(&mut params, param_idx, col.pg_type == "numeric");
        param_idx += 1;
    }

    let conflict = if is_factory_child {
        "ON CONFLICT (contract_address, block_number, tx_index, log_index) DO NOTHING"
    } else {
        "ON CONFLICT (block_number, tx_index, log_index) DO NOTHING"
    };

    format!("INSERT INTO {table} ({col_names}) VALUES ({params}) {conflict}")
}

/// Generate the rollback (DELETE) SQL.
#[must_use]
fn generate_rollback_sql(table: &str) -> String {
    format!("DELETE FROM {table} WHERE block_number > $1")
}

// ── Transfer SQL generators ───────────────────────────────────────────

/// Generate `CREATE TABLE IF NOT EXISTS` SQL for a native transfer table.
///
/// Fixed columns: `id`, `block_number`, `tx_hash`, `tx_index`, `from_address`,
/// `to_address`, `value`. Context columns inserted between `value` and the
/// UNIQUE constraint. No `log_index` column (one transfer per transaction).
#[must_use]
fn generate_transfer_create_table_sql(
    table: &str,
    context_fields: &[ContextField],
) -> String {
    use std::fmt::Write;

    let mut sql = format!(
        "CREATE TABLE IF NOT EXISTS {table} (\n  \
         id BIGSERIAL PRIMARY KEY,\n  \
         block_number BIGINT NOT NULL,\n  \
         tx_hash BYTEA NOT NULL,\n  \
         tx_index INTEGER NOT NULL,\n  \
         from_address TEXT NOT NULL,\n  \
         to_address TEXT NOT NULL,\n  \
         value NUMERIC NOT NULL"
    );

    for cf in context_fields {
        let _ = write!(sql, ",\n  {} {}", cf.pg_column_name(), cf.pg_type());
    }

    sql.push_str(",\n  UNIQUE (block_number, tx_index)\n)");
    sql
}

/// Generate INSERT SQL for a native transfer table.
///
/// Fixed params: `$1=block_number, $2=tx_hash, $3=tx_index, $4=from_address,
/// $5=to_address, $6=value::numeric`. Context columns follow.
#[must_use]
fn generate_transfer_insert_sql(
    table: &str,
    context_fields: &[ContextField],
) -> String {
    let mut col_names =
        String::from("block_number, tx_hash, tx_index, from_address, to_address, value");
    let mut params = String::from("$1, $2, $3, $4, $5, $6::numeric");
    let mut param_idx = 7;

    for cf in context_fields {
        col_names.push_str(", ");
        col_names.push_str(cf.pg_column_name());
        params.push_str(", ");
        write_param(&mut params, param_idx, matches!(cf, ContextField::TxValue));
        param_idx += 1;
    }

    format!(
        "INSERT INTO {table} ({col_names}) VALUES ({params}) \
         ON CONFLICT (block_number, tx_index) DO NOTHING"
    )
}

// ── Call SQL generators ────────────────────────────────────────────

/// Generate `CREATE TABLE IF NOT EXISTS` SQL for a function call table.
///
/// Fixed columns: `id`, `block_number`, `tx_hash`, `tx_index` (no `log_index`).
/// Optional `contract_address` for factory children. Context + user columns.
/// UNIQUE on `(block_number, tx_index)`.
#[must_use]
fn generate_call_create_table_sql(
    table: &str,
    context_fields: &[ContextField],
    columns: &[ResolvedColumn],
    is_factory_child: bool,
) -> String {
    use std::fmt::Write;

    let mut sql = format!(
        "CREATE TABLE IF NOT EXISTS {table} (\n  \
         id BIGSERIAL PRIMARY KEY,\n  \
         block_number BIGINT NOT NULL,\n  \
         tx_hash BYTEA NOT NULL,\n  \
         tx_index INTEGER NOT NULL"
    );

    if is_factory_child {
        sql.push_str(",\n  contract_address TEXT NOT NULL");
    }

    for cf in context_fields {
        let _ = write!(sql, ",\n  {} {}", cf.pg_column_name(), cf.pg_type());
    }

    for col in columns {
        let _ = write!(sql, ",\n  {} {} NOT NULL", col.column_name, col.pg_type);
    }

    if is_factory_child {
        sql.push_str(",\n  UNIQUE (contract_address, block_number, tx_index)\n)");
    } else {
        sql.push_str(",\n  UNIQUE (block_number, tx_index)\n)");
    }

    sql
}

/// Generate INSERT SQL for a function call table.
///
/// Fixed params: `$1=block_number, $2=tx_hash, $3=tx_index`.
/// Optional `$4=contract_address` for factory children. Context + user columns follow.
#[must_use]
fn generate_call_insert_sql(
    table: &str,
    context_fields: &[ContextField],
    columns: &[ResolvedColumn],
    is_factory_child: bool,
) -> String {
    let mut col_names = String::from("block_number, tx_hash, tx_index");

    let mut param_idx = if is_factory_child {
        col_names.push_str(", contract_address");
        5
    } else {
        4
    };

    for cf in context_fields {
        col_names.push_str(", ");
        col_names.push_str(cf.pg_column_name());
    }

    for col in columns {
        col_names.push_str(", ");
        col_names.push_str(&col.column_name);
    }

    let mut params = String::from("$1, $2, $3");

    if is_factory_child {
        params.push_str(", $4");
    }

    for cf in context_fields {
        params.push_str(", ");
        write_param(&mut params, param_idx, matches!(cf, ContextField::TxValue));
        param_idx += 1;
    }

    for col in columns {
        params.push_str(", ");
        write_param(&mut params, param_idx, col.pg_type == "numeric");
        param_idx += 1;
    }

    let conflict = if is_factory_child {
        "ON CONFLICT (contract_address, block_number, tx_index) DO NOTHING"
    } else {
        "ON CONFLICT (block_number, tx_index) DO NOTHING"
    };

    format!("INSERT INTO {table} ({col_names}) VALUES ({params}) {conflict}")
}

/// Resolve a single `TomlTransfer` into a `ResolvedTransfer`.
///
/// Validates the table name, checks for collisions with event tables,
/// parses context fields and filter addresses.
///
/// # Errors
///
/// Returns an error if the table name is invalid/duplicate, context fields
/// are invalid, or filter addresses fail to parse.
fn resolve_transfer(
    t: &TomlTransfer,
    table_names: &mut HashSet<String>,
) -> eyre::Result<ResolvedTransfer> {
    validate_identifier(&t.table, "table")?;
    if !table_names.insert(t.table.clone()) {
        return Err(eyre::eyre!(
            "duplicate table name '{}' (collision between events and transfers)",
            t.table
        ));
    }

    let context_fields =
        resolve_context_fields(t.context.as_deref(), &t.name, "transfer")?;

    let filter_from = parse_address_list(
        t.filter.as_ref().and_then(|f| f.from.as_deref()),
        &t.name,
        "from",
    )?;
    let filter_to = parse_address_list(
        t.filter.as_ref().and_then(|f| f.to.as_deref()),
        &t.name,
        "to",
    )?;

    let create_table_sql = generate_transfer_create_table_sql(&t.table, &context_fields);
    let create_indexes_sql = generate_indexes_sql(&t.table);
    let insert_sql = generate_transfer_insert_sql(&t.table, &context_fields);
    let rollback_sql = generate_rollback_sql(&t.table);

    Ok(ResolvedTransfer {
        name: t.name.clone(),
        table_name: t.table.clone(),
        context_fields,
        create_table_sql,
        create_indexes_sql,
        insert_sql,
        rollback_sql,
        filter_from,
        filter_to,
    })
}

/// Parse a list of hex address strings into `Vec<Address>`.
///
/// # Errors
///
/// Returns an error if any address string is invalid.
fn parse_address_list(
    addrs: Option<&[String]>,
    transfer_name: &str,
    field: &str,
) -> eyre::Result<Vec<Address>> {
    let Some(addrs) = addrs else {
        return Ok(Vec::new());
    };
    addrs
        .iter()
        .map(|a| {
            a.parse::<Address>().wrap_err_with(|| {
                format!("invalid {field} address '{a}' in transfer '{transfer_name}'")
            })
        })
        .collect()
}

// ── Stream resolution ──────────────────────────────────────────────────

/// Resolve and validate `[[streams]]` definitions from TOML config.
///
/// # Errors
///
/// Returns an error if stream names are duplicated, names fail identifier
/// validation, types are unknown, or required fields are missing.
fn resolve_streams(streams: &[TomlStream]) -> eyre::Result<Vec<ResolvedStream>> {
    let mut resolved = Vec::with_capacity(streams.len());
    let mut seen_names = HashSet::new();

    for s in streams {
        validate_identifier(&s.name, "stream")?;

        if !seen_names.insert(s.name.clone()) {
            return Err(eyre::eyre!("duplicate stream name '{}'", s.name));
        }

        let url = s.url.as_deref().unwrap_or("").to_string();

        match s.stream_type.as_str() {
            "webhook" => {
                if url.is_empty() {
                    return Err(eyre::eyre!(
                        "stream '{}' of type 'webhook' requires a 'url'",
                        s.name
                    ));
                }
                resolved.push(ResolvedStream {
                    name: s.name.clone(),
                    stream_type: "webhook".to_string(),
                    url,
                    backfill: s.backfill,
                    exchange: None,
                    routing_key: None,
                });
            }
            "rabbitmq" => {
                if url.is_empty() {
                    return Err(eyre::eyre!(
                        "stream '{}' of type 'rabbitmq' requires a 'url'",
                        s.name
                    ));
                }
                let exchange = s.exchange.as_deref().unwrap_or("").to_string();
                if exchange.is_empty() {
                    return Err(eyre::eyre!(
                        "stream '{}' of type 'rabbitmq' requires an 'exchange'",
                        s.name
                    ));
                }
                let default_routing_key = ["{table}", ".", "{event}"].concat();
                let routing_key = s
                    .routing_key
                    .clone()
                    .unwrap_or(default_routing_key);
                resolved.push(ResolvedStream {
                    name: s.name.clone(),
                    stream_type: "rabbitmq".to_string(),
                    url,
                    backfill: s.backfill,
                    exchange: Some(exchange),
                    routing_key: Some(routing_key),
                });
            }
            other => {
                return Err(eyre::eyre!(
                    "unknown stream type '{other}' for stream '{}' (supported: webhook, rabbitmq)",
                    s.name
                ));
            }
        }
    }

    Ok(resolved)
}

// ── Config loading and resolution ─────────────────────────────────────

/// Load and parse a `sieve.toml` config file.
///
/// # Errors
///
/// Returns an error if the file cannot be read or parsed.
pub fn load_config(config_path: &Path) -> eyre::Result<SieveConfig> {
    let content = std::fs::read_to_string(config_path)
        .wrap_err_with(|| format!("failed to read config file: {}", config_path.display()))?;

    let config: SieveConfig =
        toml::from_str(&content).wrap_err("failed to parse TOML config")?;

    Ok(config)
}

/// Resolve a parsed TOML config into `IndexConfig` + `Vec<ResolvedEvent>`.
///
/// Reads ABI files, validates event/param names, builds SQL, and produces
/// the filter/decode config.
///
/// # Errors
///
/// Returns an error if ABI files cannot be read, events are not found in ABIs,
/// column params don't exist, addresses are invalid, or table names collide.
pub fn resolve_config(
    config: &SieveConfig,
    config_dir: &Path,
) -> eyre::Result<ResolvedConfig> {
    let mut contract_configs = Vec::with_capacity(config.contracts.len());
    let mut resolved_events = Vec::new();
    let mut resolved_factories = Vec::new();
    let mut resolved_calls = Vec::new();
    let mut table_names = HashSet::new();

    for contract in &config.contracts {
        let contract_config = resolve_contract(
            contract,
            config_dir,
            &mut table_names,
            &mut resolved_events,
            &mut resolved_factories,
            &mut resolved_calls,
        )?;
        contract_configs.push(contract_config);
    }

    // Resolve native ETH transfers (table_names set is shared with events)
    let mut resolved_transfers = Vec::with_capacity(config.transfers.len());
    for t in &config.transfers {
        resolved_transfers.push(resolve_transfer(t, &mut table_names)?);
    }

    // Resolve streams (webhooks)
    let resolved_streams = resolve_streams(&config.streams)?;

    let index_config = IndexConfig::new(contract_configs);

    info!(
        contracts = config.contracts.len(),
        events = resolved_events.len(),
        factories = resolved_factories.len(),
        transfers = resolved_transfers.len(),
        calls = resolved_calls.len(),
        streams = resolved_streams.len(),
        "resolved TOML config"
    );

    Ok(ResolvedConfig {
        index_config,
        resolved_events,
        factories: resolved_factories,
        transfers: resolved_transfers,
        calls: resolved_calls,
        streams: resolved_streams,
    })
}

/// Resolve a single contract from TOML into a `ContractConfig` and collect
/// resolved events, factories, and calls.
fn resolve_contract(
    contract: &TomlContract,
    config_dir: &Path,
    table_names: &mut HashSet<String>,
    resolved_events: &mut Vec<ResolvedEvent>,
    resolved_factories: &mut Vec<ResolvedFactory>,
    resolved_calls: &mut Vec<ResolvedCall>,
) -> eyre::Result<ContractConfig> {
    let is_factory_child = contract.factory.is_some();

    // Validate: must have either address or factory, not both, not neither
    if contract.address.is_some() && is_factory_child {
        return Err(eyre::eyre!(
            "contract '{}' has both 'address' and 'factory'; use one or the other",
            contract.name
        ));
    }
    if contract.address.is_none() && !is_factory_child {
        return Err(eyre::eyre!(
            "contract '{}' must have either 'address' or 'factory'",
            contract.name
        ));
    }

    // Resolve address: static contract has an address, factory child uses Address::ZERO placeholder
    let address: Address = if let Some(ref addr_str) = contract.address {
        addr_str
            .parse()
            .wrap_err_with(|| format!("invalid address for contract '{}'", contract.name))?
    } else {
        Address::ZERO
    };

    // Read and parse ABI
    let abi_path = config_dir.join(&contract.abi);
    let abi_json = std::fs::read_to_string(&abi_path).wrap_err_with(|| {
        format!(
            "failed to read ABI file '{}' for contract '{}'",
            abi_path.display(),
            contract.name
        )
    })?;
    let abi: JsonAbi = serde_json::from_str(&abi_json).wrap_err_with(|| {
        format!(
            "failed to parse ABI JSON '{}' for contract '{}'",
            abi_path.display(),
            contract.name
        )
    })?;

    // Resolve factory if present
    if let Some(ref factory) = contract.factory {
        resolved_factories.push(resolve_factory(
            factory,
            &contract.name,
            &abi,
            config_dir,
        )?);
    }

    let event_names: Vec<&str> = contract.events.iter().map(|e| e.name.as_str()).collect();

    // Resolve each event
    for toml_event in &contract.events {
        resolve_event(
            toml_event,
            &contract.name,
            &abi,
            is_factory_child,
            table_names,
            resolved_events,
        )?;
    }

    // Build ContractConfig for the filter/decode pipeline
    let mut contract_config = ContractConfig::from_abi(
        &contract.name,
        address,
        &abi,
        &event_names,
    )?;

    // Populate topic filters from resolved events for this contract
    for re in &resolved_events[resolved_events.len() - contract.events.len()..] {
        if !re.topic_filters.is_empty() {
            let selector = contract_config
                .events
                .keys()
                .find(|sel| {
                    contract_config.events.get(*sel)
                        .is_some_and(|ev| ev.name == re.event_name)
                })
                .copied();
            if let Some(sel) = selector {
                contract_config.topic_filters.insert(sel, re.topic_filters.clone());
            }
        }
    }

    // Resolve each function call
    let function_names: Vec<&str> = contract.calls.iter().map(|c| c.name.as_str()).collect();
    for toml_call in &contract.calls {
        resolve_call(
            toml_call,
            &contract.name,
            &abi,
            is_factory_child,
            table_names,
            resolved_calls,
        )?;
    }

    // Populate function selectors from ABI
    for &fn_name in &function_names {
        if let Some(fns) = abi.functions.get(fn_name) {
            if let Some(func) = fns.first() {
                contract_config
                    .functions
                    .insert(func.selector(), func.clone());
            }
        }
    }

    Ok(contract_config)
}

/// Resolve a single TOML event definition into a `ResolvedEvent`.
///
/// # Errors
///
/// Returns an error if the table name is invalid/duplicate, the event is not
/// found in the ABI, or column resolution fails.
fn resolve_event(
    toml_event: &TomlEvent,
    contract_name: &str,
    abi: &JsonAbi,
    is_factory_child: bool,
    table_names: &mut HashSet<String>,
    resolved_events: &mut Vec<ResolvedEvent>,
) -> eyre::Result<()> {
    validate_identifier(&toml_event.table, "table")?;
    if !table_names.insert(toml_event.table.clone()) {
        return Err(eyre::eyre!(
            "duplicate table name '{}' across events",
            toml_event.table
        ));
    }

    let abi_events = abi.events.get(&toml_event.name).ok_or_else(|| {
        eyre::eyre!(
            "event '{}' not found in ABI for contract '{contract_name}'",
            toml_event.name,
        )
    })?;
    let abi_event = abi_events.first().ok_or_else(|| {
        eyre::eyre!(
            "no variants for event '{}' in contract '{contract_name}'",
            toml_event.name,
        )
    })?;

    let context_fields = resolve_context_fields(
        toml_event.context.as_deref(),
        contract_name,
        &toml_event.name,
    )?;
    let columns = resolve_columns(
        toml_event.columns.as_deref(),
        abi_event,
        contract_name,
        &toml_event.name,
    )?;
    let topic_filters = resolve_topic_filters(
        toml_event.filter.as_ref(),
        abi_event,
        contract_name,
        &toml_event.name,
    )?;

    let create_table_sql =
        generate_create_table_sql(&toml_event.table, &context_fields, &columns, is_factory_child);
    let create_indexes_sql = generate_indexes_sql(&toml_event.table);
    let insert_sql =
        generate_insert_sql(&toml_event.table, &context_fields, &columns, is_factory_child);
    let rollback_sql = generate_rollback_sql(&toml_event.table);

    resolved_events.push(ResolvedEvent {
        event_name: toml_event.name.clone(),
        contract_name: contract_name.to_owned(),
        table_name: toml_event.table.clone(),
        context_fields,
        columns,
        insert_sql,
        create_table_sql,
        create_indexes_sql,
        rollback_sql,
        is_factory_child,
        topic_filters,
    });

    Ok(())
}

/// Resolve a factory definition: load ABI, validate event and param, build
/// `ResolvedFactory`.
///
/// # Errors
///
/// Returns an error if the factory ABI cannot be read/parsed, the event is
/// not found, or the child-address param does not exist.
fn resolve_factory(
    factory: &TomlFactory,
    contract_name: &str,
    parent_abi: &JsonAbi,
    config_dir: &Path,
) -> eyre::Result<ResolvedFactory> {
    let factory_abi: JsonAbi = if let Some(ref factory_abi_path) = factory.abi {
        let fabi_path = config_dir.join(factory_abi_path);
        let fabi_json = std::fs::read_to_string(&fabi_path).wrap_err_with(|| {
            format!(
                "failed to read factory ABI '{}' for contract '{contract_name}'",
                fabi_path.display(),
            )
        })?;
        serde_json::from_str(&fabi_json).wrap_err_with(|| {
            format!(
                "failed to parse factory ABI JSON '{}' for contract '{contract_name}'",
                fabi_path.display(),
            )
        })?
    } else {
        parent_abi.clone()
    };

    let factory_address: Address = factory
        .address
        .parse()
        .wrap_err_with(|| format!("invalid factory address for contract '{contract_name}'"))?;

    let creation_events = factory_abi.events.get(&factory.event).ok_or_else(|| {
        eyre::eyre!(
            "factory event '{}' not found in ABI for contract '{contract_name}'",
            factory.event,
        )
    })?;
    let creation_event = creation_events.first().ok_or_else(|| {
        eyre::eyre!(
            "no variants for factory event '{}' in contract '{contract_name}'",
            factory.event,
        )
    })?;

    let param_exists = creation_event.inputs.iter().any(|p| p.name == factory.param);
    if !param_exists {
        return Err(eyre::eyre!(
            "factory param '{}' not found in event '{}' for contract '{contract_name}'",
            factory.param,
            factory.event,
        ));
    }

    Ok(ResolvedFactory {
        factory_address,
        creation_selector: creation_event.selector(),
        creation_event: creation_event.clone(),
        child_address_param: factory.param.clone(),
        child_contract_name: contract_name.to_owned(),
        start_block: factory.start_block,
    })
}

/// Resolve a single TOML call definition into a `ResolvedCall`.
///
/// # Errors
///
/// Returns an error if the table name is invalid/duplicate, the function is
/// not found in the ABI, or column resolution fails.
fn resolve_call(
    toml_call: &TomlCall,
    contract_name: &str,
    abi: &JsonAbi,
    is_factory_child: bool,
    table_names: &mut HashSet<String>,
    resolved_calls: &mut Vec<ResolvedCall>,
) -> eyre::Result<()> {
    validate_identifier(&toml_call.table, "table")?;
    if !table_names.insert(toml_call.table.clone()) {
        return Err(eyre::eyre!(
            "duplicate table name '{}' across events/calls/transfers",
            toml_call.table
        ));
    }

    let abi_functions = abi.functions.get(&toml_call.name).ok_or_else(|| {
        eyre::eyre!(
            "function '{}' not found in ABI for contract '{contract_name}'",
            toml_call.name,
        )
    })?;
    let abi_function = abi_functions.first().ok_or_else(|| {
        eyre::eyre!(
            "no variants for function '{}' in contract '{contract_name}'",
            toml_call.name,
        )
    })?;

    let context_fields = resolve_context_fields(
        toml_call.context.as_deref(),
        contract_name,
        &toml_call.name,
    )?;
    let columns = resolve_call_columns(
        toml_call.columns.as_deref(),
        abi_function,
        contract_name,
        &toml_call.name,
    )?;

    let create_table_sql = generate_call_create_table_sql(
        &toml_call.table,
        &context_fields,
        &columns,
        is_factory_child,
    );
    let create_indexes_sql = generate_indexes_sql(&toml_call.table);
    let insert_sql = generate_call_insert_sql(
        &toml_call.table,
        &context_fields,
        &columns,
        is_factory_child,
    );
    let rollback_sql = generate_rollback_sql(&toml_call.table);

    resolved_calls.push(ResolvedCall {
        function_name: toml_call.name.clone(),
        contract_name: contract_name.to_owned(),
        table_name: toml_call.table.clone(),
        context_fields,
        columns,
        insert_sql,
        create_table_sql,
        create_indexes_sql,
        rollback_sql,
        is_factory_child,
    });

    Ok(())
}

/// Resolve columns for a function call: either from explicit TOML definitions
/// or auto-generated from all ABI function input parameters.
///
/// # Errors
///
/// Returns an error if a column param is not found in the function inputs,
/// or if a column name fails identifier validation.
fn resolve_call_columns(
    toml_columns: Option<&[TomlColumn]>,
    abi_function: &alloy_json_abi::Function,
    contract_name: &str,
    function_name: &str,
) -> eyre::Result<Vec<ResolvedColumn>> {
    match toml_columns {
        Some(cols) => {
            let mut resolved = Vec::with_capacity(cols.len());
            for col in cols {
                let abi_param = abi_function
                    .inputs
                    .iter()
                    .find(|p| p.name == col.param)
                    .ok_or_else(|| {
                        eyre::eyre!(
                            "param '{}' not found in function '{}.{}'",
                            col.param,
                            contract_name,
                            function_name
                        )
                    })?;

                let column_name = col.name.clone().unwrap_or_else(|| col.param.clone());
                validate_identifier(&column_name, "column")?;
                let pg_type = col
                    .pg_type
                    .clone()
                    .unwrap_or_else(|| default_pg_type(&abi_param.ty).to_string());

                resolved.push(ResolvedColumn {
                    column_name,
                    param_name: col.param.clone(),
                    pg_type,
                });
            }
            Ok(resolved)
        }
        None => {
            let mut resolved = Vec::with_capacity(abi_function.inputs.len());
            for param in &abi_function.inputs {
                validate_identifier(&param.name, "column")?;
                resolved.push(ResolvedColumn {
                    column_name: param.name.clone(),
                    param_name: param.name.clone(),
                    pg_type: default_pg_type(&param.ty).to_string(),
                });
            }
            Ok(resolved)
        }
    }
}

/// Resolve columns for an event: either from explicit TOML definitions or
/// auto-generated from all ABI parameters.
///
/// # Errors
///
/// Returns an error if a column param is not found in the ABI event inputs,
/// or if a column name fails identifier validation.
fn resolve_columns(
    toml_columns: Option<&[TomlColumn]>,
    abi_event: &alloy_json_abi::Event,
    contract_name: &str,
    event_name: &str,
) -> eyre::Result<Vec<ResolvedColumn>> {
    match toml_columns {
        Some(cols) => {
            // Explicit columns
            let mut resolved = Vec::with_capacity(cols.len());
            for col in cols {
                // Find param in ABI inputs
                let abi_param = abi_event
                    .inputs
                    .iter()
                    .find(|p| p.name == col.param)
                    .ok_or_else(|| {
                        eyre::eyre!(
                            "param '{}' not found in event '{}.{}'",
                            col.param,
                            contract_name,
                            event_name
                        )
                    })?;

                let column_name = col.name.clone().unwrap_or_else(|| col.param.clone());
                validate_identifier(&column_name, "column")?;
                let pg_type = col
                    .pg_type
                    .clone()
                    .unwrap_or_else(|| default_pg_type(&abi_param.ty).to_string());

                resolved.push(ResolvedColumn {
                    column_name,
                    param_name: col.param.clone(),
                    pg_type,
                });
            }
            Ok(resolved)
        }
        None => {
            // Auto-generate from all ABI params
            let mut resolved = Vec::with_capacity(abi_event.inputs.len());
            for param in &abi_event.inputs {
                validate_identifier(&param.name, "column")?;
                resolved.push(ResolvedColumn {
                    column_name: param.name.clone(),
                    param_name: param.name.clone(),
                    pg_type: default_pg_type(&param.ty).to_string(),
                });
            }
            Ok(resolved)
        }
    }
}

/// Resolve context fields from optional TOML string list.
///
/// # Errors
///
/// Returns an error if any context field name is unrecognized or duplicated.
fn resolve_context_fields(
    names: Option<&[String]>,
    contract_name: &str,
    event_name: &str,
) -> eyre::Result<Vec<ContextField>> {
    let Some(names) = names else {
        return Ok(Vec::new());
    };

    let mut fields = Vec::with_capacity(names.len());
    let mut seen = HashSet::new();

    for name in names {
        let field = ContextField::from_name(name).wrap_err_with(|| {
            format!("in event '{contract_name}.{event_name}'")
        })?;

        if !seen.insert(name.as_str()) {
            return Err(eyre::eyre!(
                "duplicate context field '{name}' in event '{contract_name}.{event_name}'"
            ));
        }

        fields.push(field);
    }

    Ok(fields)
}

/// Resolve topic filters from TOML filter map.
///
/// Validates that each key names an indexed parameter in the event ABI,
/// computes the topic index (1-based among indexed params), and encodes
/// each value as a left-padded `B256` according to the Solidity type.
///
/// # Errors
///
/// Returns an error if a param name is not found, is not indexed, or a
/// value fails to parse.
fn resolve_topic_filters(
    filter: Option<&HashMap<String, Vec<String>>>,
    abi_event: &alloy_json_abi::Event,
    contract_name: &str,
    event_name: &str,
) -> eyre::Result<Vec<TopicFilter>> {
    let Some(filter) = filter else {
        return Ok(Vec::new());
    };

    let mut topic_filters = Vec::with_capacity(filter.len());

    for (param_name, raw_values) in filter {
        // Find the param in the ABI and verify it's indexed
        let param = abi_event
            .inputs
            .iter()
            .find(|p| p.name == *param_name)
            .ok_or_else(|| {
                eyre::eyre!(
                    "filter param '{param_name}' not found in event \
                     '{contract_name}.{event_name}'"
                )
            })?;

        if !param.indexed {
            return Err(eyre::eyre!(
                "filter param '{param_name}' in event '{contract_name}.{event_name}' \
                 is not indexed (only indexed params can be filtered by topic)"
            ));
        }

        // Compute topic index: count indexed params that come before this one, + 1
        let topic_index = abi_event
            .inputs
            .iter()
            .take_while(|p| p.name != *param_name)
            .filter(|p| p.indexed)
            .count()
            + 1;

        // Encode each value as B256 based on Solidity type
        let mut values = HashSet::with_capacity(raw_values.len());
        for raw in raw_values {
            let b256 = encode_topic_value(raw, &param.ty).wrap_err_with(|| {
                format!(
                    "invalid filter value '{raw}' for param '{param_name}' \
                     (type {}) in event '{contract_name}.{event_name}'",
                    param.ty
                )
            })?;
            values.insert(b256);
        }

        topic_filters.push(TopicFilter {
            topic_index,
            values,
        });
    }

    Ok(topic_filters)
}

/// Encode a filter value string as a B256 topic value.
///
/// Topics are always 32 bytes. Addresses and smaller types are left-padded.
///
/// # Errors
///
/// Returns an error if the value cannot be parsed as the given Solidity type.
fn encode_topic_value(value: &str, solidity_type: &str) -> eyre::Result<B256> {
    match solidity_type {
        "address" => {
            let addr: Address = value.parse().wrap_err("invalid address")?;
            Ok(B256::left_padding_from(addr.as_slice()))
        }
        "bool" => match value {
            "true" | "1" => {
                let mut bytes = [0u8; 32];
                bytes[31] = 1;
                Ok(B256::from(bytes))
            }
            "false" | "0" => Ok(B256::ZERO),
            _ => Err(eyre::eyre!("invalid bool value '{value}'")),
        },
        s if s.starts_with("bytes") && s.len() > 5 => {
            // bytesN (fixed-size): parse as raw 32-byte hex (only bytes32 fully supported)
            value.parse::<B256>().wrap_err("invalid bytes value")
        }
        s if s.starts_with("uint") || s.starts_with("int") => {
            // Integer types: parse as U256, convert to B256
            let n: alloy_primitives::U256 = if value.starts_with("0x") || value.starts_with("0X")
            {
                value.parse().wrap_err("invalid hex integer")?
            } else {
                alloy_primitives::U256::from_str_radix(value, 10)
                    .map_err(|e| eyre::eyre!("invalid integer: {e}"))?
            };
            Ok(n.into())
        }
        _ => {
            // Fallback: try parsing as raw B256 hex
            value.parse::<B256>().wrap_err("unsupported type for topic filter; provide raw 32-byte hex")
        }
    }
}

#[cfg(test)]
#[expect(clippy::panic_in_result_fn, reason = "assertions in tests are idiomatic")]
mod tests {
    use super::*;

    const MINIMAL_TOML: &str = r#"
[[contracts]]
name = "USDC"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/erc20.json"
start_block = 21000000

[[contracts.events]]
name = "Transfer"
table = "usdc_transfers"
"#;

    const FULL_TOML: &str = r#"
[database]
url = "postgres://localhost:5432/sieve"

[[contracts]]
name = "USDC"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/erc20.json"
start_block = 21000000

[[contracts.events]]
name = "Transfer"
table = "usdc_transfers"
columns = [
  { param = "from", name = "from_address", type = "text" },
  { param = "to", name = "to_address", type = "text" },
  { param = "value", name = "value", type = "numeric" },
]

[[contracts.events]]
name = "Approval"
table = "usdc_approvals"
"#;

    #[test]
    fn parse_minimal_toml() -> eyre::Result<()> {
        let config: SieveConfig = toml::from_str(MINIMAL_TOML)?;
        assert!(config.database.is_none());
        assert_eq!(config.contracts.len(), 1);
        assert_eq!(config.contracts[0].name, "USDC");
        assert_eq!(config.contracts[0].events.len(), 1);
        assert!(config.contracts[0].events[0].columns.is_none());
        Ok(())
    }

    #[test]
    fn parse_full_toml() -> eyre::Result<()> {
        let config: SieveConfig = toml::from_str(FULL_TOML)?;
        assert!(config.database.is_some());
        assert_eq!(
            config.database.as_ref().and_then(|d| d.url.as_deref()),
            Some("postgres://localhost:5432/sieve")
        );
        assert_eq!(config.contracts[0].events.len(), 2);

        let transfer = &config.contracts[0].events[0];
        assert_eq!(transfer.columns.as_ref().map(Vec::len), Some(3));

        let approval = &config.contracts[0].events[1];
        assert!(approval.columns.is_none());
        Ok(())
    }

    #[test]
    fn type_mapping_addresses() {
        assert_eq!(default_pg_type("address"), "text");
    }

    #[test]
    fn type_mapping_booleans() {
        assert_eq!(default_pg_type("bool"), "boolean");
    }

    #[test]
    fn type_mapping_small_uints() {
        assert_eq!(default_pg_type("uint8"), "bigint");
        assert_eq!(default_pg_type("uint16"), "bigint");
        assert_eq!(default_pg_type("uint32"), "bigint");
        assert_eq!(default_pg_type("uint64"), "bigint");
    }

    #[test]
    fn type_mapping_large_uints() {
        assert_eq!(default_pg_type("uint128"), "numeric");
        assert_eq!(default_pg_type("uint256"), "numeric");
    }

    #[test]
    fn type_mapping_small_ints() {
        assert_eq!(default_pg_type("int8"), "bigint");
        assert_eq!(default_pg_type("int64"), "bigint");
    }

    #[test]
    fn type_mapping_large_ints() {
        assert_eq!(default_pg_type("int128"), "numeric");
        assert_eq!(default_pg_type("int256"), "numeric");
    }

    #[test]
    fn type_mapping_bytes() {
        assert_eq!(default_pg_type("bytes"), "bytea");
        assert_eq!(default_pg_type("bytes32"), "bytea");
        assert_eq!(default_pg_type("bytes20"), "bytea");
    }

    #[test]
    fn type_mapping_string() {
        assert_eq!(default_pg_type("string"), "text");
    }

    #[test]
    fn type_mapping_unknown_fallback() {
        assert_eq!(default_pg_type("tuple"), "text");
    }

    #[test]
    fn type_mapping_non_standard_uint_widths() {
        assert_eq!(default_pg_type("uint40"), "bigint");
        assert_eq!(default_pg_type("uint48"), "bigint");
        assert_eq!(default_pg_type("uint96"), "numeric");
        assert_eq!(default_pg_type("uint112"), "numeric");
    }

    #[test]
    fn generate_create_table_produces_valid_sql() {
        let columns = vec![
            ResolvedColumn {
                column_name: "from_address".to_string(),
                param_name: "from".to_string(),
                pg_type: "text".to_string(),
            },
            ResolvedColumn {
                column_name: "value".to_string(),
                param_name: "value".to_string(),
                pg_type: "numeric".to_string(),
            },
        ];

        let sql = generate_create_table_sql("usdc_transfers", &[], &columns, false);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS usdc_transfers"));
        assert!(sql.contains("id BIGSERIAL PRIMARY KEY"));
        assert!(sql.contains("block_number BIGINT NOT NULL"));
        assert!(sql.contains("tx_hash BYTEA NOT NULL"));
        assert!(sql.contains("from_address text NOT NULL"));
        assert!(sql.contains("value numeric NOT NULL"));
        assert!(sql.contains("UNIQUE (block_number, tx_index, log_index)"));
    }

    #[test]
    fn generate_create_table_with_context() {
        let ctx = vec![ContextField::BlockTimestamp, ContextField::TxFrom];
        let columns = vec![ResolvedColumn {
            column_name: "value".to_string(),
            param_name: "value".to_string(),
            pg_type: "numeric".to_string(),
        }];

        let sql = generate_create_table_sql("t", &ctx, &columns, false);
        // Context columns appear between standard and user columns
        assert!(sql.contains("block_timestamp BIGINT NOT NULL"));
        assert!(sql.contains("tx_from TEXT NOT NULL"));
        assert!(sql.contains("value numeric NOT NULL"));
    }

    #[test]
    fn generate_create_table_with_nullable_tx_to() {
        let ctx = vec![ContextField::TxTo];
        let sql = generate_create_table_sql("t", &ctx, &[], false);
        // tx_to should be nullable (no NOT NULL)
        assert!(sql.contains("tx_to TEXT"));
        assert!(!sql.contains("tx_to TEXT NOT NULL"));
    }

    #[test]
    fn generate_insert_produces_correct_params() {
        let columns = vec![
            ResolvedColumn {
                column_name: "from_address".to_string(),
                param_name: "from".to_string(),
                pg_type: "text".to_string(),
            },
            ResolvedColumn {
                column_name: "to_address".to_string(),
                param_name: "to".to_string(),
                pg_type: "text".to_string(),
            },
        ];

        let sql = generate_insert_sql("usdc_transfers", &[], &columns, false);
        assert!(sql.contains("INSERT INTO usdc_transfers"));
        assert!(sql.contains("block_number, tx_hash, tx_index, log_index, from_address, to_address"));
        assert!(sql.contains("$1, $2, $3, $4, $5, $6"));
        assert!(sql.contains("ON CONFLICT"));
    }

    #[test]
    fn generate_insert_with_context() {
        let ctx = vec![ContextField::BlockTimestamp, ContextField::TxFrom];
        let columns = vec![ResolvedColumn {
            column_name: "value".to_string(),
            param_name: "value".to_string(),
            pg_type: "numeric".to_string(),
        }];

        let sql = generate_insert_sql("t", &ctx, &columns, false);
        // Context columns between standard ($1-$4) and user columns
        assert!(sql.contains("block_timestamp, tx_from, value"));
        assert!(sql.contains("$5, $6, $7::numeric"));
    }

    #[test]
    fn generate_insert_tx_value_gets_numeric_cast() {
        let ctx = vec![ContextField::TxValue];
        let sql = generate_insert_sql("t", &ctx, &[], false);
        assert!(sql.contains("$5::numeric"));
    }

    #[test]
    fn generate_rollback_sql_correct() {
        let sql = generate_rollback_sql("usdc_transfers");
        assert_eq!(sql, "DELETE FROM usdc_transfers WHERE block_number > $1");
    }

    /// Create a unique temp dir for a test, write an ABI file, and return the dir path.
    fn setup_test_dir(suffix: &str, abi_json: &str) -> eyre::Result<std::path::PathBuf> {
        let dir = std::env::temp_dir().join(format!(
            "sieve_test_{suffix}_{}",
            std::process::id()
        ));
        let abi_dir = dir.join("abis");
        std::fs::create_dir_all(&abi_dir)?;
        std::fs::write(abi_dir.join("erc20.json"), abi_json)?;
        Ok(dir)
    }

    const ERC20_ABI: &str = r#"[
        {"anonymous":false,"inputs":[
            {"indexed":true,"internalType":"address","name":"from","type":"address"},
            {"indexed":true,"internalType":"address","name":"to","type":"address"},
            {"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}
        ],"name":"Transfer","type":"event"},
        {"anonymous":false,"inputs":[
            {"indexed":true,"internalType":"address","name":"owner","type":"address"},
            {"indexed":true,"internalType":"address","name":"spender","type":"address"},
            {"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}
        ],"name":"Approval","type":"event"}
    ]"#;

    const TRANSFER_ONLY_ABI: &str = r#"[
        {"anonymous":false,"inputs":[
            {"indexed":true,"internalType":"address","name":"from","type":"address"},
            {"indexed":true,"internalType":"address","name":"to","type":"address"},
            {"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}
        ],"name":"Transfer","type":"event"}
    ]"#;

    #[test]
    fn resolve_with_full_abi_file() -> eyre::Result<()> {
        let dir = setup_test_dir("resolve", ERC20_ABI)?;

        let config: SieveConfig = toml::from_str(FULL_TOML)?;
        let ResolvedConfig { index_config, resolved_events: resolved, .. } = resolve_config(&config, &dir)?;

        assert_eq!(index_config.contracts.len(), 1);
        assert_eq!(resolved.len(), 2);

        // Transfer event has explicit columns with renames
        let transfer = &resolved[0];
        assert_eq!(transfer.event_name, "Transfer");
        assert_eq!(transfer.table_name, "usdc_transfers");
        assert_eq!(transfer.columns.len(), 3);
        assert_eq!(transfer.columns[0].column_name, "from_address");
        assert_eq!(transfer.columns[0].param_name, "from");

        // Approval event has auto-generated columns
        let approval = &resolved[1];
        assert_eq!(approval.event_name, "Approval");
        assert_eq!(approval.table_name, "usdc_approvals");
        assert_eq!(approval.columns.len(), 3);
        assert_eq!(approval.columns[0].column_name, "owner");
        assert_eq!(approval.columns[0].pg_type, "text");

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn resolve_error_missing_event_in_abi() -> eyre::Result<()> {
        let dir = setup_test_dir("missing_event", TRANSFER_ONLY_ABI)?;

        let toml_str = r#"
[[contracts]]
name = "USDC"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/erc20.json"
start_block = 21000000

[[contracts.events]]
name = "NonExistentEvent"
table = "nope"
"#;
        let config: SieveConfig = toml::from_str(toml_str)?;
        let Err(err) = resolve_config(&config, &dir) else {
            return Err(eyre::eyre!("expected error for missing event"));
        };
        assert!(err.to_string().contains("not found in ABI"));

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn resolve_error_missing_param() -> eyre::Result<()> {
        let dir = setup_test_dir("missing_param", TRANSFER_ONLY_ABI)?;

        let toml_str = r#"
[[contracts]]
name = "USDC"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/erc20.json"
start_block = 21000000

[[contracts.events]]
name = "Transfer"
table = "usdc_transfers"
columns = [
  { param = "nonexistent_param" },
]
"#;
        let config: SieveConfig = toml::from_str(toml_str)?;
        let Err(err) = resolve_config(&config, &dir) else {
            return Err(eyre::eyre!("expected error for missing param"));
        };
        assert!(err.to_string().contains("not found in event"));

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn resolve_error_invalid_address() -> eyre::Result<()> {
        let abi_json = r#"[{"anonymous":false,"inputs":[],"name":"Foo","type":"event"}]"#;
        let dir = setup_test_dir("bad_addr", abi_json)?;

        let toml_str = r#"
[[contracts]]
name = "Bad"
address = "not_a_valid_address"
abi = "abis/erc20.json"
start_block = 0

[[contracts.events]]
name = "Foo"
table = "foo"
"#;
        let config: SieveConfig = toml::from_str(toml_str)?;
        let Err(err) = resolve_config(&config, &dir) else {
            return Err(eyre::eyre!("expected error for invalid address"));
        };
        assert!(err.to_string().contains("invalid address"));

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn resolve_error_duplicate_table_names() -> eyre::Result<()> {
        let abi_json = r#"[
            {"anonymous":false,"inputs":[],"name":"Transfer","type":"event"},
            {"anonymous":false,"inputs":[],"name":"Approval","type":"event"}
        ]"#;
        let dir = setup_test_dir("dup_tables", abi_json)?;

        let toml_str = r#"
[[contracts]]
name = "USDC"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/erc20.json"
start_block = 0

[[contracts.events]]
name = "Transfer"
table = "same_table"

[[contracts.events]]
name = "Approval"
table = "same_table"
"#;
        let config: SieveConfig = toml::from_str(toml_str)?;
        let Err(err) = resolve_config(&config, &dir) else {
            return Err(eyre::eyre!("expected error for duplicate tables"));
        };
        assert!(err.to_string().contains("duplicate table name"));

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn parse_context_fields_from_toml() -> eyre::Result<()> {
        let toml_str = r#"
[[contracts]]
name = "USDC"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/erc20.json"
start_block = 21000000

[[contracts.events]]
name = "Transfer"
table = "usdc_transfers"
context = ["block_timestamp", "tx_from"]
"#;
        let config: SieveConfig = toml::from_str(toml_str)?;
        let ctx = config.contracts[0].events[0].context.as_ref();
        assert!(ctx.is_some());
        let ctx = ctx.ok_or_else(|| eyre::eyre!("missing context"))?;
        assert_eq!(ctx.len(), 2);
        assert_eq!(ctx[0], "block_timestamp");
        assert_eq!(ctx[1], "tx_from");
        Ok(())
    }

    #[test]
    fn resolve_context_fields_valid() -> eyre::Result<()> {
        let names = vec![
            "block_timestamp".to_string(),
            "block_hash".to_string(),
            "tx_from".to_string(),
            "tx_to".to_string(),
            "tx_value".to_string(),
            "tx_gas_price".to_string(),
        ];
        let fields = resolve_context_fields(Some(&names), "Test", "Foo")?;
        assert_eq!(fields.len(), 6);
        assert_eq!(fields[0], ContextField::BlockTimestamp);
        assert_eq!(fields[5], ContextField::TxGasPrice);
        Ok(())
    }

    #[test]
    fn resolve_context_fields_unknown_errors() -> eyre::Result<()> {
        let names = vec!["nonexistent".to_string()];
        let Err(err) = resolve_context_fields(Some(&names), "Test", "Foo") else {
            return Err(eyre::eyre!("expected error for unknown context field"));
        };
        let msg = format!("{err:?}");
        assert!(msg.contains("unknown context field"));
        Ok(())
    }

    #[test]
    fn resolve_context_fields_duplicate_errors() -> eyre::Result<()> {
        let names = vec!["block_timestamp".to_string(), "block_timestamp".to_string()];
        let Err(err) = resolve_context_fields(Some(&names), "Test", "Foo") else {
            return Err(eyre::eyre!("expected error for duplicate context field"));
        };
        assert!(err.to_string().contains("duplicate context field"));
        Ok(())
    }

    #[test]
    fn resolve_context_fields_none_is_empty() -> eyre::Result<()> {
        let fields = resolve_context_fields(None, "Test", "Foo")?;
        assert!(fields.is_empty());
        Ok(())
    }

    #[test]
    fn validate_identifier_accepts_valid_names() -> eyre::Result<()> {
        validate_identifier("usdc_transfers", "table")?;
        validate_identifier("from_address", "column")?;
        validate_identifier("_private", "column")?;
        validate_identifier("x", "column")?;
        Ok(())
    }

    #[test]
    fn validate_identifier_rejects_sql_injection() -> eyre::Result<()> {
        let Err(err) = validate_identifier("foo; DROP TABLE bar", "table") else {
            return Err(eyre::eyre!("expected error for SQL injection"));
        };
        assert!(err.to_string().contains("invalid characters"));
        Ok(())
    }

    #[test]
    fn validate_identifier_rejects_uppercase() -> eyre::Result<()> {
        let Err(err) = validate_identifier("MyTable", "table") else {
            return Err(eyre::eyre!("expected error for uppercase"));
        };
        assert!(err.to_string().contains("must start with"));
        Ok(())
    }

    #[test]
    fn validate_identifier_rejects_empty() -> eyre::Result<()> {
        let Err(err) = validate_identifier("", "table") else {
            return Err(eyre::eyre!("expected error for empty string"));
        };
        assert!(err.to_string().contains("must not be empty"));
        Ok(())
    }

    #[test]
    fn validate_identifier_rejects_leading_digit() -> eyre::Result<()> {
        let Err(err) = validate_identifier("123table", "table") else {
            return Err(eyre::eyre!("expected error for leading digit"));
        };
        assert!(err.to_string().contains("must start with"));
        Ok(())
    }

    #[test]
    fn validate_identifier_rejects_reserved_words() {
        assert!(validate_identifier("select", "table").is_err());
        assert!(validate_identifier("order", "table").is_err());
        assert!(validate_identifier("where", "table").is_err());
        // Column names from ABIs are allowed (e.g. "from" in Transfer events)
        assert!(validate_identifier("from", "column").is_ok());
        // Non-reserved words still pass
        assert!(validate_identifier("transfers", "table").is_ok());
    }

    #[test]
    fn resolve_error_invalid_table_name() -> eyre::Result<()> {
        let dir = setup_test_dir("bad_table", TRANSFER_ONLY_ABI)?;

        let toml_str = r#"
[[contracts]]
name = "USDC"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/erc20.json"
start_block = 0

[[contracts.events]]
name = "Transfer"
table = "robert'); drop table students;--"
"#;
        let config: SieveConfig = toml::from_str(toml_str)?;
        let Err(err) = resolve_config(&config, &dir) else {
            return Err(eyre::eyre!("expected error for invalid table name"));
        };
        assert!(err.to_string().contains("invalid characters"));

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn parse_factory_toml() -> eyre::Result<()> {
        let toml_str = r#"
[[contracts]]
name = "UniswapV3Pool"
abi = "abis/pool.json"

[contracts.factory]
address = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
event = "PoolCreated"
param = "pool"
start_block = 12369621

[[contracts.events]]
name = "Swap"
table = "uniswap_swaps"
"#;
        let config: SieveConfig = toml::from_str(toml_str)?;
        assert_eq!(config.contracts.len(), 1);

        let c = &config.contracts[0];
        assert!(c.address.is_none());
        assert!(c.start_block.is_none());

        let f = c.factory.as_ref().ok_or_else(|| eyre::eyre!("no factory"))?;
        assert_eq!(f.address, "0x1F98431c8aD98523631AE4a59f267346ea31F984");
        assert_eq!(f.event, "PoolCreated");
        assert_eq!(f.param, "pool");
        assert_eq!(f.start_block, 12_369_621);

        Ok(())
    }

    #[test]
    fn factory_child_table_has_contract_address() {
        let columns = vec![ResolvedColumn {
            column_name: "amount".to_string(),
            param_name: "amount".to_string(),
            pg_type: "numeric".to_string(),
        }];

        let sql = generate_create_table_sql("pool_swaps", &[], &columns, true);
        assert!(sql.contains("contract_address TEXT NOT NULL"));
        // UNIQUE constraint includes contract_address
        assert!(sql.contains("contract_address, block_number, tx_index, log_index"));
    }

    #[test]
    fn factory_child_insert_has_contract_address() {
        let columns = vec![ResolvedColumn {
            column_name: "amount".to_string(),
            param_name: "amount".to_string(),
            pg_type: "numeric".to_string(),
        }];

        let sql = generate_insert_sql("pool_swaps", &[], &columns, true);
        assert!(sql.contains("contract_address"));
        // contract_address is $5, user column starts at $6
        assert!(sql.contains("$5"));
        assert!(sql.contains("$6"));
    }

    // ── Transfer tests ───────────────────────────────────────────────

    #[test]
    fn parse_toml_with_transfers() -> eyre::Result<()> {
        let toml_str = r#"
[[contracts]]
name = "USDC"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/erc20.json"
start_block = 21000000

[[contracts.events]]
name = "Transfer"
table = "usdc_transfers"

[[transfers]]
name = "eth_transfers"
table = "eth_transfers"
start_block = 21000000
context = ["block_timestamp", "tx_gas_price"]

[transfers.filter]
from = ["0x28C6c06298d514Db089934071355E5743bf21d60"]
to = ["0xdAC17F958D2ee523a2206206994597C13D831ec7"]
"#;
        let config: SieveConfig = toml::from_str(toml_str)?;
        assert_eq!(config.transfers.len(), 1);
        let t = &config.transfers[0];
        assert_eq!(t.name, "eth_transfers");
        assert_eq!(t.table, "eth_transfers");
        assert_eq!(t.start_block, Some(21_000_000));
        assert_eq!(t.context.as_ref().map(Vec::len), Some(2));
        let f = t.filter.as_ref().ok_or_else(|| eyre::eyre!("no filter"))?;
        assert_eq!(f.from.as_ref().map(Vec::len), Some(1));
        assert_eq!(f.to.as_ref().map(Vec::len), Some(1));
        Ok(())
    }

    #[test]
    fn parse_toml_without_transfers_is_empty() -> eyre::Result<()> {
        let config: SieveConfig = toml::from_str(MINIMAL_TOML)?;
        assert!(config.transfers.is_empty());
        Ok(())
    }

    #[test]
    fn transfer_create_table_sql() {
        let sql = generate_transfer_create_table_sql("eth_transfers", &[]);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS eth_transfers"));
        assert!(sql.contains("id BIGSERIAL PRIMARY KEY"));
        assert!(sql.contains("block_number BIGINT NOT NULL"));
        assert!(sql.contains("tx_hash BYTEA NOT NULL"));
        assert!(sql.contains("tx_index INTEGER NOT NULL"));
        assert!(sql.contains("from_address TEXT NOT NULL"));
        assert!(sql.contains("to_address TEXT NOT NULL"));
        assert!(sql.contains("value NUMERIC NOT NULL"));
        assert!(sql.contains("UNIQUE (block_number, tx_index)"));
        // No log_index for transfer tables
        assert!(!sql.contains("log_index"));
    }

    #[test]
    fn transfer_insert_sql_with_context() {
        let ctx = vec![ContextField::BlockTimestamp, ContextField::TxGasPrice];
        let sql = generate_transfer_insert_sql("eth_transfers", &ctx);
        assert!(sql.contains("from_address, to_address, value, block_timestamp, tx_gas_price"));
        assert!(sql.contains("$6::numeric, $7, $8"));
        assert!(sql.contains("ON CONFLICT (block_number, tx_index) DO NOTHING"));
    }

    #[test]
    fn transfer_table_name_collision_with_events() -> eyre::Result<()> {
        let mut table_names = HashSet::new();
        table_names.insert("usdc_transfers".to_string());

        let t = TomlTransfer {
            name: "eth".to_string(),
            table: "usdc_transfers".to_string(),
            start_block: None,
            context: None,
            filter: None,
        };
        let Err(err) = resolve_transfer(&t, &mut table_names) else {
            return Err(eyre::eyre!("expected error for duplicate table name"));
        };
        assert!(err.to_string().contains("duplicate table name"));
        Ok(())
    }

    // ── Topic filter tests ──────────────────────────────────────────

    #[test]
    fn parse_event_with_filter() -> eyre::Result<()> {
        let toml_str = r#"
[[contracts]]
name = "USDC"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/erc20.json"
start_block = 21000000

[[contracts.events]]
name = "Transfer"
table = "usdc_transfers"

[contracts.events.filter]
to = ["0x28C6c06298d514Db089934071355E5743bf21d60"]
"#;
        let config: SieveConfig = toml::from_str(toml_str)?;
        let filter = config.contracts[0].events[0].filter.as_ref();
        assert!(filter.is_some());
        let filter = filter.ok_or_else(|| eyre::eyre!("expected filter"))?;
        assert_eq!(filter.len(), 1);
        assert!(filter.contains_key("to"));
        assert_eq!(filter["to"].len(), 1);
        Ok(())
    }

    #[test]
    fn resolve_topic_filters_valid() -> eyre::Result<()> {
        let abi: alloy_json_abi::JsonAbi = serde_json::from_str(ERC20_ABI)
            .map_err(|e| eyre::eyre!("parse ABI: {e}"))?;
        let event = abi.events.get("Transfer")
            .and_then(|v| v.first())
            .ok_or_else(|| eyre::eyre!("no Transfer event"))?;

        let mut filter = HashMap::new();
        filter.insert(
            "to".to_string(),
            vec!["0x28C6c06298d514Db089934071355E5743bf21d60".to_string()],
        );

        let filters = resolve_topic_filters(Some(&filter), event, "USDC", "Transfer")?;
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].topic_index, 2); // 'to' is the second indexed param
        assert_eq!(filters[0].values.len(), 1);

        // Verify B256 encoding: address left-padded to 32 bytes
        let expected_addr: Address = "0x28C6c06298d514Db089934071355E5743bf21d60".parse()
            .map_err(|e| eyre::eyre!("{e}"))?;
        let expected = B256::left_padding_from(expected_addr.as_slice());
        assert!(filters[0].values.contains(&expected));
        Ok(())
    }

    #[test]
    fn resolve_topic_filters_unknown_param_errors() -> eyre::Result<()> {
        let abi: alloy_json_abi::JsonAbi = serde_json::from_str(ERC20_ABI)
            .map_err(|e| eyre::eyre!("parse ABI: {e}"))?;
        let event = abi.events.get("Transfer")
            .and_then(|v| v.first())
            .ok_or_else(|| eyre::eyre!("no Transfer event"))?;

        let mut filter = HashMap::new();
        filter.insert("nonexistent".to_string(), vec!["0x00".to_string()]);

        let Err(err) = resolve_topic_filters(Some(&filter), event, "USDC", "Transfer") else {
            return Err(eyre::eyre!("expected error for unknown param"));
        };
        assert!(format!("{err:?}").contains("not found"));
        Ok(())
    }

    #[test]
    fn resolve_topic_filters_non_indexed_errors() -> eyre::Result<()> {
        let abi: alloy_json_abi::JsonAbi = serde_json::from_str(ERC20_ABI)
            .map_err(|e| eyre::eyre!("parse ABI: {e}"))?;
        let event = abi.events.get("Transfer")
            .and_then(|v| v.first())
            .ok_or_else(|| eyre::eyre!("no Transfer event"))?;

        // 'value' is NOT indexed in Transfer(address indexed from, address indexed to, uint256 value)
        let mut filter = HashMap::new();
        filter.insert(
            "value".to_string(),
            vec!["100".to_string()],
        );

        let Err(err) = resolve_topic_filters(Some(&filter), event, "USDC", "Transfer") else {
            return Err(eyre::eyre!("expected error for non-indexed param"));
        };
        assert!(format!("{err:?}").contains("not indexed"));
        Ok(())
    }

    #[test]
    fn resolve_topic_filters_multi_value() -> eyre::Result<()> {
        let abi: alloy_json_abi::JsonAbi = serde_json::from_str(ERC20_ABI)
            .map_err(|e| eyre::eyre!("parse ABI: {e}"))?;
        let event = abi.events.get("Transfer")
            .and_then(|v| v.first())
            .ok_or_else(|| eyre::eyre!("no Transfer event"))?;

        let mut filter = HashMap::new();
        filter.insert(
            "from".to_string(),
            vec![
                "0x28C6c06298d514Db089934071355E5743bf21d60".to_string(),
                "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_string(),
            ],
        );

        let filters = resolve_topic_filters(Some(&filter), event, "USDC", "Transfer")?;
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].topic_index, 1); // 'from' is the first indexed param
        assert_eq!(filters[0].values.len(), 2);
        Ok(())
    }

    #[test]
    fn encode_topic_value_address() -> eyre::Result<()> {
        let addr = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
        let b256 = encode_topic_value(addr, "address")?;
        // First 12 bytes should be zero (left-padding)
        assert!(b256.as_slice()[..12].iter().all(|&b| b == 0));
        // Last 20 bytes should be the address
        let parsed: Address = addr.parse().map_err(|e| eyre::eyre!("{e}"))?;
        assert_eq!(&b256.as_slice()[12..], parsed.as_slice());
        Ok(())
    }

    #[test]
    fn encode_topic_value_uint() -> eyre::Result<()> {
        let b256 = encode_topic_value("100", "uint256")?;
        assert_eq!(b256.as_slice()[31], 100);
        assert!(b256.as_slice()[..31].iter().all(|&b| b == 0));
        Ok(())
    }

    #[test]
    fn encode_topic_value_bool() -> eyre::Result<()> {
        let t = encode_topic_value("true", "bool")?;
        assert_eq!(t.as_slice()[31], 1);
        assert!(t.as_slice()[..31].iter().all(|&b| b == 0));

        let f = encode_topic_value("false", "bool")?;
        assert_eq!(f, B256::ZERO);
        Ok(())
    }

    #[test]
    fn resolve_config_with_topic_filters() -> eyre::Result<()> {
        let dir = setup_test_dir("topic_filter", ERC20_ABI)?;

        let toml_str = r#"
[[contracts]]
name = "USDC"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/erc20.json"
start_block = 21000000

[[contracts.events]]
name = "Transfer"
table = "usdc_transfers"

[contracts.events.filter]
to = ["0x28C6c06298d514Db089934071355E5743bf21d60"]
"#;
        let config: SieveConfig = toml::from_str(toml_str)?;
        let resolved = resolve_config(&config, &dir)?;

        // Check resolved event has topic filters
        assert_eq!(resolved.resolved_events.len(), 1);
        assert_eq!(resolved.resolved_events[0].topic_filters.len(), 1);
        assert_eq!(resolved.resolved_events[0].topic_filters[0].topic_index, 2);

        // Check ContractConfig also has topic filters
        let contract = &resolved.index_config.contracts[0];
        assert_eq!(contract.topic_filters.len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    // ── Call trace tests ──────────────────────────────────────────────

    /// ABI with both Transfer event and transfer function.
    const ERC20_WITH_FN_ABI: &str = r#"[
        {"anonymous":false,"inputs":[
            {"indexed":true,"internalType":"address","name":"from","type":"address"},
            {"indexed":true,"internalType":"address","name":"to","type":"address"},
            {"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}
        ],"name":"Transfer","type":"event"},
        {"inputs":[
            {"internalType":"address","name":"to","type":"address"},
            {"internalType":"uint256","name":"value","type":"uint256"}
        ],"name":"transfer","outputs":[
            {"internalType":"bool","name":"","type":"bool"}
        ],"stateMutability":"nonpayable","type":"function"}
    ]"#;

    #[test]
    fn parse_toml_with_calls() -> eyre::Result<()> {
        let toml_str = r#"
            [[contracts]]
            name = "USDC"
            address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
            abi = "abis/erc20.json"
            start_block = 21_000_000

            [[contracts.events]]
            name = "Transfer"
            table = "usdc_transfers"

            [[contracts.calls]]
            name = "transfer"
            table = "usdc_transfer_calls"
            context = ["block_timestamp"]
            columns = [
              { param = "to",    name = "to_address", type = "text" },
              { param = "value", name = "value",      type = "numeric" },
            ]
        "#;
        let config: SieveConfig = toml::from_str(toml_str)?;
        assert_eq!(config.contracts[0].calls.len(), 1);
        assert_eq!(config.contracts[0].calls[0].name, "transfer");
        assert_eq!(config.contracts[0].calls[0].table, "usdc_transfer_calls");
        Ok(())
    }

    #[test]
    fn call_create_table_sql() {
        let sql = generate_call_create_table_sql(
            "swap_calls",
            &[],
            &[
                ResolvedColumn {
                    column_name: "recipient".to_string(),
                    param_name: "recipient".to_string(),
                    pg_type: "text".to_string(),
                },
            ],
            false,
        );
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS swap_calls"));
        assert!(sql.contains("block_number BIGINT NOT NULL"));
        assert!(sql.contains("tx_hash BYTEA NOT NULL"));
        assert!(sql.contains("tx_index INTEGER NOT NULL"));
        assert!(sql.contains("recipient text NOT NULL"));
        assert!(sql.contains("UNIQUE (block_number, tx_index)"));
        // No log_index for calls
        assert!(!sql.contains("log_index"));
    }

    #[test]
    fn call_create_table_factory_child() {
        let sql = generate_call_create_table_sql(
            "pool_calls",
            &[],
            &[],
            true,
        );
        assert!(sql.contains("contract_address TEXT NOT NULL"));
        assert!(sql.contains("UNIQUE (contract_address, block_number, tx_index)"));
    }

    #[test]
    fn call_insert_sql() {
        let sql = generate_call_insert_sql(
            "swap_calls",
            &[ContextField::BlockTimestamp],
            &[
                ResolvedColumn {
                    column_name: "recipient".to_string(),
                    param_name: "recipient".to_string(),
                    pg_type: "text".to_string(),
                },
            ],
            false,
        );
        // Standard 3 params + 1 context + 1 user = 5
        assert!(sql.contains("$1"));
        assert!(sql.contains("$5"));
        assert!(sql.contains("block_number, tx_hash, tx_index, block_timestamp, recipient"));
    }

    #[test]
    fn call_table_name_collision_with_events() -> eyre::Result<()> {
        let dir = setup_test_dir("call_collision", ERC20_WITH_FN_ABI)?;

        let toml_str = r#"
[[contracts]]
name = "USDC"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/erc20.json"
start_block = 21000000

[[contracts.events]]
name = "Transfer"
table = "usdc_transfers"

[[contracts.calls]]
name = "transfer"
table = "usdc_transfers"
"#;
        let config: SieveConfig = toml::from_str(toml_str)?;
        let Err(err) = resolve_config(&config, &dir) else {
            return Err(eyre::eyre!("expected error for duplicate call/event table names"));
        };
        assert!(err.to_string().contains("duplicate table name"));

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn resolve_config_with_calls() -> eyre::Result<()> {
        let dir = setup_test_dir("call_resolve", ERC20_WITH_FN_ABI)?;

        let toml_str = r#"
[[contracts]]
name = "USDC"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/erc20.json"
start_block = 21000000

[[contracts.events]]
name = "Transfer"
table = "usdc_transfers"

[[contracts.calls]]
name = "transfer"
table = "usdc_transfer_calls"
context = ["block_timestamp", "tx_from"]
columns = [
  { param = "to",    name = "to_address", type = "text" },
  { param = "value", name = "value",      type = "numeric" },
]
"#;
        let config: SieveConfig = toml::from_str(toml_str)?;
        let resolved = resolve_config(&config, &dir)?;

        // Check resolved calls
        assert_eq!(resolved.calls.len(), 1);
        let call = &resolved.calls[0];
        assert_eq!(call.function_name, "transfer");
        assert_eq!(call.contract_name, "USDC");
        assert_eq!(call.table_name, "usdc_transfer_calls");
        assert_eq!(call.context_fields.len(), 2);
        assert_eq!(call.columns.len(), 2);
        assert_eq!(call.columns[0].column_name, "to_address");
        assert_eq!(call.columns[1].column_name, "value");
        assert!(!call.is_factory_child);

        // Check ContractConfig has the function selector
        let contract = &resolved.index_config.contracts[0];
        assert_eq!(contract.functions.len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    // ── Stream config tests ──────────────────────────────────────────

    #[test]
    fn parse_toml_with_streams() -> eyre::Result<()> {
        let toml_str = r#"
[[contracts]]
name = "USDC"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/erc20.json"
start_block = 21000000

[[contracts.events]]
name = "Transfer"
table = "usdc_transfers"

[[streams]]
name = "my_webhook"
type = "webhook"
url = "http://localhost:8080/events"
backfill = false
"#;
        let config: SieveConfig = toml::from_str(toml_str)?;
        assert_eq!(config.streams.len(), 1);
        assert_eq!(config.streams[0].name, "my_webhook");
        assert_eq!(config.streams[0].stream_type, "webhook");
        assert_eq!(config.streams[0].url.as_deref(), Some("http://localhost:8080/events"));
        assert!(!config.streams[0].backfill);
        Ok(())
    }

    #[test]
    fn stream_backfill_defaults_to_true() -> eyre::Result<()> {
        let toml_str = r#"
[[contracts]]
name = "USDC"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/erc20.json"
start_block = 21000000

[[contracts.events]]
name = "Transfer"
table = "usdc_transfers"

[[streams]]
name = "my_webhook"
type = "webhook"
url = "http://localhost:8080/events"
"#;
        let config: SieveConfig = toml::from_str(toml_str)?;
        assert!(config.streams[0].backfill);
        Ok(())
    }

    #[test]
    fn resolve_streams_rejects_duplicate_names() {
        let streams = vec![
            TomlStream {
                name: "dup".to_string(),
                stream_type: "webhook".to_string(),
                url: Some("http://a".to_string()),
                backfill: true,
                exchange: None,
                routing_key: None,
            },
            TomlStream {
                name: "dup".to_string(),
                stream_type: "webhook".to_string(),
                url: Some("http://b".to_string()),
                backfill: true,
                exchange: None,
                routing_key: None,
            },
        ];
        let result = resolve_streams(&streams);
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("duplicate stream name"));
    }

    #[test]
    fn resolve_streams_rejects_unknown_type() {
        let streams = vec![TomlStream {
            name: "bad".to_string(),
            stream_type: "kafka".to_string(),
            url: Some("http://a".to_string()),
            backfill: true,
            exchange: None,
            routing_key: None,
        }];
        let result = resolve_streams(&streams);
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("unknown stream type"));
    }

    #[test]
    fn resolve_streams_rejects_missing_url() {
        let streams = vec![TomlStream {
            name: "no_url".to_string(),
            stream_type: "webhook".to_string(),
            url: None,
            backfill: true,
            exchange: None,
            routing_key: None,
        }];
        let result = resolve_streams(&streams);
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("requires a 'url'"));
    }

    #[test]
    fn resolve_streams_rejects_empty_url() {
        let streams = vec![TomlStream {
            name: "empty_url".to_string(),
            stream_type: "webhook".to_string(),
            url: Some(String::new()),
            backfill: true,
            exchange: None,
            routing_key: None,
        }];
        let result = resolve_streams(&streams);
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("requires a 'url'"));
    }

    #[test]
    fn resolve_streams_valid() -> eyre::Result<()> {
        let streams = vec![
            TomlStream {
                name: "hook_a".to_string(),
                stream_type: "webhook".to_string(),
                url: Some("http://localhost:8080".to_string()),
                backfill: true,
                exchange: None,
                routing_key: None,
            },
            TomlStream {
                name: "hook_b".to_string(),
                stream_type: "webhook".to_string(),
                url: Some("http://localhost:9090".to_string()),
                backfill: false,
                exchange: None,
                routing_key: None,
            },
        ];
        let resolved = resolve_streams(&streams)?;
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].name, "hook_a");
        assert_eq!(resolved[0].stream_type, "webhook");
        assert!(resolved[0].backfill);
        assert_eq!(resolved[1].name, "hook_b");
        assert!(!resolved[1].backfill);
        Ok(())
    }

    #[test]
    fn resolve_rabbitmq_stream() -> eyre::Result<()> {
        let rk = ["events.", "{table}"].concat();
        let streams = vec![TomlStream {
            name: "mq".to_string(),
            stream_type: "rabbitmq".to_string(),
            url: Some("amqp://guest:guest@localhost:5672/%2f".to_string()),
            backfill: false,
            exchange: Some("sieve_events".to_string()),
            routing_key: Some(rk.clone()),
        }];
        let resolved = resolve_streams(&streams)?;
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].stream_type, "rabbitmq");
        assert_eq!(resolved[0].exchange.as_deref(), Some("sieve_events"));
        assert_eq!(resolved[0].routing_key.as_deref(), Some(rk.as_str()));
        Ok(())
    }

    #[test]
    fn resolve_rabbitmq_missing_exchange() {
        let streams = vec![TomlStream {
            name: "mq".to_string(),
            stream_type: "rabbitmq".to_string(),
            url: Some("amqp://localhost".to_string()),
            backfill: true,
            exchange: None,
            routing_key: None,
        }];
        let result = resolve_streams(&streams);
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("requires an 'exchange'"));
    }

    #[test]
    fn resolve_rabbitmq_missing_url() {
        let streams = vec![TomlStream {
            name: "mq".to_string(),
            stream_type: "rabbitmq".to_string(),
            url: None,
            backfill: true,
            exchange: Some("ex".to_string()),
            routing_key: None,
        }];
        let result = resolve_streams(&streams);
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("requires a 'url'"));
    }

    #[test]
    fn resolve_rabbitmq_default_routing_key() -> eyre::Result<()> {
        let streams = vec![TomlStream {
            name: "mq".to_string(),
            stream_type: "rabbitmq".to_string(),
            url: Some("amqp://localhost".to_string()),
            backfill: true,
            exchange: Some("ex".to_string()),
            routing_key: None,
        }];
        let resolved = resolve_streams(&streams)?;
        let expected = ["{table}", ".", "{event}"].concat();
        assert_eq!(resolved[0].routing_key.as_deref(), Some(expected.as_str()));
        Ok(())
    }
}
