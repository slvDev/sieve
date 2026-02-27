//! TOML configuration file parsing and resolution.
//!
//! Reads `sieve.toml`, parses contract/event definitions, resolves ABI files,
//! and produces [`IndexConfig`] + [`ResolvedEvent`] structs for the pipeline.

use crate::config::{ContractConfig, IndexConfig};
use alloy_json_abi::JsonAbi;
use alloy_primitives::Address;
use eyre::WrapErr;
use serde::Deserialize;
use std::collections::HashSet;
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
    use std::fmt::Write;

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
        if matches!(cf, ContextField::TxValue) {
            let _ = write!(params, "${param_idx}::numeric");
        } else {
            let _ = write!(params, "${param_idx}");
        }
        param_idx += 1;
    }

    for col in columns {
        params.push_str(", ");
        if col.pg_type == "numeric" {
            let _ = write!(params, "${param_idx}::numeric");
        } else {
            let _ = write!(params, "${param_idx}");
        }
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
    let mut table_names = HashSet::new();

    for contract in &config.contracts {
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
                &mut table_names,
                &mut resolved_events,
            )?;
        }

        // Build ContractConfig for the filter/decode pipeline
        let contract_config = ContractConfig::from_abi(
            &contract.name,
            address,
            &abi,
            &event_names,
        )?;
        contract_configs.push(contract_config);
    }

    let index_config = IndexConfig::new(contract_configs);

    info!(
        contracts = config.contracts.len(),
        events = resolved_events.len(),
        factories = resolved_factories.len(),
        "resolved TOML config"
    );

    Ok(ResolvedConfig {
        index_config,
        resolved_events,
        factories: resolved_factories,
    })
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
}
