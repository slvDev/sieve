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
    /// Hex address with 0x prefix.
    pub address: String,
    /// Path to ABI JSON file (relative to config dir).
    pub abi: String,
    /// Block number to start indexing from.
    pub start_block: u64,
    /// Events to index from this contract.
    pub events: Vec<TomlEvent>,
}

/// An event definition from TOML.
#[derive(Debug, Deserialize)]
pub struct TomlEvent {
    /// Event name as it appears in the ABI (e.g. "Transfer").
    pub name: String,
    /// Postgres table name for this event's data.
    pub table: String,
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
#[must_use]
fn generate_create_table_sql(table: &str, columns: &[ResolvedColumn]) -> String {
    use std::fmt::Write;

    let mut sql = format!(
        "CREATE TABLE IF NOT EXISTS {table} (\n  \
         id BIGSERIAL PRIMARY KEY,\n  \
         block_number BIGINT NOT NULL,\n  \
         tx_hash BYTEA NOT NULL,\n  \
         tx_index INTEGER NOT NULL,\n  \
         log_index INTEGER NOT NULL"
    );

    for col in columns {
        let _ = write!(sql, ",\n  {} {} NOT NULL", col.column_name, col.pg_type);
    }

    sql.push_str(",\n  UNIQUE (block_number, tx_index, log_index)\n)");

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
#[must_use]
fn generate_insert_sql(table: &str, columns: &[ResolvedColumn]) -> String {
    use std::fmt::Write;

    let mut col_names = String::from("block_number, tx_hash, tx_index, log_index");
    for col in columns {
        col_names.push_str(", ");
        col_names.push_str(&col.column_name);
    }

    // Standard columns ($1-$4) have native Rust type bindings.
    // User columns may need explicit casts when bound as text (e.g. numeric).
    let mut params = String::from("$1, $2, $3, $4");
    for (i, col) in columns.iter().enumerate() {
        params.push_str(", ");
        let param_idx = i + 5;
        if col.pg_type == "numeric" {
            let _ = write!(params, "${param_idx}::numeric");
        } else {
            let _ = write!(params, "${param_idx}");
        }
    }

    format!(
        "INSERT INTO {table} ({col_names}) VALUES ({params}) \
         ON CONFLICT (block_number, tx_index, log_index) DO NOTHING"
    )
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
    let mut table_names = HashSet::new();

    for contract in &config.contracts {
        // Validate address
        let address: Address = contract
            .address
            .parse()
            .wrap_err_with(|| format!("invalid address for contract '{}'", contract.name))?;

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

        let event_names: Vec<&str> = contract.events.iter().map(|e| e.name.as_str()).collect();

        // Resolve each event
        for toml_event in &contract.events {
            // Validate and check for duplicate table names
            validate_identifier(&toml_event.table, "table")?;
            if !table_names.insert(toml_event.table.clone()) {
                return Err(eyre::eyre!(
                    "duplicate table name '{}' across events",
                    toml_event.table
                ));
            }

            // Find event in ABI
            let abi_events = abi.events.get(&toml_event.name).ok_or_else(|| {
                eyre::eyre!(
                    "event '{}' not found in ABI for contract '{}'",
                    toml_event.name,
                    contract.name
                )
            })?;
            let abi_event = abi_events.first().ok_or_else(|| {
                eyre::eyre!(
                    "no variants for event '{}' in contract '{}'",
                    toml_event.name,
                    contract.name
                )
            })?;

            // Resolve columns
            let columns = resolve_columns(
                toml_event.columns.as_deref(),
                abi_event,
                &contract.name,
                &toml_event.name,
            )?;

            let create_table_sql =
                generate_create_table_sql(&toml_event.table, &columns);
            let create_indexes_sql = generate_indexes_sql(&toml_event.table);
            let insert_sql = generate_insert_sql(&toml_event.table, &columns);
            let rollback_sql = generate_rollback_sql(&toml_event.table);

            resolved_events.push(ResolvedEvent {
                event_name: toml_event.name.clone(),
                contract_name: contract.name.clone(),
                table_name: toml_event.table.clone(),
                columns,
                insert_sql,
                create_table_sql,
                create_indexes_sql,
                rollback_sql,
            });
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
        "resolved TOML config"
    );

    Ok(ResolvedConfig {
        index_config,
        resolved_events,
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

        let sql = generate_create_table_sql("usdc_transfers", &columns);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS usdc_transfers"));
        assert!(sql.contains("id BIGSERIAL PRIMARY KEY"));
        assert!(sql.contains("block_number BIGINT NOT NULL"));
        assert!(sql.contains("tx_hash BYTEA NOT NULL"));
        assert!(sql.contains("from_address text NOT NULL"));
        assert!(sql.contains("value numeric NOT NULL"));
        assert!(sql.contains("UNIQUE (block_number, tx_index, log_index)"));
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

        let sql = generate_insert_sql("usdc_transfers", &columns);
        assert!(sql.contains("INSERT INTO usdc_transfers"));
        assert!(sql.contains("block_number, tx_hash, tx_index, log_index, from_address, to_address"));
        assert!(sql.contains("$1, $2, $3, $4, $5, $6"));
        assert!(sql.contains("ON CONFLICT"));
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
        let ResolvedConfig { index_config, resolved_events: resolved } = resolve_config(&config, &dir)?;

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
}
