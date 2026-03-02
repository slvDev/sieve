//! Type mapping between PostgreSQL, GraphQL, and Rust.
//!
//! Provides helpers for building SELECT clauses with proper type casts,
//! extracting `PgRow` values into JSON maps, and mapping PG types to
//! GraphQL `TypeRef` values.

use async_graphql::dynamic::TypeRef;
use crate::toml_config::{ContextField, ResolvedCall, ResolvedEvent, ResolvedTransfer};
use serde_json::Value as JsonValue;
use sqlx::postgres::PgRow;
use sqlx::Row;
use std::collections::HashMap;

// ── Column metadata ──────────────────────────────────────────────────

/// Metadata for a single column in a user table.
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    /// Column name in PostgreSQL.
    pub name: String,
    /// PostgreSQL type (e.g. "text", "bigint", "numeric", "boolean", "bytea", "integer").
    pub pg_type: String,
    /// Whether this column is nullable.
    pub nullable: bool,
}

/// Build column metadata from a resolved event.
///
/// Includes standard columns, context columns, and user columns.
#[must_use]
pub fn build_columns_meta(event: &ResolvedEvent) -> Vec<ColumnMeta> {
    let mut cols = vec![
        ColumnMeta { name: "id".to_string(), pg_type: "bigserial".to_string(), nullable: false },
        ColumnMeta { name: "block_number".to_string(), pg_type: "bigint".to_string(), nullable: false },
        ColumnMeta { name: "tx_hash".to_string(), pg_type: "bytea".to_string(), nullable: false },
        ColumnMeta { name: "tx_index".to_string(), pg_type: "integer".to_string(), nullable: false },
        ColumnMeta { name: "log_index".to_string(), pg_type: "integer".to_string(), nullable: false },
    ];

    if event.is_factory_child {
        cols.push(ColumnMeta {
            name: "contract_address".to_string(),
            pg_type: "text".to_string(),
            nullable: false,
        });
    }

    for cf in &event.context_fields {
        cols.push(ColumnMeta {
            name: cf.pg_column_name().to_string(),
            pg_type: context_field_base_type(*cf).to_string(),
            nullable: *cf == ContextField::TxTo,
        });
    }

    for col in &event.columns {
        cols.push(ColumnMeta {
            name: col.column_name.clone(),
            pg_type: col.pg_type.clone(),
            nullable: false,
        });
    }

    cols
}

/// Build column metadata from a resolved native transfer definition.
///
/// Fixed columns: id, block_number, tx_hash, tx_index, from_address, to_address, value.
/// Plus context columns.
#[must_use]
pub fn build_transfer_columns_meta(transfer: &ResolvedTransfer) -> Vec<ColumnMeta> {
    let mut cols = vec![
        ColumnMeta { name: "id".to_string(), pg_type: "bigserial".to_string(), nullable: false },
        ColumnMeta { name: "block_number".to_string(), pg_type: "bigint".to_string(), nullable: false },
        ColumnMeta { name: "tx_hash".to_string(), pg_type: "bytea".to_string(), nullable: false },
        ColumnMeta { name: "tx_index".to_string(), pg_type: "integer".to_string(), nullable: false },
        ColumnMeta { name: "from_address".to_string(), pg_type: "text".to_string(), nullable: false },
        ColumnMeta { name: "to_address".to_string(), pg_type: "text".to_string(), nullable: false },
        ColumnMeta { name: "value".to_string(), pg_type: "numeric".to_string(), nullable: false },
    ];

    for cf in &transfer.context_fields {
        cols.push(ColumnMeta {
            name: cf.pg_column_name().to_string(),
            pg_type: context_field_base_type(*cf).to_string(),
            nullable: *cf == ContextField::TxTo,
        });
    }

    cols
}

/// Build column metadata from a resolved function call definition.
///
/// Standard columns: id, block_number, tx_hash, tx_index (no log_index).
/// Plus optional contract_address, context columns, and user columns.
#[must_use]
pub fn build_call_columns_meta(call: &ResolvedCall) -> Vec<ColumnMeta> {
    let mut cols = vec![
        ColumnMeta { name: "id".to_string(), pg_type: "bigserial".to_string(), nullable: false },
        ColumnMeta { name: "block_number".to_string(), pg_type: "bigint".to_string(), nullable: false },
        ColumnMeta { name: "tx_hash".to_string(), pg_type: "bytea".to_string(), nullable: false },
        ColumnMeta { name: "tx_index".to_string(), pg_type: "integer".to_string(), nullable: false },
    ];

    if call.is_factory_child {
        cols.push(ColumnMeta {
            name: "contract_address".to_string(),
            pg_type: "text".to_string(),
            nullable: false,
        });
    }

    for cf in &call.context_fields {
        cols.push(ColumnMeta {
            name: cf.pg_column_name().to_string(),
            pg_type: context_field_base_type(*cf).to_string(),
            nullable: *cf == ContextField::TxTo,
        });
    }

    for col in &call.columns {
        cols.push(ColumnMeta {
            name: col.column_name.clone(),
            pg_type: col.pg_type.clone(),
            nullable: false,
        });
    }

    cols
}

/// Base PG type for a context field (without NOT NULL suffix).
const fn context_field_base_type(cf: ContextField) -> &'static str {
    match cf {
        ContextField::BlockTimestamp | ContextField::TxGasPrice => "bigint",
        ContextField::BlockHash => "bytea",
        ContextField::TxFrom | ContextField::TxTo => "text",
        ContextField::TxValue => "numeric",
    }
}

// ── SELECT clause generation ─────────────────────────────────────────

/// Build a SELECT column list with appropriate casts.
///
/// - `bytea` columns: `'0x' || encode(col, 'hex') AS col`
/// - `numeric` columns: `col::text AS col`
/// - `bigint`/`bigserial` columns: `col::text AS col`
/// - others: `col` as-is
#[must_use]
pub fn build_select_clause(columns: &[ColumnMeta]) -> String {
    let exprs: Vec<String> = columns
        .iter()
        .map(|col| {
            let name = &col.name;
            match col.pg_type.as_str() {
                "bytea" => format!("'0x' || encode({name}, 'hex') AS {name}"),
                "numeric" | "bigint" | "bigserial" => format!("{name}::text AS {name}"),
                _ => name.clone(),
            }
        })
        .collect();
    exprs.join(", ")
}

// ── Row extraction ───────────────────────────────────────────────────

/// Extract a `PgRow` into a JSON map using column metadata.
///
/// After `build_select_clause` casts, the actual Rust types are:
/// - `bytea` → `String` (already hex-encoded in SELECT)
/// - `numeric`/`bigint`/`bigserial` → `String` (cast to text in SELECT)
/// - `text` → `String`
/// - `boolean` → `bool`
/// - `integer` → `i32`
///
/// # Errors
///
/// Returns an error if a column value cannot be extracted.
pub fn row_to_json(
    row: &PgRow,
    columns: &[ColumnMeta],
) -> eyre::Result<HashMap<String, JsonValue>> {
    let mut map = HashMap::with_capacity(columns.len());

    for col in columns {
        let name = col.name.as_str();
        let value = extract_column(row, name, &col.pg_type, col.nullable)?;
        map.insert(col.name.clone(), value);
    }

    Ok(map)
}

fn extract_column(
    row: &PgRow,
    name: &str,
    pg_type: &str,
    nullable: bool,
) -> eyre::Result<JsonValue> {
    // After SELECT casts, numeric/bigint/bigserial/bytea are all text
    match pg_type {
        "boolean" => {
            if nullable {
                let v: Option<bool> = row.try_get(name)?;
                Ok(v.map_or(JsonValue::Null, JsonValue::Bool))
            } else {
                let v: bool = row.try_get(name)?;
                Ok(JsonValue::Bool(v))
            }
        }
        "integer" => {
            if nullable {
                let v: Option<i32> = row.try_get(name)?;
                Ok(v.map_or(JsonValue::Null, |n| JsonValue::Number(n.into())))
            } else {
                let v: i32 = row.try_get(name)?;
                Ok(JsonValue::Number(v.into()))
            }
        }
        // Everything else was cast to text in SELECT (bigint, numeric, bytea, text, bigserial)
        _ => {
            if nullable {
                let v: Option<String> = row.try_get(name)?;
                Ok(v.map_or(JsonValue::Null, JsonValue::String))
            } else {
                let v: String = row.try_get(name)?;
                Ok(JsonValue::String(v))
            }
        }
    }
}

// ── GraphQL type mapping ─────────────────────────────────────────────

/// Map a PG type to a GraphQL output `TypeRef`.
///
/// All numeric types become `String` because GraphQL `Int` is only i32.
#[must_use]
pub fn pg_to_graphql_type(pg_type: &str, nullable: bool) -> TypeRef {
    let base = match pg_type {
        "boolean" => TypeRef::BOOLEAN,
        "integer" => TypeRef::INT,
        "bigserial" => TypeRef::ID,
        // bigint, numeric, bytea, text → all String
        _ => TypeRef::STRING,
    };

    if nullable {
        TypeRef::named(base)
    } else {
        TypeRef::named_nn(base)
    }
}

/// Filter operator suffixes for a given PG type.
///
/// Returns `(suffix, graphql_input_type)` pairs. Empty suffix = equality.
#[must_use]
pub fn operators_for_type(pg_type: &str) -> Vec<(&'static str, TypeRef)> {
    match pg_type {
        "text" => vec![
            ("", TypeRef::named(TypeRef::STRING)),
            ("_ne", TypeRef::named(TypeRef::STRING)),
            ("_contains", TypeRef::named(TypeRef::STRING)),
            ("_starts_with", TypeRef::named(TypeRef::STRING)),
            ("_in", TypeRef::named_list(TypeRef::STRING)),
            ("_not_in", TypeRef::named_list(TypeRef::STRING)),
        ],
        "bigint" | "numeric" | "integer" | "bigserial" => vec![
            ("", TypeRef::named(TypeRef::STRING)),
            ("_ne", TypeRef::named(TypeRef::STRING)),
            ("_gt", TypeRef::named(TypeRef::STRING)),
            ("_gte", TypeRef::named(TypeRef::STRING)),
            ("_lt", TypeRef::named(TypeRef::STRING)),
            ("_lte", TypeRef::named(TypeRef::STRING)),
            ("_in", TypeRef::named_list(TypeRef::STRING)),
            ("_not_in", TypeRef::named_list(TypeRef::STRING)),
        ],
        "boolean" => vec![
            ("", TypeRef::named(TypeRef::BOOLEAN)),
            ("_ne", TypeRef::named(TypeRef::BOOLEAN)),
            ("_in", TypeRef::named_list(TypeRef::BOOLEAN)),
            ("_not_in", TypeRef::named_list(TypeRef::BOOLEAN)),
        ],
        "bytea" => vec![
            ("", TypeRef::named(TypeRef::STRING)),
            ("_ne", TypeRef::named(TypeRef::STRING)),
            ("_in", TypeRef::named_list(TypeRef::STRING)),
            ("_not_in", TypeRef::named_list(TypeRef::STRING)),
        ],
        _ => vec![
            ("", TypeRef::named(TypeRef::STRING)),
            ("_in", TypeRef::named_list(TypeRef::STRING)),
            ("_not_in", TypeRef::named_list(TypeRef::STRING)),
        ],
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Convert a snake_case table name to `PascalCase`.
///
/// `usdc_transfers` → `UsdcTransfers`
#[must_use]
pub fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|part| {
            let mut chars = part.chars();
            chars.next().map_or_else(String::new, |c| {
                let upper: String = c.to_uppercase().collect();
                format!("{upper}{}", chars.as_str().to_lowercase())
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::toml_config::ResolvedColumn;

    fn test_event() -> ResolvedEvent {
        ResolvedEvent {
            event_name: "Transfer".to_string(),
            contract_name: "USDC".to_string(),
            table_name: "usdc_transfers".to_string(),
            context_fields: vec![ContextField::BlockTimestamp, ContextField::TxFrom],
            columns: vec![
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
            ],
            insert_sql: String::new(),
            create_table_sql: String::new(),
            create_indexes_sql: vec![],
            rollback_sql: String::new(),
            is_factory_child: false,
            topic_filters: vec![],
        }
    }

    #[test]
    fn pascal_case_conversion() {
        assert_eq!(to_pascal_case("usdc_transfers"), "UsdcTransfers");
        assert_eq!(to_pascal_case("hello"), "Hello");
        assert_eq!(to_pascal_case("a_b_c"), "ABC");
    }

    #[test]
    fn columns_meta_includes_all_columns() {
        let event = test_event();
        let meta = build_columns_meta(&event);
        let names: Vec<&str> = meta.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["id", "block_number", "tx_hash", "tx_index", "log_index",
                 "block_timestamp", "tx_from", "from_address", "value"]
        );
    }

    #[test]
    fn select_clause_casts_correctly() {
        let meta = vec![
            ColumnMeta { name: "id".to_string(), pg_type: "bigserial".to_string(), nullable: false },
            ColumnMeta { name: "block_number".to_string(), pg_type: "bigint".to_string(), nullable: false },
            ColumnMeta { name: "tx_hash".to_string(), pg_type: "bytea".to_string(), nullable: false },
            ColumnMeta { name: "name".to_string(), pg_type: "text".to_string(), nullable: false },
            ColumnMeta { name: "amount".to_string(), pg_type: "numeric".to_string(), nullable: false },
            ColumnMeta { name: "active".to_string(), pg_type: "boolean".to_string(), nullable: false },
            ColumnMeta { name: "idx".to_string(), pg_type: "integer".to_string(), nullable: false },
        ];
        let clause = build_select_clause(&meta);
        assert_eq!(
            clause,
            "id::text AS id, block_number::text AS block_number, \
             '0x' || encode(tx_hash, 'hex') AS tx_hash, \
             name, amount::text AS amount, active, idx"
        );
    }

    #[test]
    fn graphql_type_mapping() {
        // Non-nullable
        assert_eq!(format!("{:?}", pg_to_graphql_type("text", false)), format!("{:?}", TypeRef::named_nn(TypeRef::STRING)));
        assert_eq!(format!("{:?}", pg_to_graphql_type("boolean", false)), format!("{:?}", TypeRef::named_nn(TypeRef::BOOLEAN)));
        assert_eq!(format!("{:?}", pg_to_graphql_type("integer", false)), format!("{:?}", TypeRef::named_nn(TypeRef::INT)));
        assert_eq!(format!("{:?}", pg_to_graphql_type("bigserial", false)), format!("{:?}", TypeRef::named_nn(TypeRef::ID)));
        // bigint → String (not Int, because i32 overflow)
        assert_eq!(format!("{:?}", pg_to_graphql_type("bigint", false)), format!("{:?}", TypeRef::named_nn(TypeRef::STRING)));
        // Nullable
        assert_eq!(format!("{:?}", pg_to_graphql_type("text", true)), format!("{:?}", TypeRef::named(TypeRef::STRING)));
    }

    #[test]
    fn operators_for_text_type() {
        let ops = operators_for_type("text");
        let suffixes: Vec<&str> = ops.iter().map(|(s, _)| *s).collect();
        assert_eq!(
            suffixes,
            vec!["", "_ne", "_contains", "_starts_with", "_in", "_not_in"]
        );
    }

    #[test]
    fn operators_for_numeric_type() {
        let ops = operators_for_type("bigint");
        let suffixes: Vec<&str> = ops.iter().map(|(s, _)| *s).collect();
        assert_eq!(
            suffixes,
            vec!["", "_ne", "_gt", "_gte", "_lt", "_lte", "_in", "_not_in"]
        );
    }

    #[test]
    fn operators_for_boolean_type() {
        let ops = operators_for_type("boolean");
        assert_eq!(ops.len(), 4);
    }

    #[test]
    fn factory_child_columns_meta() {
        let mut event = test_event();
        event.is_factory_child = true;

        let meta = build_columns_meta(&event);
        let names: Vec<&str> = meta.iter().map(|c| c.name.as_str()).collect();

        // contract_address appears after standard columns, before context columns
        assert_eq!(
            names,
            vec!["id", "block_number", "tx_hash", "tx_index", "log_index",
                 "contract_address", "block_timestamp", "tx_from", "from_address", "value"]
        );

        // contract_address is text, not nullable
        let ca = meta.iter().find(|c| c.name == "contract_address");
        assert!(ca.is_some());
        assert_eq!(ca.map(|c| c.pg_type.as_str()), Some("text"));
        assert!(!ca.is_none_or(|c| c.nullable));
    }

    fn test_transfer() -> ResolvedTransfer {
        ResolvedTransfer {
            name: "eth_transfers".to_string(),
            table_name: "eth_transfers".to_string(),
            context_fields: vec![],
            create_table_sql: String::new(),
            create_indexes_sql: vec![],
            insert_sql: String::new(),
            rollback_sql: String::new(),
            filter_from: vec![],
            filter_to: vec![],
        }
    }

    #[test]
    fn transfer_columns_meta_includes_correct_columns() {
        let transfer = test_transfer();
        let meta = build_transfer_columns_meta(&transfer);
        let names: Vec<&str> = meta.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["id", "block_number", "tx_hash", "tx_index",
                 "from_address", "to_address", "value"]
        );
        // No log_index for transfers
        assert!(meta.iter().all(|c| c.name != "log_index"));
    }

    fn test_call() -> ResolvedCall {
        ResolvedCall {
            function_name: "exactInputSingle".to_string(),
            contract_name: "UniswapV3Router".to_string(),
            table_name: "uniswap_swaps".to_string(),
            context_fields: vec![ContextField::BlockTimestamp],
            columns: vec![
                ResolvedColumn {
                    column_name: "recipient".to_string(),
                    param_name: "recipient".to_string(),
                    pg_type: "text".to_string(),
                },
                ResolvedColumn {
                    column_name: "amount_in".to_string(),
                    param_name: "amountIn".to_string(),
                    pg_type: "numeric".to_string(),
                },
            ],
            insert_sql: String::new(),
            create_table_sql: String::new(),
            create_indexes_sql: vec![],
            rollback_sql: String::new(),
            is_factory_child: false,
        }
    }

    #[test]
    fn call_columns_meta_includes_correct_columns() {
        let call = test_call();
        let meta = build_call_columns_meta(&call);
        let names: Vec<&str> = meta.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["id", "block_number", "tx_hash", "tx_index",
                 "block_timestamp", "recipient", "amount_in"]
        );
        // No log_index for calls
        assert!(meta.iter().all(|c| c.name != "log_index"));
    }

    #[test]
    fn call_columns_meta_factory_child() {
        let mut call = test_call();
        call.is_factory_child = true;
        let meta = build_call_columns_meta(&call);
        let names: Vec<&str> = meta.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["id", "block_number", "tx_hash", "tx_index",
                 "contract_address", "block_timestamp", "recipient", "amount_in"]
        );
    }

    #[test]
    fn transfer_columns_meta_with_context() {
        let mut transfer = test_transfer();
        transfer.context_fields = vec![ContextField::BlockTimestamp, ContextField::TxGasPrice];
        let meta = build_transfer_columns_meta(&transfer);
        let names: Vec<&str> = meta.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["id", "block_number", "tx_hash", "tx_index",
                 "from_address", "to_address", "value",
                 "block_timestamp", "tx_gas_price"]
        );
    }
}
