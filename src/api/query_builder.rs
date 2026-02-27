//! Parameterized SQL SELECT query builder for GraphQL resolvers.
//!
//! Builds `SELECT ... FROM table WHERE ... ORDER BY ... LIMIT ... OFFSET ...`
//! queries from GraphQL filter/sort/pagination arguments. All user values are
//! bound as `$N` parameters — table and column names come from validated
//! `ResolvedEvent` metadata and are safe to interpolate.

use crate::api::types::ColumnMeta;

use base64::Engine;
use std::fmt::Write as _;

// ── Filter types ─────────────────────────────────────────────────────

/// A filter operator parsed from a GraphQL input field name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FilterOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
    StartsWith,
}

/// A typed SQL parameter value.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SqlParam {
    Text(String),
    Int64(i64),
    Int32(i32),
    Bool(bool),
}

/// A parsed filter condition ready for SQL generation.
#[derive(Debug, Clone)]
pub struct FilterCondition {
    pub column: String,
    pub pg_type: String,
    pub op: FilterOp,
    pub value: SqlParam,
}

// ── Filter key parsing ───────────────────────────────────────────────

/// Known operator suffixes, ordered longest-first to avoid ambiguity.
const SUFFIXES: &[(&str, FilterOp)] = &[
    ("_starts_with", FilterOp::StartsWith),
    ("_contains", FilterOp::Contains),
    ("_gte", FilterOp::Gte),
    ("_lte", FilterOp::Lte),
    ("_gt", FilterOp::Gt),
    ("_lt", FilterOp::Lt),
    ("_ne", FilterOp::Ne),
];

/// Parse a filter key like `"block_number_gte"` into `("block_number", Gte)`.
///
/// Tries longest suffix first, validates that the base name is a known column.
/// If no suffix matches, treats the key as an equality filter.
///
/// # Errors
///
/// Returns an error if the key does not match any known column.
pub fn parse_filter_key<'a>(
    key: &'a str,
    columns: &[ColumnMeta],
) -> eyre::Result<(&'a str, FilterOp)> {
    // Try suffixes from longest to shortest
    for (suffix, op) in SUFFIXES {
        if let Some(col_name) = key.strip_suffix(suffix) {
            if columns.iter().any(|c| c.name == col_name) {
                return Ok((col_name, *op));
            }
        }
    }

    // No suffix → equality
    if columns.iter().any(|c| c.name == key) {
        return Ok((key, FilterOp::Eq));
    }

    Err(eyre::eyre!("unknown filter field: '{key}'"))
}

/// Look up the PG type for a column name.
fn column_pg_type<'a>(col_name: &str, columns: &'a [ColumnMeta]) -> &'a str {
    columns
        .iter()
        .find(|c| c.name == col_name)
        .map_or("text", |c| c.pg_type.as_str())
}

// ── SQL generation ───────────────────────────────────────────────────

/// Maximum number of rows a query can return.
pub const MAX_LIMIT: i64 = 1000;

/// Default number of rows returned.
pub const DEFAULT_LIMIT: i64 = 100;

/// Build a complete SELECT query from components.
///
/// `select_clause` is the pre-built column list with casts (from
/// `build_select_clause`). Returns the SQL string and ordered parameter list.
#[must_use]
pub fn build_select(
    table: &str,
    select_clause: &str,
    filters: &[FilterCondition],
    order_by: &str,
    order_dir: &str,
    limit: i64,
    offset: i64,
) -> (String, Vec<SqlParam>) {
    let mut sql = format!("SELECT {select_clause} FROM {table}");
    let mut params: Vec<SqlParam> = Vec::new();
    let mut param_idx = 1usize;

    if !filters.is_empty() {
        let mut conditions = Vec::with_capacity(filters.len());
        for f in filters {
            let condition = build_condition(&f.column, &f.pg_type, f.op, &mut param_idx);
            conditions.push(condition);
            params.push(f.value.clone());
        }
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }

    let _ = write!(sql, " ORDER BY {order_by} {order_dir}");

    let clamped_limit = limit.clamp(1, MAX_LIMIT);
    let clamped_offset = offset.max(0);

    let _ = write!(sql, " LIMIT ${param_idx}");
    params.push(SqlParam::Int64(clamped_limit));
    param_idx += 1;

    let _ = write!(sql, " OFFSET ${param_idx}");
    params.push(SqlParam::Int64(clamped_offset));

    (sql, params)
}

/// Build a single WHERE condition string for a filter.
fn build_condition(column: &str, pg_type: &str, op: FilterOp, param_idx: &mut usize) -> String {
    let idx = *param_idx;
    *param_idx += 1;

    // For bytea columns, decode hex input
    let is_bytea = pg_type == "bytea";
    // For numeric comparisons, cast both sides
    let is_numeric = matches!(pg_type, "bigint" | "numeric" | "bigserial");

    match op {
        FilterOp::Eq => {
            if is_bytea {
                format!("{column} = decode(${idx}, 'hex')")
            } else {
                format!("{column} = ${idx}")
            }
        }
        FilterOp::Ne => {
            if is_bytea {
                format!("{column} != decode(${idx}, 'hex')")
            } else {
                format!("{column} != ${idx}")
            }
        }
        FilterOp::Gt => {
            if is_numeric {
                format!("{column} > ${idx}::{pg_type}")
            } else {
                format!("{column} > ${idx}")
            }
        }
        FilterOp::Gte => {
            if is_numeric {
                format!("{column} >= ${idx}::{pg_type}")
            } else {
                format!("{column} >= ${idx}")
            }
        }
        FilterOp::Lt => {
            if is_numeric {
                format!("{column} < ${idx}::{pg_type}")
            } else {
                format!("{column} < ${idx}")
            }
        }
        FilterOp::Lte => {
            if is_numeric {
                format!("{column} <= ${idx}::{pg_type}")
            } else {
                format!("{column} <= ${idx}")
            }
        }
        FilterOp::Contains | FilterOp::StartsWith => format!("{column} LIKE ${idx}"),
    }
}

/// Parse a GraphQL filter value into a `SqlParam`.
///
/// For `Contains`, wraps the value in `%..%`. For `StartsWith`, appends `%`.
/// For `bytea` columns, strips the `0x` prefix.
///
/// # Errors
///
/// Returns an error if the value cannot be extracted.
pub fn parse_filter_value(
    value: &async_graphql::Value,
    pg_type: &str,
    op: FilterOp,
) -> eyre::Result<SqlParam> {
    match pg_type {
        "boolean" => {
            if let async_graphql::Value::Boolean(b) = value {
                Ok(SqlParam::Bool(*b))
            } else {
                Err(eyre::eyre!("expected boolean value"))
            }
        }
        "integer" => {
            let s = value_as_string(value)?;
            let n: i32 = s.parse().map_err(|_| eyre::eyre!("invalid integer: '{s}'"))?;
            Ok(SqlParam::Int32(n))
        }
        "bytea" => {
            let s = value_as_string(value)?;
            // Strip 0x prefix for decode()
            let hex = s.strip_prefix("0x").unwrap_or(&s);
            Ok(SqlParam::Text(hex.to_string()))
        }
        _ => {
            // text, bigint, numeric, bigserial
            let s = value_as_string(value)?;
            match op {
                FilterOp::Contains => Ok(SqlParam::Text(format!("%{s}%"))),
                FilterOp::StartsWith => Ok(SqlParam::Text(format!("{s}%"))),
                _ => Ok(SqlParam::Text(s)),
            }
        }
    }
}

/// Extract a string from a GraphQL `Value`.
fn value_as_string(value: &async_graphql::Value) -> eyre::Result<String> {
    match value {
        async_graphql::Value::String(s) => Ok(s.clone()),
        async_graphql::Value::Number(n) => Ok(n.to_string()),
        async_graphql::Value::Boolean(b) => Ok(b.to_string()),
        _ => Err(eyre::eyre!("expected string or number value")),
    }
}

/// Parse all filter fields from a GraphQL input object into `FilterCondition`s.
///
/// # Errors
///
/// Returns an error if any filter key is unknown or value is invalid.
pub fn parse_filters(
    filter_obj: &async_graphql::Value,
    columns: &[ColumnMeta],
) -> eyre::Result<Vec<FilterCondition>> {
    let async_graphql::Value::Object(obj) = filter_obj else {
        return Err(eyre::eyre!("filter must be an object"));
    };

    let mut conditions = Vec::new();
    for (key, value) in obj {
        if matches!(value, async_graphql::Value::Null) {
            continue;
        }
        let key_str = key.as_str();
        let (col_name, op) = parse_filter_key(key_str, columns)?;
        let pg_type = column_pg_type(col_name, columns);
        let param = parse_filter_value(value, pg_type, op)?;
        conditions.push(FilterCondition {
            column: col_name.to_string(),
            pg_type: pg_type.to_string(),
            op,
            value: param,
        });
    }
    Ok(conditions)
}

/// Validate that an `orderBy` value is a known column name.
///
/// # Errors
///
/// Returns an error if the column name is not found.
pub fn validate_order_column<'a>(
    order_by: &'a str,
    columns: &[ColumnMeta],
) -> eyre::Result<&'a str> {
    if columns.iter().any(|c| c.name == order_by) {
        Ok(order_by)
    } else {
        Err(eyre::eyre!("unknown orderBy column: '{order_by}'"))
    }
}

// ── Cursor pagination ────────────────────────────────────────────────

/// A decoded cursor for keyset pagination.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Cursor {
    /// Value of the sort column at the cursor position.
    pub sort_value: String,
    /// Primary key (`id`) at the cursor position.
    pub id: i64,
}

/// Encode a cursor as a base64 JSON string.
#[must_use]
pub fn encode_cursor(sort_value: &str, id: i64) -> String {
    let json = serde_json::json!({"v": sort_value, "id": id});
    base64::engine::general_purpose::STANDARD.encode(json.to_string().as_bytes())
}

/// Decode a base64 JSON cursor string.
///
/// # Errors
///
/// Returns an error if the cursor is invalid base64 or JSON.
pub fn decode_cursor(cursor: &str) -> eyre::Result<Cursor> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(cursor)
        .map_err(|e| eyre::eyre!("invalid cursor encoding: {e}"))?;
    let s = String::from_utf8(bytes)
        .map_err(|e| eyre::eyre!("cursor is not valid UTF-8: {e}"))?;
    let parsed: serde_json::Value =
        serde_json::from_str(&s).map_err(|e| eyre::eyre!("invalid cursor JSON: {e}"))?;

    let sort_value = parsed
        .get("v")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| eyre::eyre!("cursor missing 'v' field"))?
        .to_string();
    let id = parsed
        .get("id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| eyre::eyre!("cursor missing 'id' field"))?;

    Ok(Cursor { sort_value, id })
}

/// Build a cursor-based WHERE condition for keyset pagination.
///
/// For DESC order: `(order_col, id) < ($sort_val, $cursor_id)`
/// For ASC order:  `(order_col, id) > ($sort_val, $cursor_id)`
///
/// Returns the SQL fragment and parameter values to append.
#[must_use]
pub fn build_cursor_condition(
    order_col: &str,
    order_pg_type: &str,
    direction: &str,
    cursor: &Cursor,
    param_idx: &mut usize,
) -> (String, Vec<SqlParam>) {
    let cmp = if direction == "ASC" { ">" } else { "<" };
    let val_idx = *param_idx;
    *param_idx += 1;
    let id_idx = *param_idx;
    *param_idx += 1;

    let is_numeric = matches!(order_pg_type, "bigint" | "numeric" | "bigserial");
    let cast = if is_numeric {
        // bigserial is a DDL pseudo-type; runtime cast type is bigint
        let cast_type = if order_pg_type == "bigserial" { "bigint" } else { order_pg_type };
        format!("::{cast_type}")
    } else {
        String::new()
    };

    let sql = format!(
        "({order_col}, id) {cmp} (${val_idx}{cast}, ${id_idx})"
    );

    let params = vec![
        SqlParam::Text(cursor.sort_value.clone()),
        SqlParam::Int64(cursor.id),
    ];

    (sql, params)
}

/// Parameters for building a SELECT query with cursor support.
#[non_exhaustive]
pub struct SelectParams<'a> {
    pub table: &'a str,
    pub select_clause: &'a str,
    pub filters: &'a [FilterCondition],
    pub order_by: &'a str,
    pub order_pg_type: &'a str,
    pub order_dir: &'a str,
    pub limit: i64,
    pub offset: i64,
    pub cursor: Option<&'a Cursor>,
}

/// Build a SELECT query with optional cursor-based pagination.
///
/// When `cursor` is `Some`, uses keyset pagination (ignoring `offset`).
/// When `None`, falls back to LIMIT/OFFSET.
#[must_use]
pub fn build_select_with_cursor(p: &SelectParams<'_>) -> (String, Vec<SqlParam>) {
    let mut sql = format!("SELECT {} FROM {}", p.select_clause, p.table);
    let mut params: Vec<SqlParam> = Vec::new();
    let mut param_idx = 1usize;

    // Collect all WHERE conditions
    let mut conditions = Vec::new();

    for f in p.filters {
        let condition = build_condition(&f.column, &f.pg_type, f.op, &mut param_idx);
        conditions.push(condition);
        params.push(f.value.clone());
    }

    if let Some(c) = p.cursor {
        let (cursor_cond, cursor_params) =
            build_cursor_condition(p.order_by, p.order_pg_type, p.order_dir, c, &mut param_idx);
        conditions.push(cursor_cond);
        params.extend(cursor_params);
    }

    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }

    let _ = write!(sql, " ORDER BY {} {}, id {}", p.order_by, p.order_dir, p.order_dir);

    let clamped_limit = p.limit.clamp(1, MAX_LIMIT);

    let _ = write!(sql, " LIMIT ${param_idx}");
    params.push(SqlParam::Int64(clamped_limit));
    param_idx += 1;

    // Only add OFFSET when not using cursor pagination
    if p.cursor.is_none() {
        let clamped_offset = p.offset.max(0);
        let _ = write!(sql, " OFFSET ${param_idx}");
        params.push(SqlParam::Int64(clamped_offset));
    }

    (sql, params)
}

// Compile-time size assertions (reth pattern).
#[cfg(target_pointer_width = "64")]
const _: [(); 32] = [(); core::mem::size_of::<Cursor>()];
#[cfg(target_pointer_width = "64")]
const _: [(); 120] = [(); core::mem::size_of::<SelectParams<'_>>()];

/// Bind `SqlParam` values to a sqlx query.
pub fn bind_params<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    params: &'q [SqlParam],
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    for param in params {
        query = match param {
            SqlParam::Text(s) => query.bind(s.as_str()),
            SqlParam::Int64(n) => query.bind(*n),
            SqlParam::Int32(n) => query.bind(*n),
            SqlParam::Bool(b) => query.bind(*b),
        };
    }
    query
}

#[cfg(test)]
#[expect(clippy::panic_in_result_fn, reason = "assertions in tests are idiomatic")]
mod tests {
    use super::*;
    use crate::test_utils::test_columns;

    #[test]
    fn parse_key_equality() -> eyre::Result<()> {
        let cols = test_columns();
        let (col, op) = parse_filter_key("block_number", &cols)?;
        assert_eq!(col, "block_number");
        assert_eq!(op, FilterOp::Eq);
        Ok(())
    }

    #[test]
    fn parse_key_with_suffix() -> eyre::Result<()> {
        let cols = test_columns();
        let (col, op) = parse_filter_key("block_number_gte", &cols)?;
        assert_eq!(col, "block_number");
        assert_eq!(op, FilterOp::Gte);
        Ok(())
    }

    #[test]
    fn parse_key_text_contains() -> eyre::Result<()> {
        let cols = test_columns();
        let (col, op) = parse_filter_key("from_address_contains", &cols)?;
        assert_eq!(col, "from_address");
        assert_eq!(op, FilterOp::Contains);
        Ok(())
    }

    #[test]
    fn parse_key_unknown_column_errors() {
        let cols = test_columns();
        let result = parse_filter_key("nonexistent_gte", &cols);
        assert!(result.is_err());
    }

    #[test]
    fn build_select_no_filters() {
        let (sql, params) = build_select(
            "usdc_transfers",
            "id, block_number",
            &[],
            "id",
            "DESC",
            100,
            0,
        );
        assert_eq!(
            sql,
            "SELECT id, block_number FROM usdc_transfers ORDER BY id DESC LIMIT $1 OFFSET $2"
        );
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn build_select_with_filter() {
        let filters = vec![FilterCondition {
            column: "block_number".into(),
            pg_type: "bigint".into(),
            op: FilterOp::Gte,
            value: SqlParam::Text("21000000".into()),
        }];
        let (sql, params) = build_select(
            "usdc_transfers",
            "*",
            &filters,
            "id",
            "DESC",
            100,
            0,
        );
        assert_eq!(
            sql,
            "SELECT * FROM usdc_transfers WHERE block_number >= $1::bigint ORDER BY id DESC LIMIT $2 OFFSET $3"
        );
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn build_select_bytea_filter() {
        let filters = vec![FilterCondition {
            column: "tx_hash".into(),
            pg_type: "bytea".into(),
            op: FilterOp::Eq,
            value: SqlParam::Text("abcd1234".into()),
        }];
        let (sql, _) = build_select("t", "*", &filters, "id", "DESC", 10, 0);
        assert!(sql.contains("tx_hash = decode($1, 'hex')"));
    }

    #[test]
    fn build_select_clamps_limit() {
        let (sql, params) = build_select("t", "*", &[], "id", "DESC", 9999, 0);
        // Should clamp to MAX_LIMIT (1000)
        assert!(sql.contains("LIMIT $1"));
        if let SqlParam::Int64(limit) = &params[0] {
            assert_eq!(*limit, 1000);
        }
    }

    #[test]
    fn build_select_multiple_filters() {
        let filters = vec![
            FilterCondition {
                column: "block_number".into(),
                pg_type: "bigint".into(),
                op: FilterOp::Gte,
                value: SqlParam::Text("100".into()),
            },
            FilterCondition {
                column: "from_address".into(),
                pg_type: "text".into(),
                op: FilterOp::Eq,
                value: SqlParam::Text("0xABC".into()),
            },
        ];
        let (sql, params) = build_select("t", "*", &filters, "id", "ASC", 50, 10);
        assert_eq!(
            sql,
            "SELECT * FROM t WHERE block_number >= $1::bigint AND from_address = $2 ORDER BY id ASC LIMIT $3 OFFSET $4"
        );
        assert_eq!(params.len(), 4);
    }

    #[test]
    fn parse_filter_value_contains_wraps_wildcards() -> eyre::Result<()> {
        let val = async_graphql::Value::String("abc".to_string());
        let param = parse_filter_value(&val, "text", FilterOp::Contains)?;
        let SqlParam::Text(s) = param else {
            return Err(eyre::eyre!("expected Text"));
        };
        assert_eq!(s, "%abc%");
        Ok(())
    }

    #[test]
    fn parse_filter_value_bytea_strips_prefix() -> eyre::Result<()> {
        let val = async_graphql::Value::String("0xabcd".to_string());
        let param = parse_filter_value(&val, "bytea", FilterOp::Eq)?;
        let SqlParam::Text(s) = param else {
            return Err(eyre::eyre!("expected Text"));
        };
        assert_eq!(s, "abcd");
        Ok(())
    }

    // ── Cursor tests ─────────────────────────────────────────────────

    #[test]
    fn encode_decode_roundtrip() -> eyre::Result<()> {
        let encoded = encode_cursor("21000042", 99);
        let decoded = decode_cursor(&encoded)?;
        assert_eq!(decoded.sort_value, "21000042");
        assert_eq!(decoded.id, 99);
        Ok(())
    }

    #[test]
    fn cursor_sql_desc() {
        let cursor = Cursor {
            sort_value: "21000000".into(),
            id: 50,
        };
        let mut idx = 1;
        let (sql, params) =
            build_cursor_condition("block_number", "bigint", "DESC", &cursor, &mut idx);
        assert_eq!(sql, "(block_number, id) < ($1::bigint, $2)");
        assert_eq!(params.len(), 2);
        assert_eq!(idx, 3);
    }

    #[test]
    fn cursor_sql_asc() {
        let cursor = Cursor {
            sort_value: "100".into(),
            id: 10,
        };
        let mut idx = 1;
        let (sql, _) = build_cursor_condition("id", "bigserial", "ASC", &cursor, &mut idx);
        // bigserial maps to bigint at runtime (bigserial is a DDL pseudo-type)
        assert_eq!(sql, "(id, id) > ($1::bigint, $2)");
    }

    #[test]
    fn decode_invalid_errors() {
        assert!(decode_cursor("not-base64!!!").is_err());
        // Valid base64 but invalid JSON
        let bad = base64::engine::general_purpose::STANDARD.encode(b"not json");
        assert!(decode_cursor(&bad).is_err());
    }

    #[test]
    fn build_select_with_cursor_desc() {
        let cursor = Cursor {
            sort_value: "21000000".into(),
            id: 50,
        };
        let (sql, params) = build_select_with_cursor(&SelectParams {
            table: "t",
            select_clause: "*",
            filters: &[],
            order_by: "block_number",
            order_pg_type: "bigint",
            order_dir: "DESC",
            limit: 10,
            offset: 0,
            cursor: Some(&cursor),
        });
        assert!(sql.contains("(block_number, id) < ($1::bigint, $2)"));
        assert!(sql.contains("ORDER BY block_number DESC, id DESC"));
        assert!(sql.contains("LIMIT $3"));
        // No OFFSET when cursor is present
        assert!(!sql.contains("OFFSET"));
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn build_select_with_cursor_none_has_offset() {
        let (sql, params) = build_select_with_cursor(&SelectParams {
            table: "t",
            select_clause: "*",
            filters: &[],
            order_by: "id",
            order_pg_type: "bigserial",
            order_dir: "DESC",
            limit: 10,
            offset: 5,
            cursor: None,
        });
        assert!(sql.contains("OFFSET $2"));
        assert_eq!(params.len(), 2);
    }
}
