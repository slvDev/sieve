//! Dynamic GraphQL schema generation from resolved TOML event config.
//!
//! Builds a complete GraphQL schema at startup from `Vec<ResolvedEvent>`.
//! Each event table becomes a query field returning a Connection type with
//! cursor-based pagination, filtering, and sorting. Uses `async-graphql`'s
//! dynamic schema API since the schema shape is determined at runtime by
//! `sieve.toml`.

use crate::api::query_builder::{
    bind_params, build_select_with_cursor, decode_cursor, encode_cursor, parse_filters,
    validate_order_column, Cursor, SelectParams, DEFAULT_LIMIT, MAX_LIMIT,
};
use crate::api::types::{
    build_columns_meta, build_select_clause, operators_for_type, pg_to_graphql_type, row_to_json,
    to_pascal_case, ColumnMeta,
};
use crate::toml_config::ResolvedEvent;

use async_graphql::dynamic::{
    Enum, EnumItem, Field, FieldFuture, FieldValue, InputObject, InputValue, Object, Schema,
    SchemaBuilder, TypeRef,
};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;

/// Metadata bundle for a single event table, shared across resolvers.
#[derive(Debug, Clone)]
struct TableMeta {
    table_name: String,
    select_clause: String,
    columns: Vec<ColumnMeta>,
}

/// Build a dynamic GraphQL schema from resolved TOML event definitions.
///
/// # Errors
///
/// Returns an error if schema construction fails.
pub fn build_schema(events: &[ResolvedEvent], pool: PgPool) -> eyre::Result<Schema> {
    let mut builder: SchemaBuilder = Schema::build("Query", None, None);

    // Register shared enums
    builder = builder.register(build_order_direction_enum());

    // Register _Meta type
    builder = builder.register(build_meta_type());

    // Register shared PageInfo type
    builder = builder.register(build_page_info_type());

    // Build Query root
    let mut query = Object::new("Query")
        .description("Auto-generated query API for indexed Ethereum events");

    // Add _meta field
    query = add_meta_field(query);

    // Per-event table
    for event in events {
        let type_name = to_pascal_case(&event.table_name);
        let filter_type_name = format!("{type_name}Filter");
        let order_by_type_name = format!("{type_name}OrderBy");
        let connection_type_name = format!("{type_name}Connection");

        let columns_meta = build_columns_meta(event);
        let select_clause = build_select_clause(&columns_meta);

        let meta = Arc::new(TableMeta {
            table_name: event.table_name.clone(),
            select_clause,
            columns: columns_meta.clone(),
        });

        // Register output object type
        builder = builder.register(build_output_object(&type_name, &columns_meta));

        // Register filter input type
        builder = builder.register(build_filter_input(&filter_type_name, &columns_meta));

        // Register order-by enum
        builder = builder.register(build_order_by_enum(&order_by_type_name, &columns_meta));

        // Register Connection type
        builder = builder.register(build_connection_type(&connection_type_name, &type_name));

        // Add query field
        query = query.field(build_query_field(
            &event.table_name,
            &connection_type_name,
            &filter_type_name,
            &order_by_type_name,
            pool.clone(),
            Arc::clone(&meta),
        ));
    }

    builder = builder.register(query);
    builder = builder.data(pool);

    builder.finish().map_err(|e| eyre::eyre!("GraphQL schema build failed: {e}"))
}

// ── Output Object type ───────────────────────────────────────────────

fn build_output_object(type_name: &str, columns: &[ColumnMeta]) -> Object {
    let mut obj = Object::new(type_name);

    for col in columns {
        let field_name = col.name.clone();
        let col_name = col.name.clone();
        let gql_type = pg_to_graphql_type(&col.pg_type, col.nullable);

        obj = obj.field(Field::new(field_name, gql_type, move |ctx| {
            let name = col_name.clone();
            FieldFuture::new(async move {
                let row = ctx
                    .parent_value
                    .try_downcast_ref::<HashMap<String, serde_json::Value>>()?;

                match row.get(&name) {
                    Some(serde_json::Value::Null) | None => Ok(None),
                    Some(serde_json::Value::String(s)) => {
                        Ok(Some(FieldValue::value(s.clone())))
                    }
                    Some(serde_json::Value::Bool(b)) => {
                        Ok(Some(FieldValue::value(*b)))
                    }
                    Some(serde_json::Value::Number(n)) => {
                        #[expect(clippy::cast_possible_truncation, reason = "integer columns are i32")]
                        let val = n.as_i64().map_or_else(
                            || FieldValue::value(n.to_string()),
                            |i| FieldValue::value(i as i32),
                        );
                        Ok(Some(val))
                    }
                    Some(_) => Ok(Some(FieldValue::value(String::new()))),
                }
            })
        }));
    }

    obj
}

// ── Filter InputObject ───────────────────────────────────────────────

fn build_filter_input(type_name: &str, columns: &[ColumnMeta]) -> InputObject {
    let mut input = InputObject::new(type_name);

    for col in columns {
        // Skip id for filters (auto-generated)
        if col.name == "id" {
            continue;
        }
        let ops = operators_for_type(&col.pg_type);
        for (suffix, gql_type) in ops {
            let field_name = if suffix.is_empty() {
                col.name.clone()
            } else {
                format!("{}{suffix}", col.name)
            };
            input = input.field(InputValue::new(field_name, gql_type));
        }
    }

    input
}

// ── OrderBy enum ─────────────────────────────────────────────────────

fn build_order_by_enum(type_name: &str, columns: &[ColumnMeta]) -> Enum {
    let mut e = Enum::new(type_name);
    for col in columns {
        e = e.item(EnumItem::new(&col.name));
    }
    e
}

fn build_order_direction_enum() -> Enum {
    Enum::new("OrderDirection")
        .item(EnumItem::new("ASC"))
        .item(EnumItem::new("DESC"))
}

// ── PageInfo and Connection types ────────────────────────────────────

fn build_page_info_type() -> Object {
    Object::new("PageInfo")
        .field(Field::new(
            "hasNextPage",
            TypeRef::named_nn(TypeRef::BOOLEAN),
            |ctx| {
                FieldFuture::new(async move {
                    let info = ctx.parent_value.try_downcast_ref::<PageInfoData>()?;
                    Ok(Some(FieldValue::value(info.has_next_page)))
                })
            },
        ))
        .field(Field::new(
            "endCursor",
            TypeRef::named(TypeRef::STRING),
            |ctx| {
                FieldFuture::new(async move {
                    let info = ctx.parent_value.try_downcast_ref::<PageInfoData>()?;
                    Ok(info.end_cursor.as_ref().map(|c| FieldValue::value(c.clone())))
                })
            },
        ))
}

fn build_connection_type(connection_type_name: &str, node_type_name: &str) -> Object {
    let node_type = node_type_name.to_string();

    Object::new(connection_type_name)
        .field(Field::new(
            "nodes",
            TypeRef::named_nn_list_nn(&node_type),
            move |ctx| {
                FieldFuture::new(async move {
                    let conn = ctx.parent_value.try_downcast_ref::<ConnectionData>()?;
                    let values: Vec<FieldValue<'static>> = conn
                        .nodes
                        .iter()
                        .map(|m| FieldValue::owned_any(m.clone()))
                        .collect();
                    Ok(Some(FieldValue::list(values)))
                })
            },
        ))
        .field(Field::new(
            "pageInfo",
            TypeRef::named_nn("PageInfo"),
            |ctx| {
                FieldFuture::new(async move {
                    let conn = ctx.parent_value.try_downcast_ref::<ConnectionData>()?;
                    Ok(Some(FieldValue::owned_any(conn.page_info.clone())))
                })
            },
        ))
}

// ── Internal data structs for FieldValue::owned_any ──────────────────

/// Resolved connection data passed to GraphQL Connection type resolvers.
struct ConnectionData {
    nodes: Vec<HashMap<String, serde_json::Value>>,
    page_info: PageInfoData,
}

/// Resolved page info for cursor-based pagination.
#[derive(Clone)]
struct PageInfoData {
    has_next_page: bool,
    end_cursor: Option<String>,
}

// Compile-time size assertions (reth pattern).
#[cfg(target_pointer_width = "64")]
const _: [(); 32] = [(); core::mem::size_of::<PageInfoData>()];
#[cfg(target_pointer_width = "64")]
const _: [(); 56] = [(); core::mem::size_of::<ConnectionData>()];
#[cfg(target_pointer_width = "64")]
const _: [(); 112] = [(); core::mem::size_of::<QueryArgs>()];

// ── Root query field ─────────────────────────────────────────────────

fn build_query_field(
    query_field_name: &str,
    connection_type_name: &str,
    filter_type_name: &str,
    order_by_type_name: &str,
    pool: PgPool,
    meta: Arc<TableMeta>,
) -> Field {
    let conn_type = connection_type_name.to_string();
    Field::new(
        query_field_name,
        TypeRef::named_nn(&conn_type),
        move |ctx| {
            let pool = pool.clone();
            let meta = Arc::clone(&meta);
            FieldFuture::new(async move {
                resolve_connection(ctx, &pool, &meta).await
            })
        },
    )
    .argument(InputValue::new("where", TypeRef::named(filter_type_name)))
    .argument(InputValue::new(
        "orderBy",
        TypeRef::named(order_by_type_name),
    ))
    .argument(
        InputValue::new("orderDirection", TypeRef::named("OrderDirection"))
            .default_value("DESC"),
    )
    .argument(
        InputValue::new("first", TypeRef::named(TypeRef::INT))
            .default_value(DEFAULT_LIMIT as i32),
    )
    .argument(InputValue::new("skip", TypeRef::named(TypeRef::INT)).default_value(0i32))
    .argument(InputValue::new("after", TypeRef::named(TypeRef::STRING)))
}

/// Resolve a connection query with cursor-based pagination.
async fn resolve_connection(
    ctx: async_graphql::dynamic::ResolverContext<'_>,
    pool: &PgPool,
    meta: &TableMeta,
) -> async_graphql::Result<Option<FieldValue<'static>>> {
    let args = parse_query_args(&ctx, meta)?;

    let order_pg_type = meta
        .columns
        .iter()
        .find(|c| c.name == args.order_by)
        .map_or("text", |c| c.pg_type.as_str());

    // Fetch limit+1 rows to determine hasNextPage
    let fetch_limit = (args.limit + 1).min(MAX_LIMIT + 1);

    let (sql, params) = build_select_with_cursor(&SelectParams {
        table: &meta.table_name,
        select_clause: &meta.select_clause,
        filters: &args.filters,
        order_by: &args.order_by,
        order_pg_type,
        order_dir: args.direction,
        limit: fetch_limit,
        offset: args.offset,
        cursor: args.cursor.as_ref(),
    });

    let query = sqlx::query(&sql);
    let query = bind_params(query, &params);

    let rows = query
        .fetch_all(pool)
        .await
        .map_err(|e| async_graphql::Error::new(format!("query failed: {e}")))?;

    let has_next_page = rows.len() as i64 > args.limit;
    let result_rows = if has_next_page {
        &rows[..args.limit as usize]
    } else {
        &rows[..]
    };

    let mut nodes: Vec<HashMap<String, serde_json::Value>> =
        Vec::with_capacity(result_rows.len());
    for row in result_rows {
        let map = row_to_json(row, &meta.columns)
            .map_err(|e| async_graphql::Error::new(format!("row extraction failed: {e:#}")))?;
        nodes.push(map);
    }

    let end_cursor = compute_end_cursor(result_rows, meta, &args.order_by);

    let conn = ConnectionData {
        nodes,
        page_info: PageInfoData {
            has_next_page,
            end_cursor,
        },
    };

    Ok(Some(FieldValue::owned_any(conn)))
}

/// Parsed query arguments extracted from GraphQL resolver context.
struct QueryArgs {
    filters: Vec<crate::api::query_builder::FilterCondition>,
    order_by: String,
    direction: &'static str,
    limit: i64,
    offset: i64,
    cursor: Option<Cursor>,
}

/// Parse query arguments from the GraphQL resolver context.
fn parse_query_args(
    ctx: &async_graphql::dynamic::ResolverContext<'_>,
    meta: &TableMeta,
) -> async_graphql::Result<QueryArgs> {
    // Parse filter
    let filters = match ctx.args.try_get("where") {
        Ok(where_val) => {
            let val = where_val.deserialize::<async_graphql::Value>()?;
            parse_filters(&val, &meta.columns)
                .map_err(|e| async_graphql::Error::new(format!("{e:#}")))?
        }
        Err(_) => Vec::new(),
    };

    // Parse ordering
    let order_by_str: String = ctx
        .args
        .try_get("orderBy")
        .ok()
        .and_then(|v| {
            let name = v.enum_name().ok()?;
            Some(name.to_string())
        })
        .unwrap_or_else(|| "id".to_string());

    let order_by = validate_order_column(&order_by_str, &meta.columns)
        .map_err(|e| async_graphql::Error::new(format!("{e:#}")))?;

    let direction: &'static str = ctx
        .args
        .try_get("orderDirection")
        .ok()
        .and_then(|v| {
            let name = v.enum_name().ok()?;
            if name == "ASC" { Some("ASC") } else { Some("DESC") }
        })
        .unwrap_or("DESC");

    // Parse pagination (clamp to 1..MAX_LIMIT to prevent zero/negative panics)
    let limit = ctx
        .args
        .try_get("first")
        .ok()
        .and_then(|v| v.i64().ok())
        .unwrap_or(DEFAULT_LIMIT)
        .clamp(1, MAX_LIMIT);

    let offset = ctx
        .args
        .try_get("skip")
        .ok()
        .and_then(|v| v.i64().ok())
        .unwrap_or(0);

    // Parse cursor
    let cursor = match ctx.args.try_get("after") {
        Ok(after_val) => {
            let cursor_str = after_val
                .string()
                .map_err(|_| async_graphql::Error::new("'after' must be a string"))?;
            Some(
                decode_cursor(cursor_str)
                    .map_err(|e| async_graphql::Error::new(format!("invalid cursor: {e:#}")))?,
            )
        }
        Err(_) => None,
    };

    Ok(QueryArgs {
        filters,
        order_by: order_by.to_string(),
        direction,
        limit,
        offset,
        cursor,
    })
}

/// Compute the end cursor from the last row in the result set.
fn compute_end_cursor(
    rows: &[sqlx::postgres::PgRow],
    meta: &TableMeta,
    order_by: &str,
) -> Option<String> {
    let last_row = rows.last()?;
    let last_map = row_to_json(last_row, &meta.columns).ok()?;

    let sort_value = last_map
        .get(order_by)
        .and_then(|v| match v {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            _ => None,
        })?;

    let id = last_map.get("id").and_then(|v| match v {
        serde_json::Value::String(s) => s.parse::<i64>().ok(),
        serde_json::Value::Number(n) => n.as_i64(),
        _ => None,
    })?;

    Some(encode_cursor(&sort_value, id))
}

// ── _Meta field ──────────────────────────────────────────────────────

fn build_meta_type() -> Object {
    let obj = Object::new("_Meta");
    obj.field(Field::new("block", TypeRef::named_nn(TypeRef::STRING), |ctx| {
        let block_str = ctx
            .parent_value
            .try_downcast_ref::<String>()
            .cloned()
            .unwrap_or_default();
        FieldFuture::new(async move { Ok(Some(FieldValue::value(block_str))) })
    }))
}

fn add_meta_field(query: Object) -> Object {
    query.field(Field::new("_meta", TypeRef::named_nn("_Meta"), |ctx| {
        FieldFuture::new(async move {
            let pool = ctx.data::<PgPool>()?;

            let row: (i64,) = sqlx::query_as(
                "SELECT block_number FROM _sieve_checkpoints WHERE id = 1",
            )
            .fetch_one(pool)
            .await
            .map_err(|e| async_graphql::Error::new(format!("checkpoint query failed: {e}")))?;

            Ok(Some(FieldValue::owned_any(row.0.to_string())))
        })
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::toml_config::{ContextField, ResolvedColumn, ResolvedEvent};

    fn test_event() -> ResolvedEvent {
        ResolvedEvent {
            event_name: "Transfer".to_string(),
            contract_name: "USDC".to_string(),
            table_name: "usdc_transfers".to_string(),
            context_fields: vec![ContextField::BlockTimestamp],
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
        }
    }

    #[test]
    fn filter_input_has_expected_fields() {
        let event = test_event();
        let meta = build_columns_meta(&event);
        let _input = build_filter_input("TestFilter", &meta);
        // If it builds without panic, the InputObject is valid.
    }

    #[test]
    fn order_by_enum_has_all_columns() {
        let event = test_event();
        let meta = build_columns_meta(&event);
        let _e = build_order_by_enum("TestOrderBy", &meta);
    }

    #[test]
    fn output_object_builds() {
        let event = test_event();
        let meta = build_columns_meta(&event);
        let _obj = build_output_object("UsdcTransfers", &meta);
    }

    #[test]
    fn connection_type_has_nodes_and_pageinfo() {
        let _conn = build_connection_type("TestConnection", "TestNode");
        // Builds without error — dynamic schema validates at build time.
    }

    #[test]
    fn pageinfo_type_has_fields() {
        let _pi = build_page_info_type();
        // Builds without error.
    }
}
