//! GraphQL API server for querying indexed Ethereum events.
//!
//! Auto-generates a GraphQL schema from `sieve.toml` event definitions.
//! Activated via `--api-port <PORT>` CLI flag. Runs in parallel with
//! the indexer, sharing the same PostgreSQL connection pool.

pub mod query_builder;
pub mod schema;
pub mod types;

pub use schema::build_schema;

use async_graphql::dynamic::Schema;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::State;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Router;
use tokio::sync::watch;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

/// Shared state for the API server.
#[derive(Clone)]
struct ApiState {
    schema: Schema,
}

/// Start the GraphQL API server.
///
/// Serves GraphiQL at `GET /`, GraphQL at `POST /`, and a health check at
/// `GET /health`. Runs until `stop_rx` signals shutdown.
///
/// # Errors
///
/// Returns an error if the server fails to bind to the port.
pub async fn run_api_server(
    port: u16,
    schema: Schema,
    mut stop_rx: watch::Receiver<bool>,
) -> eyre::Result<()> {
    let state = ApiState { schema };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/", get(graphiql_handler).post(graphql_handler))
        .route("/health", get(health_handler))
        .layer(cors)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .map_err(|e| eyre::eyre!("failed to bind API server to port {port}: {e}"))?;

    info!(port, "GraphQL API server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = stop_rx.changed().await;
        })
        .await
        .map_err(|e| eyre::eyre!("API server error: {e}"))
}

async fn graphql_handler(
    State(state): State<ApiState>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    state.schema.execute(req.into_inner()).await.into()
}

async fn graphiql_handler() -> impl IntoResponse {
    Html(
        async_graphql::http::GraphiQLSource::build()
            .endpoint("/")
            .finish(),
    )
}

async fn health_handler() -> &'static str {
    "ok"
}
