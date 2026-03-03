//! Sieve — Ethereum event indexer over P2P.
//!
//! Entry point: loads TOML config, connects to PostgreSQL, optionally spawns
//! the GraphQL API server, then runs the P2P sync engine. Supports historical
//! backfill (`--end-block`) and live head-following modes. Graceful shutdown
//! on first Ctrl+C, hard exit on second.

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod api;
mod cli;
mod config;
mod db;
mod decode;
mod filter;
mod handler;
mod metrics;
mod p2p;
mod stream;
mod sync;
mod toml_config;
mod types;
#[cfg(test)]
mod test_utils;

use types::BlockNumber;

use clap::Parser;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = cli::Cli::parse();
    let startup = load_toml_config(&cli)?;

    // Validate --end-block if provided
    if let Some(end_block) = cli.end_block {
        if end_block < startup.start_block.as_u64() {
            return Err(eyre::eyre!(
                "--end-block ({end_block}) must be >= start_block ({})",
                startup.start_block
            ));
        }
    }

    info!(
        start_block = startup.start_block.as_u64(),
        end_block = cli.end_block,
        mode = if cli.end_block.is_some() { "historical" } else { "follow" },
        "sieve starting"
    );

    // Graceful shutdown signal
    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(shutdown_handler(stop_tx));

    let db = Arc::new(setup_database(&cli, &startup).await?);

    let index_config = Arc::new(startup.index_config);
    info!(contracts = index_config.contracts.len(), "loaded index config");

    // Load persisted factory children from previous runs
    if !startup.factories.is_empty() {
        db::load_factory_children(&db, &index_config).await?;
    }
    let factories = Arc::new(startup.factories);

    // Build handlers from resolved events
    let handlers: Vec<Box<dyn handler::EventHandler>> = startup
        .resolved_events
        .iter()
        .map(|re| -> Box<dyn handler::EventHandler> {
            Box::new(handler::ConfigDrivenHandler::new(re.clone()))
        })
        .collect();
    let handlers = Arc::new(handler::HandlerRegistry::new(handlers));
    info!(handlers = handlers.len(), "registered event handlers");

    // Metrics
    let metrics = Arc::new(metrics::SieveMetrics::new());

    // GraphQL API server (optional — must be built before transfers are consumed)
    if let Some(api_port) = cli.api_port {
        let schema = api::build_schema(
            &startup.resolved_events,
            &startup.resolved_transfers,
            &startup.resolved_calls,
            db.pool().clone(),
        )?;
        let api_stop = stop_rx.clone();
        let api_metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            if let Err(e) = api::run_api_server(api_port, schema, api_metrics, api_stop).await {
                tracing::error!(error = %e, "API server error");
            }
        });
    }

    // Build event_table_map: "contract:event" → (table_name, event_name)
    let event_table_map = build_event_table_map(&startup.resolved_events);

    // Build receipt_tables before transfers/calls are consumed
    let receipt_tables = Arc::new(build_receipt_tables(
        &startup.resolved_events,
        &startup.resolved_transfers,
        &startup.resolved_calls,
    ));

    // Build transfer handlers from resolved transfers
    let transfer_handlers: Vec<handler::TransferHandler> = startup
        .resolved_transfers
        .into_iter()
        .map(handler::TransferHandler::new)
        .collect();
    let transfer_handlers = Arc::new(handler::TransferRegistry::new(transfer_handlers));

    // Build call handlers from resolved calls
    let call_handlers: Vec<handler::CallHandler> = startup
        .resolved_calls
        .into_iter()
        .map(handler::CallHandler::new)
        .collect();
    let call_handlers = Arc::new(handler::CallRegistry::new(call_handlers));

    // Stream dispatcher (webhooks)
    let stream_dispatcher = if startup.resolved_streams.is_empty() {
        None
    } else {
        let sinks = build_stream_sinks(&startup.resolved_streams);
        info!(streams = sinks.len(), "configured stream sinks");
        Some(Arc::new(stream::StreamDispatcher::new(sinks, 256)))
    };

    // P2P
    let session = p2p::connect_mainnet_peers().await?;
    info!(peers = session.pool.len(), "connected to ethereum p2p network");

    let is_backfill = cli.end_block.is_some();

    let ctx = sync::SyncContext {
        pool: Arc::clone(&session.pool),
        config: index_config,
        db,
        handlers,
        metrics,
        stop_rx,
        factories,
        transfer_handlers,
        call_handlers,
        stream_dispatcher,
        event_table_map: Arc::new(event_table_map),
        is_backfill,
        receipt_tables,
    };

    run_indexer(&cli, startup.start_block, ctx).await
}

/// Resolved startup parameters from TOML config + CLI.
#[derive(Debug)]
struct StartupConfig {
    database_url: String,
    index_config: config::IndexConfig,
    resolved_events: Vec<toml_config::ResolvedEvent>,
    factories: Vec<toml_config::ResolvedFactory>,
    resolved_transfers: Vec<toml_config::ResolvedTransfer>,
    resolved_calls: Vec<toml_config::ResolvedCall>,
    resolved_streams: Vec<toml_config::ResolvedStream>,
    start_block: BlockNumber,
}

/// Load TOML config, resolve ABI files, and compute startup parameters.
///
/// # Errors
///
/// Returns an error if the config file cannot be read, parsed, or resolved.
fn load_toml_config(cli: &cli::Cli) -> eyre::Result<StartupConfig> {
    let config_path = Path::new(&cli.config);
    let sieve_config = toml_config::load_config(config_path)?;
    let config_dir = config_path.parent().unwrap_or_else(|| Path::new("."));

    // Resolve database URL: CLI > env > TOML
    let database_url = cli
        .database_url
        .clone()
        .or_else(|| {
            sieve_config
                .database
                .as_ref()
                .and_then(|d| d.url.clone())
        })
        .ok_or_else(|| {
            eyre::eyre!(
                "no database URL provided. Use --database-url, DATABASE_URL env var, or [database].url in config"
            )
        })?;

    let resolved = toml_config::resolve_config(&sieve_config, config_dir)?;

    // Compute effective start_block: CLI override or minimum across contracts, factories, and transfers
    let start_block = BlockNumber::new(cli.start_block.unwrap_or_else(|| {
        let contract_min = sieve_config
            .contracts
            .iter()
            .filter_map(|c| c.start_block)
            .min()
            .unwrap_or(u64::MAX);
        let factory_min = resolved
            .factories
            .iter()
            .map(|f| f.start_block)
            .min()
            .unwrap_or(u64::MAX);
        let transfer_min = sieve_config
            .transfers
            .iter()
            .filter_map(|t| t.start_block)
            .min()
            .unwrap_or(u64::MAX);
        contract_min.min(factory_min).min(transfer_min)
    }));

    Ok(StartupConfig {
        database_url,
        index_config: resolved.index_config,
        resolved_events: resolved.resolved_events,
        factories: resolved.factories,
        resolved_transfers: resolved.transfers,
        resolved_calls: resolved.calls,
        resolved_streams: resolved.streams,
        start_block,
    })
}

/// Run the indexer in either historical or follow mode.
///
/// # Errors
///
/// Returns an error on sync or database failures.
async fn run_indexer(
    cli: &cli::Cli,
    start_block: BlockNumber,
    ctx: sync::SyncContext,
) -> eyre::Result<()> {
    if let Some(end_block_raw) = cli.end_block {
        let end_block = BlockNumber::new(end_block_raw);
        let effective_start = resolve_effective_start(&ctx.db, start_block, end_block).await?;

        if effective_start > end_block {
            info!("nothing to index");
            ctx.metrics.is_ready.store(true, std::sync::atomic::Ordering::Relaxed);
            return Ok(());
        }

        let metrics = Arc::clone(&ctx.metrics);
        let outcome = sync::run_sync(effective_start, end_block, ctx).await?;

        // Historical sync complete — mark as ready
        metrics.is_ready.store(true, std::sync::atomic::Ordering::Relaxed);

        info!(
            blocks = outcome.blocks_fetched,
            receipts = outcome.total_receipts,
            events_matched = outcome.events_matched,
            events_decoded = outcome.events_decoded,
            events_stored = outcome.events_stored,
            transfers_stored = outcome.transfers_stored,
            calls_stored = outcome.calls_stored,
            elapsed_ms = outcome.elapsed.as_millis() as u64,
            "sync complete"
        );
    } else {
        sync::run_follow_loop(start_block, ctx).await?;
    }

    Ok(())
}

/// Connect to PostgreSQL, optionally drop tables, and create schema.
///
/// # Errors
///
/// Returns an error if the database connection or DDL fails.
async fn setup_database(cli: &cli::Cli, startup: &StartupConfig) -> eyre::Result<db::Database> {
    let db = db::Database::connect(&startup.database_url).await?;
    if cli.fresh {
        warn!("--fresh: dropping all tables");
        db::drop_all_tables(
            &db,
            &startup.resolved_events,
            &startup.resolved_transfers,
            &startup.resolved_calls,
        )
        .await?;
    }
    db::create_internal_tables(&db).await?;
    db::create_user_tables(&db, &startup.resolved_events).await?;
    db::create_transfer_tables(&db, &startup.resolved_transfers).await?;
    db::create_call_tables(&db, &startup.resolved_calls).await?;
    Ok(db)
}

/// Determine the effective start block, accounting for checkpoint resume.
///
/// # Errors
///
/// Returns an error if the checkpoint read fails.
async fn resolve_effective_start(
    db: &db::Database,
    start_block: BlockNumber,
    end_block: BlockNumber,
) -> eyre::Result<BlockNumber> {
    if let Some(checkpoint) = db.last_checkpoint().await? {
        if checkpoint >= end_block {
            info!(
                checkpoint = checkpoint.as_u64(),
                end_block = end_block.as_u64(),
                "range already indexed, nothing to do"
            );
            return Ok(BlockNumber::new(end_block.as_u64() + 1)); // signals "nothing to index"
        }
        if checkpoint >= start_block {
            let resume_from = BlockNumber::new(checkpoint.as_u64() + 1);
            info!(checkpoint = checkpoint.as_u64(), resume_from = resume_from.as_u64(), "resuming from checkpoint");
            return Ok(resume_from);
        }
    }
    Ok(start_block)
}

/// Handle Ctrl+C for graceful shutdown.
///
/// First signal: set the stop flag so all loops drain gracefully.
/// Second signal: hard exit (for impatient users).
#[expect(clippy::exit, reason = "second Ctrl+C requires immediate hard exit")]
async fn shutdown_handler(stop_tx: watch::Sender<bool>) {
    // First Ctrl+C → graceful shutdown
    tokio::signal::ctrl_c()
        .await
        .ok();
    warn!("shutdown signal received; stopping after draining");
    let _ = stop_tx.send(true);

    // Second Ctrl+C → hard exit
    tokio::signal::ctrl_c()
        .await
        .ok();
    warn!("second shutdown signal received; forcing exit");
    std::process::exit(130);
}

/// Build the event table map: `"contract:event"` → `(table_name, event_name)`.
///
/// Used by the sync engine to map decoded events to table names for
/// stream notification payloads.
fn build_event_table_map(
    events: &[toml_config::ResolvedEvent],
) -> HashMap<String, (String, String)> {
    let mut map = HashMap::with_capacity(events.len());
    for re in events {
        let key = format!("{}:{}", re.contract_name, re.event_name);
        map.insert(key, (re.table_name.clone(), re.event_name.clone()));
    }
    map
}

/// Build the set of table names that have `include_receipts = true`.
///
/// Used by the sync engine to decide whether to enrich streaming payloads
/// with receipt/tx metadata for a given table.
fn build_receipt_tables(
    events: &[toml_config::ResolvedEvent],
    transfers: &[toml_config::ResolvedTransfer],
    calls: &[toml_config::ResolvedCall],
) -> HashSet<String> {
    let mut set = HashSet::new();
    for e in events {
        if e.include_receipts {
            set.insert(e.table_name.clone());
        }
    }
    for t in transfers {
        if t.include_receipts {
            set.insert(t.table_name.clone());
        }
    }
    for c in calls {
        if c.include_receipts {
            set.insert(c.table_name.clone());
        }
    }
    set
}

/// Build stream sinks from resolved stream definitions.
///
/// Returns `Vec<(sink, backfill)>` for the `StreamDispatcher`.
fn build_stream_sinks(
    streams: &[toml_config::ResolvedStream],
) -> Vec<(Box<dyn stream::StreamSink>, bool)> {
    streams
        .iter()
        .filter_map(|s| {
            let sink: Box<dyn stream::StreamSink> = match s.stream_type.as_str() {
                "webhook" => {
                    Box::new(stream::webhook::WebhookSink::new(s.name.clone(), s.url.clone()))
                }
                "rabbitmq" => {
                    let exchange = s.exchange.clone().unwrap_or_default();
                    let default_routing_key = ["{table}", ".", "{event}"].concat();
                    let routing_key = s
                        .routing_key
                        .clone()
                        .unwrap_or(default_routing_key);
                    Box::new(stream::rabbitmq::RabbitMqSink::new(
                        s.name.clone(),
                        s.url.clone(),
                        exchange,
                        routing_key,
                    ))
                }
                other => {
                    warn!(stream = %s.name, stream_type = %other, "unknown stream type, skipping");
                    return None;
                }
            };
            Some((sink, s.backfill))
        })
        .collect()
}
