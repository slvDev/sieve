#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod cli;
mod config;
mod db;
mod decode;
mod filter;
mod handler;
mod p2p;
mod sync;
mod toml_config;

use clap::Parser;
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
        if end_block < startup.start_block {
            return Err(eyre::eyre!(
                "--end-block ({end_block}) must be >= start_block ({})",
                startup.start_block
            ));
        }
    }

    info!(
        start_block = startup.start_block,
        end_block = cli.end_block,
        mode = if cli.end_block.is_some() { "historical" } else { "follow" },
        "sieve starting"
    );

    // Graceful shutdown signal
    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(shutdown_handler(stop_tx));

    // Database — connect early so checkpoint check is fast (before P2P)
    let db = Arc::new(db::Database::connect(&startup.database_url).await?);
    db::create_user_tables(&db, &startup.resolved_events).await?;

    let index_config = Arc::new(startup.index_config);
    info!(contracts = index_config.contracts.len(), "loaded index config");

    // Build handlers from resolved events
    let handlers: Vec<Box<dyn handler::EventHandler>> = startup
        .resolved_events
        .into_iter()
        .map(|re| -> Box<dyn handler::EventHandler> {
            Box::new(handler::ConfigDrivenHandler::new(re))
        })
        .collect();
    let handlers = Arc::new(handler::HandlerRegistry::new(handlers));
    info!(handlers = handlers.len(), "registered event handlers");

    // P2P
    let session = p2p::connect_mainnet_peers().await?;
    info!(peers = session.pool.len(), "connected to ethereum p2p network");

    run_indexer(&cli, startup.start_block, &session, index_config, db, handlers, stop_rx).await
}

/// Resolved startup parameters from TOML config + CLI.
#[derive(Debug)]
struct StartupConfig {
    database_url: String,
    index_config: config::IndexConfig,
    resolved_events: Vec<toml_config::ResolvedEvent>,
    start_block: u64,
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

    // Compute effective start_block: CLI override or minimum across contracts
    let start_block = cli.start_block.unwrap_or_else(|| {
        sieve_config
            .contracts
            .iter()
            .map(|c| c.start_block)
            .min()
            .unwrap_or(0)
    });

    Ok(StartupConfig {
        database_url,
        index_config: resolved.index_config,
        resolved_events: resolved.resolved_events,
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
    start_block: u64,
    session: &p2p::NetworkSession,
    index_config: Arc<config::IndexConfig>,
    db: Arc<db::Database>,
    handlers: Arc<handler::HandlerRegistry>,
    stop_rx: watch::Receiver<bool>,
) -> eyre::Result<()> {
    if let Some(end_block) = cli.end_block {
        let effective_start = resolve_effective_start(&db, start_block, end_block).await?;

        if effective_start > end_block {
            info!("nothing to index");
            return Ok(());
        }

        let outcome = sync::engine::run_sync(
            Arc::clone(&session.pool),
            effective_start,
            end_block,
            index_config,
            db,
            handlers,
            stop_rx,
        )
        .await?;

        info!(
            blocks = outcome.blocks_fetched,
            receipts = outcome.total_receipts,
            events_matched = outcome.events_matched,
            events_decoded = outcome.events_decoded,
            events_stored = outcome.events_stored,
            elapsed_ms = outcome.elapsed.as_millis() as u64,
            "sync complete"
        );
    } else {
        sync::follow::run_follow_loop(
            Arc::clone(&session.pool),
            start_block,
            index_config,
            db,
            handlers,
            stop_rx,
        )
        .await?;
    }

    Ok(())
}

/// Determine the effective start block, accounting for checkpoint resume.
///
/// # Errors
///
/// Returns an error if the checkpoint read fails.
async fn resolve_effective_start(
    db: &db::Database,
    start_block: u64,
    end_block: u64,
) -> eyre::Result<u64> {
    if let Some(checkpoint) = db.last_checkpoint().await? {
        if checkpoint >= end_block {
            info!(
                checkpoint,
                end_block,
                "range already indexed, nothing to do"
            );
            return Ok(end_block + 1); // signals "nothing to index"
        }
        if checkpoint >= start_block {
            let resume_from = checkpoint + 1;
            info!(checkpoint, resume_from, "resuming from checkpoint");
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
