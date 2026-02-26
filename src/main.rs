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

use clap::Parser;
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

    // Validate --end-block if provided
    if let Some(end_block) = cli.end_block {
        if end_block < cli.start_block {
            return Err(eyre::eyre!(
                "--end-block ({end_block}) must be >= --start-block ({})",
                cli.start_block
            ));
        }
    }

    info!(
        start_block = cli.start_block,
        end_block = cli.end_block,
        mode = if cli.end_block.is_some() { "historical" } else { "follow" },
        "sieve starting"
    );

    // Graceful shutdown signal
    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(shutdown_handler(stop_tx));

    // Database — connect early so checkpoint check is fast (before P2P)
    let db = Arc::new(db::Database::connect(&cli.database_url).await?);

    // Index config
    let index_config = Arc::new(config::usdc_transfer_config()?);
    info!(
        contracts = index_config.contracts.len(),
        "loaded index config"
    );

    // Handlers
    let handlers = Arc::new(handler::HandlerRegistry::new(vec![Box::new(
        handler::UsdcTransferHandler,
    )]));
    info!(handlers = handlers.len(), "registered event handlers");

    // P2P
    let session = p2p::connect_mainnet_peers().await?;
    info!(peers = session.pool.len(), "connected to ethereum p2p network");

    if let Some(end_block) = cli.end_block {
        // Historical mode: sync a fixed range then exit
        let effective_start = resolve_effective_start(&db, cli.start_block, end_block).await?;

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
        // Follow mode: sync to tip, then follow new blocks continuously
        sync::follow::run_follow_loop(
            Arc::clone(&session.pool),
            cli.start_block,
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
