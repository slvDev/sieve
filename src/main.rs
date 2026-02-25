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

use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("sieve starting");

    let index_config = Arc::new(config::usdc_transfer_config()?);
    info!(
        contracts = index_config.contracts.len(),
        "loaded index config"
    );

    let session = p2p::connect_mainnet_peers().await?;

    info!(peers = session.pool.len(), "connected to ethereum p2p network");

    let start_block: u64 = 21_000_000;
    let end_block: u64 = 21_000_100;

    let outcome = sync::engine::run_sync(
        Arc::clone(&session.pool),
        start_block,
        end_block,
        index_config,
    )
    .await?;

    info!(
        blocks = outcome.blocks_fetched,
        receipts = outcome.total_receipts,
        events_matched = outcome.events_matched,
        events_decoded = outcome.events_decoded,
        elapsed_ms = outcome.elapsed.as_millis() as u64,
        "sync complete"
    );

    Ok(())
}
