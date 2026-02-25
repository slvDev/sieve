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

use reth_network::PeersInfo;
use std::sync::atomic::Ordering;
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

    let session = p2p::connect_mainnet_peers().await?;

    info!(peers = session.pool.len(), "connected to ethereum p2p network");

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

        let stats = &session.p2p_stats;
        info!(
            pool_peers = session.pool.len(),
            reth_connected = session.handle.num_connected_peers(),
            discovered = stats.discovered_count.load(Ordering::Relaxed),
            sessions_established = stats.sessions_established.load(Ordering::Relaxed),
            sessions_closed = stats.sessions_closed.load(Ordering::Relaxed),
            genesis_mismatches = stats.genesis_mismatch_count.load(Ordering::Relaxed),
            "p2p status"
        );
    }
}
