//! P2P networking layer — adapted from SHiNode.
//!
//! Connects to Ethereum mainnet via devp2p, discovers peers, establishes
//! sessions, and maintains a pool of active peers for future data fetching.

use eyre::{eyre, Result, WrapErr};
use futures::StreamExt;
use parking_lot::RwLock;
use reth_chainspec::MAINNET;
use reth_eth_wire::{EthNetworkPrimitives, EthVersion};
use reth_network::config::{rng_secret_key, NetworkConfigBuilder};
use reth_network::import::ProofOfStakeBlockImport;
use reth_network::{NetworkHandle, PeersConfig, PeersInfo};
use reth_network_api::events::PeerEvent;
use reth_network_api::{
    DiscoveredEvent, DiscoveryEvent, NetworkEvent, NetworkEventListenerProvider, PeerId,
    PeerRequest, PeerRequestSender,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::time::{sleep, Duration, Instant};
use tracing::{debug, info};

const MIN_PEER_START: usize = 1;
const PEER_DISCOVERY_TIMEOUT: Option<Duration> = None;
const PEER_START_WARMUP_SECS: u64 = 2;
const MAX_OUTBOUND: usize = 400;
const MAX_INBOUND: usize = 200;
const MAX_CONCURRENT_DIALS: usize = 200;
const PEER_REFILL_INTERVAL_MS: u64 = 500;

// ── NetworkPeer ──────────────────────────────────────────────────────

/// Active peer session information used for requests.
#[derive(Clone, Debug)]
pub struct NetworkPeer {
    pub peer_id: PeerId,
    pub eth_version: EthVersion,
    pub messages: PeerRequestSender<PeerRequest<EthNetworkPrimitives>>,
    pub head_number: u64,
}

// ── P2pStats ─────────────────────────────────────────────────────────

/// Shared atomic counters for P2P discovery and session visibility.
#[derive(Debug)]
pub struct P2pStats {
    pub discovered_count: AtomicUsize,
    pub genesis_mismatch_count: AtomicUsize,
    pub sessions_established: AtomicUsize,
    pub sessions_closed: AtomicUsize,
}

impl P2pStats {
    const fn new() -> Self {
        Self {
            discovered_count: AtomicUsize::new(0),
            genesis_mismatch_count: AtomicUsize::new(0),
            sessions_established: AtomicUsize::new(0),
            sessions_closed: AtomicUsize::new(0),
        }
    }
}

// ── PeerPool ─────────────────────────────────────────────────────────

/// Thread-safe pool of active Ethereum peers.
#[derive(Debug)]
pub struct PeerPool {
    peers: RwLock<Vec<NetworkPeer>>,
}

impl PeerPool {
    const fn new() -> Self {
        Self {
            peers: RwLock::new(Vec::new()),
        }
    }

    /// Number of peers currently in the pool.
    #[must_use]
    pub fn len(&self) -> usize {
        self.peers.read().len()
    }

    /// Whether the pool has no peers.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.peers.read().is_empty()
    }

    /// Clone the current list of peers (snapshot in time).
    #[must_use]
    pub fn snapshot(&self) -> Vec<NetworkPeer> {
        self.peers.read().clone()
    }

    /// Add a peer if not already present.
    fn add_peer(&self, peer: NetworkPeer) {
        let mut peers = self.peers.write();
        if peers.iter().any(|existing| existing.peer_id == peer.peer_id) {
            return;
        }
        peers.push(peer);
    }

    /// Remove a peer by id.
    fn remove_peer(&self, peer_id: PeerId) {
        let mut peers = self.peers.write();
        peers.retain(|peer| peer.peer_id != peer_id);
    }

    /// Get a peer's reported head block number.
    #[must_use]
    pub fn get_peer_head(&self, peer_id: PeerId) -> Option<u64> {
        self.peers
            .read()
            .iter()
            .find(|p| p.peer_id == peer_id)
            .map(|p| p.head_number)
    }

    /// Update a peer's head block number.
    pub fn update_peer_head(&self, peer_id: PeerId, head_number: u64) {
        let mut peers = self.peers.write();
        if let Some(peer) = peers.iter_mut().find(|p| p.peer_id == peer_id) {
            peer.head_number = head_number;
        }
    }
}

// ── NetworkSession ───────────────────────────────────────────────────

/// Keeps the network handle alive and provides access to the peer pool.
#[derive(Debug)]
pub struct NetworkSession {
    pub handle: NetworkHandle<EthNetworkPrimitives>,
    pub pool: Arc<PeerPool>,
    pub p2p_stats: Arc<P2pStats>,
}

// ── connect_mainnet_peers ────────────────────────────────────────────

/// Start the devp2p network, discover peers, and wait for initial connections.
///
/// # Errors
///
/// Returns an error if the network fails to start or no peers connect
/// within the configured timeout.
pub async fn connect_mainnet_peers() -> Result<NetworkSession> {
    let secret_key = rng_secret_key();
    let peers_config = PeersConfig::default()
        .with_max_outbound(MAX_OUTBOUND)
        .with_max_inbound(MAX_INBOUND)
        .with_max_concurrent_dials(MAX_CONCURRENT_DIALS)
        .with_refill_slots_interval(Duration::from_millis(PEER_REFILL_INTERVAL_MS));

    let net_config = NetworkConfigBuilder::<EthNetworkPrimitives>::new(secret_key)
        .mainnet_boot_nodes()
        .with_unused_ports()
        .peer_config(peers_config)
        .disable_tx_gossip(true)
        .block_import(Box::new(ProofOfStakeBlockImport::default()))
        .build_with_noop_provider(MAINNET.clone());

    let handle = net_config
        .start_network()
        .await
        .wrap_err("failed to start p2p network")?;

    let pool = Arc::new(PeerPool::new());
    let p2p_stats = Arc::new(P2pStats::new());

    spawn_peer_discovery_watcher(handle.clone(), Arc::clone(&p2p_stats));
    spawn_peer_watcher(
        handle.clone(),
        Arc::clone(&pool),
        Arc::clone(&p2p_stats),
    );

    let warmup_started = Instant::now();
    let _connected =
        wait_for_peer_pool(Arc::clone(&pool), MIN_PEER_START, PEER_DISCOVERY_TIMEOUT).await?;

    if PEER_START_WARMUP_SECS > 0 {
        let min = Duration::from_secs(PEER_START_WARMUP_SECS);
        let elapsed = warmup_started.elapsed();
        if let Some(remaining) = min.checked_sub(elapsed) {
            sleep(remaining).await;
        }
    }

    info!(
        reth_connected = handle.num_connected_peers(),
        pool_peers = pool.len(),
        discovered = p2p_stats.discovered_count.load(Ordering::Relaxed),
        genesis_mismatches = p2p_stats.genesis_mismatch_count.load(Ordering::Relaxed),
        warmup_ms = warmup_started.elapsed().as_millis() as u64,
        "peer startup complete"
    );

    Ok(NetworkSession {
        handle,
        pool,
        p2p_stats,
    })
}

// ── spawn_peer_watcher ───────────────────────────────────────────────

/// Watch for peer session events and update the pool accordingly.
fn spawn_peer_watcher(
    handle: NetworkHandle<EthNetworkPrimitives>,
    pool: Arc<PeerPool>,
    p2p_stats: Arc<P2pStats>,
) {
    tokio::spawn(async move {
        let mut events = handle.event_listener();
        while let Some(event) = events.next().await {
            match event {
                NetworkEvent::ActivePeerSession { info, messages } => {
                    p2p_stats
                        .sessions_established
                        .fetch_add(1, Ordering::Relaxed);

                    if info.status.genesis != MAINNET.genesis_hash() {
                        p2p_stats
                            .genesis_mismatch_count
                            .fetch_add(1, Ordering::Relaxed);
                        debug!(
                            peer_id = %format!("{:#}", info.peer_id),
                            "ignoring peer: genesis mismatch"
                        );
                        continue;
                    }

                    let peer_id = info.peer_id;
                    debug!(
                        peer_id = %format!("{:#}", peer_id),
                        eth_version = %info.version,
                        "peer session established"
                    );

                    pool.add_peer(NetworkPeer {
                        peer_id,
                        eth_version: info.version,
                        messages,
                        head_number: 0,
                    });
                }
                NetworkEvent::Peer(PeerEvent::SessionClosed { peer_id, reason }) => {
                    p2p_stats.sessions_closed.fetch_add(1, Ordering::Relaxed);
                    debug!(
                        peer_id = %format!("{:#}", peer_id),
                        reason = ?reason,
                        "peer session closed"
                    );
                    pool.remove_peer(peer_id);
                }
                NetworkEvent::Peer(PeerEvent::PeerRemoved(peer_id)) => {
                    pool.remove_peer(peer_id);
                }
                NetworkEvent::Peer(_) => {}
            }
        }
    });
}

// ── spawn_peer_discovery_watcher ─────────────────────────────────────

/// Watch discovery events and count discovered peers.
fn spawn_peer_discovery_watcher(
    handle: NetworkHandle<EthNetworkPrimitives>,
    p2p_stats: Arc<P2pStats>,
) {
    tokio::spawn(async move {
        let mut events = handle.discovery_listener();
        let mut log_interval = tokio::time::interval(Duration::from_secs(30));
        log_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                event = events.next() => {
                    let Some(event) = event else { break };
                    if let DiscoveryEvent::NewNode(
                        DiscoveredEvent::EventQueued { .. }
                    ) = event
                    {
                        p2p_stats.discovered_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                _ = log_interval.tick() => {
                    let count = p2p_stats.discovered_count.load(Ordering::Relaxed);
                    debug!(discovered = count, "DHT discovery progress");
                }
            }
        }
    });
}

// ── wait_for_peer_pool ───────────────────────────────────────────────

/// Poll until the peer pool reaches the target size (or timeout expires).
///
/// # Errors
///
/// Returns an error if the timeout expires and zero peers have connected.
async fn wait_for_peer_pool(
    pool: Arc<PeerPool>,
    target: usize,
    timeout_after: Option<Duration>,
) -> Result<usize> {
    let deadline = timeout_after.map(|d| Instant::now() + d);

    loop {
        let peers = pool.len();
        if peers >= target {
            return Ok(peers);
        }

        if let Some(deadline) = deadline {
            if Instant::now() >= deadline {
                if peers == 0 {
                    return Err(eyre!(
                        "no peers connected within {:?}; check network access",
                        timeout_after.unwrap_or_default()
                    ));
                }
                return Ok(peers);
            }
        }

        sleep(Duration::from_millis(200)).await;
    }
}
