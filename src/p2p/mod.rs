//! P2P networking layer.
//!
//! Connects to Ethereum mainnet via devp2p, discovers peers, establishes
//! sessions, and maintains a pool of active peers for future data fetching.

use alloy_primitives::B256;
use eyre::{eyre, Result, WrapErr};
use futures::StreamExt;
use parking_lot::RwLock;
use reth_chainspec::MAINNET;
use reth_eth_wire::{EthNetworkPrimitives, EthVersion};
use reth_eth_wire_types::{
    BlockHashOrNumber, GetBlockBodies, GetBlockHeaders, GetReceipts, GetReceipts70,
    HeadersDirection,
};
use reth_ethereum_primitives::Receipt;
use reth_network::config::{rng_secret_key, NetworkConfigBuilder};
use reth_network::import::ProofOfStakeBlockImport;
use reth_network::{NetworkHandle, PeersConfig, PeersInfo};
use reth_network_api::events::PeerEvent;
use reth_network_api::{
    DiscoveredEvent, DiscoveryEvent, NetworkEvent, NetworkEventListenerProvider, PeerId,
    PeerRequest, PeerRequestSender,
};
use reth_primitives_traits::{Header, SealedHeader};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{oneshot, Semaphore};
use tokio::time::{sleep, timeout, Duration, Instant};
use tracing::{debug, info};

use crate::sync::BlockPayload;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(4);
const MIN_PEER_START: usize = 1;
const PEER_DISCOVERY_TIMEOUT: Option<Duration> = None;
const PEER_START_WARMUP_SECS: u64 = 2;
const MAX_OUTBOUND: usize = 400;
const MAX_INBOUND: usize = 200;
const MAX_CONCURRENT_DIALS: usize = 200;
const PEER_REFILL_INTERVAL_MS: u64 = 500;
const MAX_HEADERS_PER_REQUEST: usize = 1024;

// ── NetworkPeer ──────────────────────────────────────────────────────

/// Active peer session information used for requests.
#[derive(Clone, Debug)]
pub struct NetworkPeer {
    pub peer_id: PeerId,
    pub eth_version: EthVersion,
    pub messages: PeerRequestSender<PeerRequest<EthNetworkPrimitives>>,
    pub head_number: u64,
    pub last_success: Instant,
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

    /// Clone the current list of peers (snapshot in time).
    #[must_use]
    pub fn snapshot(&self) -> Vec<NetworkPeer> {
        self.peers.read().clone()
    }

    /// Add a peer if not already present.
    fn add_peer(&self, peer: NetworkPeer) {
        let mut peers = self.peers.write();
        if peers
            .iter()
            .any(|existing| existing.peer_id == peer.peer_id)
        {
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

    /// Update a peer's head block number (monotonic: only advances forward).
    pub fn update_peer_head(&self, peer_id: PeerId, head_number: u64) {
        let mut peers = self.peers.write();
        if let Some(peer) = peers.iter_mut().find(|p| p.peer_id == peer_id) {
            if head_number > peer.head_number {
                peer.head_number = head_number;
            }
        }
    }

    /// Return the highest known head block number across all peers.
    ///
    /// Ignores peers whose head has not been probed yet (`head_number == 0`).
    #[must_use]
    pub fn best_peer_head(&self) -> Option<u64> {
        self.peers
            .read()
            .iter()
            .map(|p| p.head_number)
            .filter(|&h| h > 0)
            .max()
    }

    /// Remove peers with no successful request within `threshold`.
    pub fn evict_stale(&self, threshold: Duration) -> usize {
        let mut peers = self.peers.write();
        let before = peers.len();
        peers.retain(|p| p.last_success.elapsed() < threshold);
        before - peers.len()
    }

    /// Update last-success timestamp for a peer.
    #[expect(dead_code, reason = "called from sync engine on successful fetches")]
    pub fn mark_peer_success(&self, peer_id: PeerId) {
        let mut peers = self.peers.write();
        if let Some(peer) = peers.iter_mut().find(|p| p.peer_id == peer_id) {
            peer.last_success = Instant::now();
        }
    }
}

// ── Fetch types ─────────────────────────────────────────────────────

/// Timing and request counts per fetch stage.
#[derive(Debug, Clone, Copy, Default)]
#[expect(
    dead_code,
    reason = "stats populated during fetch for future metrics/logging"
)]
pub struct FetchStageStats {
    pub headers_ms: u64,
    pub bodies_ms: u64,
    pub receipts_ms: u64,
    pub headers_requests: u64,
    pub bodies_requests: u64,
    pub receipts_requests: u64,
}

/// Headers response with request count for stats tracking.
#[derive(Debug)]
struct HeadersChunkedResponse {
    headers: Vec<Header>,
    requests: u64,
}

/// Chunked response with partial results (None = missing item).
#[derive(Debug)]
struct ChunkedResponse<T> {
    results: Vec<Option<T>>,
    requests: u64,
}

/// Outcome of a header-only fetch for a peer.
#[derive(Debug)]
pub struct HeaderFetchOutcome {
    pub headers: Vec<Header>,
    pub missing_blocks: Vec<u64>,
    pub headers_ms: u64,
    pub headers_requests: u64,
}

/// Outcome of a full payload fetch for a peer.
#[derive(Debug)]
pub struct PayloadFetchOutcome {
    pub payloads: Vec<BlockPayload>,
    pub missing_blocks: Vec<u64>,
    pub bloom_skipped: Vec<u64>,
    pub fetch_stats: FetchStageStats,
}

// ── NetworkSession ───────────────────────────────────────────────────

/// Keeps the network handle alive and provides access to the peer pool.
#[derive(Debug)]
pub struct NetworkSession {
    /// Must be held alive to keep the P2P network running.
    #[expect(dead_code, reason = "held alive to keep network running")]
    pub handle: NetworkHandle<EthNetworkPrimitives>,
    pub pool: Arc<PeerPool>,
    /// Aggregate connection statistics.
    #[expect(dead_code, reason = "populated for future metrics/logging")]
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
    spawn_peer_watcher(handle.clone(), Arc::clone(&pool), Arc::clone(&p2p_stats));

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
/// Also probes each new peer's head block number via semaphore-limited tasks.
fn spawn_peer_watcher(
    handle: NetworkHandle<EthNetworkPrimitives>,
    pool: Arc<PeerPool>,
    p2p_stats: Arc<P2pStats>,
) {
    tokio::spawn(async move {
        let mut events = handle.event_listener();
        let head_probe_semaphore = Arc::new(Semaphore::new(24));
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

                    let head_hash = info.status.blockhash;
                    let messages_for_probe = messages.clone();

                    pool.add_peer(NetworkPeer {
                        peer_id,
                        eth_version: info.version,
                        messages,
                        head_number: 0,
                        last_success: Instant::now(),
                    });

                    info!(peers = pool.len(), "peer connected");

                    let pool_for_probe = Arc::clone(&pool);
                    let semaphore = Arc::clone(&head_probe_semaphore);
                    tokio::spawn(async move {
                        let Ok(_permit) = semaphore.acquire_owned().await else {
                            return;
                        };
                        match request_head_number(peer_id, head_hash, &messages_for_probe).await {
                            Ok(head_number) => {
                                pool_for_probe.update_peer_head(peer_id, head_number);
                            }
                            Err(err) => {
                                debug!(
                                    peer_id = ?peer_id,
                                    error = %err,
                                    "failed to probe peer head; keeping peer with unknown head"
                                );
                            }
                        }
                    });
                }
                NetworkEvent::Peer(PeerEvent::SessionClosed { peer_id, reason }) => {
                    p2p_stats.sessions_closed.fetch_add(1, Ordering::Relaxed);
                    pool.remove_peer(peer_id);
                    debug!(
                        peer_id = %format!("{:#}", peer_id),
                        reason = ?reason,
                        peers = pool.len(),
                        "peer disconnected"
                    );
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

// ── Head discovery ──────────────────────────────────────────────────

/// Global cursor for round-robin peer rotation across `discover_head_p2p` calls.
static HEAD_PROBE_CURSOR: AtomicUsize = AtomicUsize::new(0);

/// Discover the highest block number from the peer pool above `baseline`.
///
/// Probes up to `probe_peers` peers, requesting headers starting at
/// `baseline + 1`. Returns the highest block number actually confirmed
/// via header fetch, or `None` if the pool is empty.
///
/// Uses a global cursor to rotate across peers between calls, spreading
/// probe load evenly across the pool.
///
/// # Errors
///
/// Returns an error only on unexpected failures (not peer timeouts).
pub async fn discover_head_p2p(
    pool: &PeerPool,
    baseline: u64,
    probe_peers: usize,
    probe_limit: usize,
) -> Result<Option<u64>> {
    let peers = pool.snapshot();
    if peers.is_empty() {
        return Ok(None);
    }

    // IMPORTANT: do not trust `peer.head_number` as a head signal for follow mode.
    //
    // Many peers will return a `Status` best hash, but later refuse to serve headers by number
    // (or will be behind / on a different fork). If we treat `head_number` as authoritative, we
    // will tip-chase and spam `GetBlockHeaders` beyond the peer's view.
    //
    // Instead, only advance the observed head if we can actually fetch headers above `baseline`.
    let mut best = baseline;
    let probe_peers = probe_peers.max(1);
    let probe_limit = probe_limit.clamp(1, MAX_HEADERS_PER_REQUEST);
    let start = baseline.saturating_add(1);

    let len = peers.len();
    let start_idx = HEAD_PROBE_CURSOR.fetch_add(1, Ordering::Relaxed) % len;
    for (probed, offset) in (0..len).enumerate() {
        if probed >= probe_peers {
            break;
        }
        let peer = &peers[(start_idx + offset) % len];
        match request_headers_batch(peer, start, probe_limit).await {
            Ok(headers) => {
                if let Some(last) = headers.last() {
                    best = best.max(last.number);
                }
            }
            Err(e) => {
                debug!(
                    peer_id = ?peer.peer_id,
                    error = %e,
                    "head probe failed"
                );
            }
        }
    }

    Ok(Some(best))
}

// ── Low-level request functions ──────────────────────────────────────

async fn request_head_number(
    peer_id: PeerId,
    head_hash: B256,
    messages: &PeerRequestSender<PeerRequest<EthNetworkPrimitives>>,
) -> Result<u64> {
    let headers = request_headers_by_hash(peer_id, head_hash, messages).await?;
    let header = headers
        .first()
        .ok_or_else(|| eyre!("empty header response for head"))?;
    Ok(header.number)
}

async fn request_headers_by_number(
    peer_id: PeerId,
    start_block: u64,
    limit: usize,
    messages: &PeerRequestSender<PeerRequest<EthNetworkPrimitives>>,
) -> Result<Vec<Header>> {
    let request = GetBlockHeaders {
        start_block: BlockHashOrNumber::Number(start_block),
        limit: limit as u64,
        skip: 0,
        direction: HeadersDirection::Rising,
    };
    let (tx, rx) = oneshot::channel();
    messages
        .try_send(PeerRequest::GetBlockHeaders {
            request,
            response: tx,
        })
        .map_err(|err| eyre!("failed to send header request: {err:?}"))?;
    let response = timeout(REQUEST_TIMEOUT, rx)
        .await
        .map_err(|_| eyre!("header request to {peer_id:?} timed out"))??;
    let headers =
        response.map_err(|err| eyre!("header response error from {peer_id:?}: {err:?}"))?;
    Ok(headers.0)
}

async fn request_headers_by_hash(
    peer_id: PeerId,
    hash: B256,
    messages: &PeerRequestSender<PeerRequest<EthNetworkPrimitives>>,
) -> Result<Vec<Header>> {
    let request = GetBlockHeaders {
        start_block: BlockHashOrNumber::Hash(hash),
        limit: 1,
        skip: 0,
        direction: HeadersDirection::Rising,
    };
    let (tx, rx) = oneshot::channel();
    messages
        .try_send(PeerRequest::GetBlockHeaders {
            request,
            response: tx,
        })
        .map_err(|err| eyre!("failed to send header request: {err:?}"))?;
    let response = timeout(REQUEST_TIMEOUT, rx)
        .await
        .map_err(|_| eyre!("header request to {peer_id:?} timed out"))??;
    let headers =
        response.map_err(|err| eyre!("header response error from {peer_id:?}: {err:?}"))?;
    Ok(headers.0)
}

async fn request_bodies(
    peer: &NetworkPeer,
    hashes: &[B256],
) -> Result<Vec<reth_ethereum_primitives::BlockBody>> {
    let request = GetBlockBodies::from(hashes.to_vec());
    let (tx, rx) = oneshot::channel();
    peer.messages
        .try_send(PeerRequest::GetBlockBodies {
            request,
            response: tx,
        })
        .map_err(|err| eyre!("failed to send body request: {err:?}"))?;
    let response = timeout(REQUEST_TIMEOUT, rx)
        .await
        .map_err(|_| eyre!("body request to {:?} timed out", peer.peer_id))??;
    let bodies =
        response.map_err(|err| eyre!("body response error from {:?}: {err:?}", peer.peer_id))?;
    Ok(bodies.0)
}

async fn request_receipts_legacy(peer: &NetworkPeer, hashes: &[B256]) -> Result<Vec<Vec<Receipt>>> {
    let request = GetReceipts(hashes.to_vec());
    let (tx, rx) = oneshot::channel();
    peer.messages
        .try_send(PeerRequest::GetReceipts {
            request,
            response: tx,
        })
        .map_err(|err| eyre!("failed to send receipts request: {err:?}"))?;
    let response = timeout(REQUEST_TIMEOUT, rx)
        .await
        .map_err(|_| eyre!("receipts request to {:?} timed out", peer.peer_id))??;
    let receipts = response
        .map_err(|err| eyre!("receipts response error from {:?}: {err:?}", peer.peer_id))?;
    Ok(receipts
        .0
        .into_iter()
        .map(|block| block.into_iter().map(|r| r.receipt).collect())
        .collect())
}

async fn request_receipts69(peer: &NetworkPeer, hashes: &[B256]) -> Result<Vec<Vec<Receipt>>> {
    let request = GetReceipts(hashes.to_vec());
    let (tx, rx) = oneshot::channel();
    peer.messages
        .try_send(PeerRequest::GetReceipts69 {
            request,
            response: tx,
        })
        .map_err(|err| eyre!("failed to send receipts69 request: {err:?}"))?;
    let response = timeout(REQUEST_TIMEOUT, rx)
        .await
        .map_err(|_| eyre!("receipts69 request to {:?} timed out", peer.peer_id))??;
    let receipts = response
        .map_err(|err| eyre!("receipts69 response error from {:?}: {err:?}", peer.peer_id))?;
    Ok(receipts.0)
}

async fn request_receipts70(peer: &NetworkPeer, hashes: &[B256]) -> Result<Vec<Vec<Receipt>>> {
    let request = GetReceipts70 {
        first_block_receipt_index: 0,
        block_hashes: hashes.to_vec(),
    };
    let (tx, rx) = oneshot::channel();
    peer.messages
        .try_send(PeerRequest::GetReceipts70 {
            request,
            response: tx,
        })
        .map_err(|err| eyre!("failed to send receipts70 request: {err:?}"))?;
    let response = timeout(REQUEST_TIMEOUT, rx)
        .await
        .map_err(|_| eyre!("receipts70 request to {:?} timed out", peer.peer_id))??;
    let receipts = response
        .map_err(|err| eyre!("receipts70 response error from {:?}: {err:?}", peer.peer_id))?;
    Ok(receipts.receipts)
}

// ── Mid-level request functions ──────────────────────────────────────

/// Fetch `count` sequential headers starting from `start_block`.
///
/// Chunks large requests to stay within protocol limits.
///
/// # Errors
///
/// Returns an error if the P2P request times out or the peer disconnects.
pub async fn request_headers_batch(
    peer: &NetworkPeer,
    start_block: u64,
    limit: usize,
) -> Result<Vec<Header>> {
    request_headers_by_number(peer.peer_id, start_block, limit, &peer.messages).await
}

async fn request_headers_chunked_with_stats(
    peer: &NetworkPeer,
    start_block: u64,
    count: usize,
) -> Result<HeadersChunkedResponse> {
    if count == 0 {
        return Ok(HeadersChunkedResponse {
            headers: Vec::new(),
            requests: 0,
        });
    }
    let mut headers = Vec::with_capacity(count);
    let mut current = start_block;
    let mut remaining = count;
    let mut requests = 0u64;
    while remaining > 0 {
        let batch = remaining.min(MAX_HEADERS_PER_REQUEST);
        let mut batch_headers = request_headers_batch(peer, current, batch).await?;
        requests = requests.saturating_add(1);
        if batch_headers.is_empty() {
            break;
        }
        let received = batch_headers.len();
        headers.append(&mut batch_headers);
        if received < batch {
            break;
        }
        current = current.saturating_add(batch as u64);
        remaining = remaining.saturating_sub(batch);
    }
    Ok(HeadersChunkedResponse { headers, requests })
}

/// Fetch receipts for blocks identified by hash, using the peer's ETH version.
///
/// # Errors
///
/// Returns an error if the P2P request times out or the peer disconnects.
pub async fn request_receipts(peer: &NetworkPeer, hashes: &[B256]) -> Result<Vec<Vec<Receipt>>> {
    match peer.eth_version {
        EthVersion::Eth70 => request_receipts70(peer, hashes).await,
        EthVersion::Eth69 => request_receipts69(peer, hashes).await,
        _ => request_receipts_legacy(peer, hashes).await,
    }
}

async fn request_bodies_chunked_partial_with_stats(
    peer: &NetworkPeer,
    hashes: &[B256],
) -> Result<ChunkedResponse<reth_ethereum_primitives::BlockBody>> {
    if hashes.is_empty() {
        return Ok(ChunkedResponse {
            results: Vec::new(),
            requests: 0,
        });
    }

    let mut results: Vec<Option<reth_ethereum_primitives::BlockBody>> = vec![None; hashes.len()];
    let mut cursor = 0usize;
    let mut requests = 0u64;
    while cursor < hashes.len() {
        let slice = &hashes[cursor..];
        let requested = slice.len();
        let bodies = request_bodies(peer, slice).await?;
        requests = requests.saturating_add(1);
        if bodies.is_empty() {
            break;
        }
        if bodies.len() > slice.len() {
            return Err(eyre!(
                "body count mismatch: expected <= {}, got {}",
                slice.len(),
                bodies.len()
            ));
        }
        let received = bodies.len();
        for (offset, body) in bodies.into_iter().enumerate() {
            results[cursor + offset] = Some(body);
        }
        cursor = cursor.saturating_add(received);
        if received < requested {
            break;
        }
    }

    Ok(ChunkedResponse { results, requests })
}

async fn request_receipts_chunked_partial_with_stats(
    peer: &NetworkPeer,
    hashes: &[B256],
) -> Result<ChunkedResponse<Vec<Receipt>>> {
    if hashes.is_empty() {
        return Ok(ChunkedResponse {
            results: Vec::new(),
            requests: 0,
        });
    }

    let mut results: Vec<Option<Vec<Receipt>>> = vec![None; hashes.len()];
    let mut cursor = 0usize;
    let mut requests = 0u64;
    while cursor < hashes.len() {
        let slice = &hashes[cursor..];
        let requested = slice.len();
        let receipts = request_receipts(peer, slice).await?;
        requests = requests.saturating_add(1);
        if receipts.is_empty() {
            break;
        }
        if receipts.len() > slice.len() {
            return Err(eyre!(
                "receipt count mismatch: expected <= {}, got {}",
                slice.len(),
                receipts.len()
            ));
        }
        let received = receipts.len();
        for (offset, block_receipts) in receipts.into_iter().enumerate() {
            results[cursor + offset] = Some(block_receipts);
        }
        cursor = cursor.saturating_add(received);
        if received < requested {
            break;
        }
    }

    Ok(ChunkedResponse { results, requests })
}

// ── High-level fetch ─────────────────────────────────────────────────

/// Fetch full payloads (header + body + receipts) for a block range from a peer.
///
/// Fetches headers first, then bodies and receipts in parallel. Returns
/// assembled payloads and a list of any missing blocks.
///
/// # Errors
///
/// Returns an error if the underlying P2P requests fail.
/// Fetch only headers for a block range from a peer.
pub async fn fetch_headers_for_peer(
    peer: &NetworkPeer,
    range: std::ops::RangeInclusive<u64>,
) -> Result<HeaderFetchOutcome> {
    let start = *range.start();
    let end = *range.end();
    let count = (end - start + 1) as usize;

    let headers_start = Instant::now();
    let headers_response = request_headers_chunked_with_stats(peer, start, count).await?;
    let headers_ms = headers_start.elapsed().as_millis() as u64;
    let headers_requests = headers_response.requests;

    let mut headers_by_number = HashMap::new();
    for header in headers_response.headers {
        headers_by_number.insert(header.number, header);
    }

    let mut ordered_headers = Vec::new();
    let mut missing_blocks = Vec::new();
    for number in start..=end {
        if let Some(header) = headers_by_number.remove(&number) {
            ordered_headers.push(header);
        } else {
            missing_blocks.push(number);
        }
    }

    Ok(HeaderFetchOutcome {
        headers: ordered_headers,
        missing_blocks,
        headers_ms,
        headers_requests,
    })
}

/// Fetch bodies and receipts for a set of headers from a peer.
///
/// Headers must already be fetched; this computes their hashes and requests
/// the corresponding bodies and receipts in parallel.
pub async fn fetch_payloads_for_headers(
    peer: &NetworkPeer,
    ordered_headers: Vec<Header>,
) -> Result<PayloadFetchOutcome> {
    if ordered_headers.is_empty() {
        return Ok(PayloadFetchOutcome {
            payloads: Vec::new(),
            missing_blocks: Vec::new(),
            bloom_skipped: Vec::new(),
            fetch_stats: FetchStageStats::default(),
        });
    }

    let mut hashes = Vec::with_capacity(ordered_headers.len());
    for header in &ordered_headers {
        let hash = SealedHeader::seal_slow(header.clone()).hash();
        hashes.push(hash);
    }

    let bodies_fut = async {
        let started = Instant::now();
        let resp = request_bodies_chunked_partial_with_stats(peer, &hashes).await?;
        Ok::<_, eyre::Report>((resp, started.elapsed().as_millis() as u64))
    };
    let receipts_fut = async {
        let started = Instant::now();
        let resp = request_receipts_chunked_partial_with_stats(peer, &hashes).await?;
        Ok::<_, eyre::Report>((resp, started.elapsed().as_millis() as u64))
    };
    let ((bodies, bodies_ms), (receipts, receipts_ms)) =
        tokio::try_join!(bodies_fut, receipts_fut)?;
    let bodies_requests = bodies.requests;
    let receipts_requests = receipts.requests;
    let mut bodies = bodies.results;
    let mut receipts = receipts.results;

    let mut payloads = Vec::with_capacity(ordered_headers.len());
    let mut missing_blocks = Vec::new();
    for (idx, header) in ordered_headers.into_iter().enumerate() {
        let number = header.number;
        let body = bodies.get_mut(idx).and_then(Option::take);
        let block_receipts = receipts.get_mut(idx).and_then(Option::take);

        match (body, block_receipts) {
            (Some(body), Some(block_receipts)) => {
                if body.transactions.len() != block_receipts.len() {
                    missing_blocks.push(number);
                    continue;
                }
                payloads.push(BlockPayload::new(header, body, block_receipts));
            }
            _ => {
                missing_blocks.push(number);
            }
        }
    }

    Ok(PayloadFetchOutcome {
        payloads,
        missing_blocks,
        bloom_skipped: Vec::new(),
        fetch_stats: FetchStageStats {
            headers_ms: 0,
            bodies_ms,
            receipts_ms,
            headers_requests: 0,
            bodies_requests,
            receipts_requests,
        },
    })
}

/// Fetch full block payloads (headers + bodies + receipts) for a range.
pub async fn fetch_payloads_for_peer(
    peer: &NetworkPeer,
    range: std::ops::RangeInclusive<u64>,
) -> Result<PayloadFetchOutcome> {
    let header_outcome = fetch_headers_for_peer(peer, range).await?;

    if header_outcome.headers.is_empty() {
        return Ok(PayloadFetchOutcome {
            payloads: Vec::new(),
            missing_blocks: header_outcome.missing_blocks,
            bloom_skipped: Vec::new(),
            fetch_stats: FetchStageStats {
                headers_ms: header_outcome.headers_ms,
                headers_requests: header_outcome.headers_requests,
                ..FetchStageStats::default()
            },
        });
    }

    let mut result = fetch_payloads_for_headers(peer, header_outcome.headers).await?;
    result.missing_blocks.extend(header_outcome.missing_blocks);
    result.fetch_stats.headers_ms = header_outcome.headers_ms;
    result.fetch_stats.headers_requests = header_outcome.headers_requests;
    Ok(result)
}
