//! P2P networking layer.
//!
//! Connects to a chain's devp2p network, discovers peers, establishes
//! sessions, and maintains a pool of active peers for data fetching.
//! Generic over [`ChainTypes`] so the same engine serves every supported
//! chain.

use alloy_consensus::{proofs, BlockBody};
use alloy_primitives::B256;
use eyre::{eyre, Result, WrapErr};
use futures::StreamExt;
use parking_lot::RwLock;
use reth_chainspec::EthChainSpec;
use reth_eth_wire::EthVersion;
use reth_eth_wire_types::{
    BlockHashOrNumber, GetBlockBodies, GetBlockHeaders, GetReceipts, GetReceipts70,
    HeadersDirection,
};
use reth_network::config::{rng_secret_key, NetworkConfigBuilder};
use reth_network::import::ProofOfStakeBlockImport;
use reth_network::{NetworkHandle, PeersConfig, PeersInfo};
use reth_network_api::events::PeerEvent;
use reth_network_api::{
    DiscoveredEvent, DiscoveryEvent, NetworkEvent, NetworkEventListenerProvider, PeerId,
    PeerRequest, PeerRequestSender,
};
use reth_primitives_traits::{Header, SealedHeader};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{oneshot, Semaphore};
use tokio::time::{sleep, timeout, Duration, Instant};
use tracing::{debug, info};

use crate::chain::ChainTypes;
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
const DEFAULT_P2P_PORT: u16 = 30303;
// ── NetworkPeer ──────────────────────────────────────────────────────

/// Active peer session information used for requests.
#[derive(Debug)]
pub struct NetworkPeer<C: ChainTypes> {
    pub peer_id: PeerId,
    pub eth_version: EthVersion,
    pub messages: PeerRequestSender<PeerRequest<C::Net>>,
    pub head_number: u64,
    /// Earliest block this peer can serve (eth/69 Status). `None` = unknown
    /// (pre-eth/69 peer); `Some(0)` = full history.
    pub earliest_block: Option<u64>,
    pub last_success: Instant,
}

// Manual Clone: `C` itself need not be `Clone`, and derive would require it.
impl<C: ChainTypes> Clone for NetworkPeer<C> {
    fn clone(&self) -> Self {
        Self {
            peer_id: self.peer_id,
            eth_version: self.eth_version,
            messages: self.messages.clone(),
            head_number: self.head_number,
            earliest_block: self.earliest_block,
            last_success: self.last_success,
        }
    }
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

/// Thread-safe pool of active peers.
#[derive(Debug)]
pub struct PeerPool<C: ChainTypes> {
    peers: RwLock<Vec<NetworkPeer<C>>>,
}

impl<C: ChainTypes> PeerPool<C> {
    const fn new() -> Self {
        Self {
            peers: RwLock::new(Vec::new()),
        }
    }

    /// Construct an empty pool (test-only: lets the canonical stage's
    /// no-peer refusal path be exercised without a live network).
    #[cfg(test)]
    #[must_use]
    pub const fn new_empty() -> Self {
        Self::new()
    }

    /// Number of peers currently in the pool.
    #[must_use]
    pub fn len(&self) -> usize {
        self.peers.read().len()
    }

    /// Clone the current list of peers (snapshot in time).
    #[must_use]
    pub fn snapshot(&self) -> Vec<NetworkPeer<C>> {
        self.peers.read().clone()
    }

    /// Add a peer if not already present.
    fn add_peer(&self, peer: NetworkPeer<C>) {
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

/// Chunked response with partial results (None = missing item).
#[derive(Debug)]
struct ChunkedResponse<T> {
    results: Vec<Option<T>>,
    requests: u64,
}

/// Outcome of a full payload fetch for a peer.
#[derive(Debug)]
pub struct PayloadFetchOutcome<C: ChainTypes> {
    pub payloads: Vec<BlockPayload<C>>,
    pub missing_blocks: Vec<u64>,
    pub fetch_stats: FetchStageStats,
}

// ── NetworkSession ───────────────────────────────────────────────────

/// Keeps the network handle alive and provides access to the peer pool.
#[derive(Debug)]
pub struct NetworkSession<C: ChainTypes> {
    /// Must be held alive to keep the P2P network running.
    #[expect(dead_code, reason = "held alive to keep network running")]
    pub handle: NetworkHandle<C::Net>,
    pub pool: Arc<PeerPool<C>>,
    /// Aggregate connection statistics.
    #[expect(dead_code, reason = "populated for future metrics/logging")]
    pub p2p_stats: Arc<P2pStats>,
}

// ── connect_peers ────────────────────────────────────────────────────

/// Start the devp2p network for chain `C`, discover peers, and wait for
/// initial connections.
///
/// `trusted_peers` are always dialed and kept connected regardless of
/// reputation (validated upstream at config-resolution time).
///
/// # Errors
///
/// Returns an error if the network fails to start or no peers connect
/// within the configured timeout.
pub async fn connect_peers<C: ChainTypes>(
    p2p_port: Option<u16>,
    trusted_peers: &[reth_network_peers::TrustedPeer],
) -> Result<NetworkSession<C>> {
    let secret_key = rng_secret_key();
    let peers_config = PeersConfig::default()
        .with_max_outbound(MAX_OUTBOUND)
        .with_max_inbound(MAX_INBOUND)
        .with_max_concurrent_dials(MAX_CONCURRENT_DIALS)
        .with_refill_slots_interval(Duration::from_millis(PEER_REFILL_INTERVAL_MS))
        .with_trusted_nodes(trusted_peers.to_vec());

    let chain_spec = C::chain_spec();
    let boot_nodes = chain_spec.bootnodes().unwrap_or_default();

    let mut builder = NetworkConfigBuilder::<C::Net>::new(secret_key)
        .boot_nodes(boot_nodes)
        .peer_config(peers_config)
        .disable_tx_gossip(true)
        .block_import(Box::new(ProofOfStakeBlockImport::default()));

    let listen_addr =
        std::net::SocketAddr::from(([0, 0, 0, 0], p2p_port.unwrap_or(DEFAULT_P2P_PORT)));
    if let Some(port) = p2p_port {
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
        builder = builder.listener_addr(addr).discovery_addr(addr);
    }

    // Chain-specific discovery/boot-node overrides (e.g. Base's basev0 discv5).
    builder = C::configure_network(builder, listen_addr);

    let net_config = builder
        .build(reth_storage_api::noop::NoopProvider::<C::Spec, C::Primitives>::new(chain_spec));

    let handle = net_config
        .start_network()
        .await
        .wrap_err("failed to start p2p network")?;

    let pool = Arc::new(PeerPool::<C>::new());
    let p2p_stats = Arc::new(P2pStats::new());

    let genesis_hash = C::chain_spec().genesis_hash();
    spawn_peer_discovery_watcher::<C>(handle.clone(), Arc::clone(&p2p_stats));
    spawn_peer_watcher::<C>(
        handle.clone(),
        Arc::clone(&pool),
        Arc::clone(&p2p_stats),
        genesis_hash,
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
        chain = C::NAME,
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
fn spawn_peer_watcher<C: ChainTypes>(
    handle: NetworkHandle<C::Net>,
    pool: Arc<PeerPool<C>>,
    p2p_stats: Arc<P2pStats>,
    genesis_hash: B256,
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

                    if info.status.genesis != genesis_hash {
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

                    // The Status head/history range are peer-claimed hints.
                    // The history range is only ever used to AVOID asking a
                    // peer for work, so it is safe to take as-is; the head
                    // number is verified by probing the claimed head hash.
                    let earliest_block = info.status.earliest_block;

                    pool.add_peer(NetworkPeer {
                        peer_id,
                        eth_version: info.version,
                        messages,
                        head_number: 0,
                        earliest_block,
                        last_success: Instant::now(),
                    });

                    info!(peers = pool.len(), "peer connected");

                    let pool_for_probe = Arc::clone(&pool);
                    let semaphore = Arc::clone(&head_probe_semaphore);
                    tokio::spawn(async move {
                        let Ok(_permit) = semaphore.acquire_owned().await else {
                            return;
                        };
                        match request_head_number::<C>(peer_id, head_hash, &messages_for_probe)
                            .await
                        {
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
fn spawn_peer_discovery_watcher<C: ChainTypes>(
    handle: NetworkHandle<C::Net>,
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
async fn wait_for_peer_pool<C: ChainTypes>(
    pool: Arc<PeerPool<C>>,
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
pub async fn discover_head_p2p<C: ChainTypes>(
    pool: &PeerPool<C>,
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
                pool.mark_peer_success(peer.peer_id);
                if let Some(highest) = highest_valid_ascending(start, headers) {
                    best = best.max(highest);
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

/// Validate a rising header response and return the highest trustworthy
/// block number in it.
///
/// Headers must start at exactly `start`, ascend contiguously, and each
/// must parent-link to the previous one. The valid prefix ends at the
/// first violation — a peer replaying unrelated headers (or inventing a
/// single header with a huge number) cannot inflate the observed head.
fn highest_valid_ascending(start: u64, headers: Vec<Header>) -> Option<u64> {
    let mut prev_hash: Option<B256> = None;
    let mut best: Option<u64> = None;
    for (idx, header) in headers.into_iter().enumerate() {
        if header.number != start.checked_add(idx as u64)? {
            break;
        }
        let parent_hash = header.parent_hash;
        let sealed = SealedHeader::seal_slow(header);
        if let Some(prev) = prev_hash {
            if parent_hash != prev {
                break;
            }
        }
        best = Some(sealed.header().number);
        prev_hash = Some(sealed.hash());
    }
    best
}

// ── Low-level request functions ──────────────────────────────────────

/// Resolve the block number of a peer's claimed head hash.
///
/// The response header is only accepted if it actually seals to the
/// requested hash — a peer cannot claim an arbitrary head number without
/// producing a header that hashes to its advertised Status hash.
async fn request_head_number<C: ChainTypes>(
    peer_id: PeerId,
    head_hash: B256,
    messages: &PeerRequestSender<PeerRequest<C::Net>>,
) -> Result<u64> {
    let mut headers = request_headers_by_hash::<C>(peer_id, head_hash, messages).await?;
    if headers.len() != 1 {
        return Err(eyre!(
            "expected exactly one header for head probe, got {}",
            headers.len()
        ));
    }
    let header = headers
        .pop()
        .ok_or_else(|| eyre!("empty header response for head"))?;
    let sealed = SealedHeader::seal_slow(header);
    if sealed.hash() != head_hash {
        return Err(eyre!(
            "head probe header hash mismatch: requested {head_hash}, got {}",
            sealed.hash()
        ));
    }
    Ok(sealed.header().number)
}

async fn request_headers_by_number<C: ChainTypes>(
    peer_id: PeerId,
    start_block: u64,
    limit: usize,
    messages: &PeerRequestSender<PeerRequest<C::Net>>,
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

async fn request_headers_by_hash<C: ChainTypes>(
    peer_id: PeerId,
    hash: B256,
    messages: &PeerRequestSender<PeerRequest<C::Net>>,
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

async fn request_bodies<C: ChainTypes>(
    peer: &NetworkPeer<C>,
    hashes: &[B256],
) -> Result<Vec<BlockBody<C::SignedTx>>> {
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

async fn request_receipts_legacy<C: ChainTypes>(
    peer: &NetworkPeer<C>,
    hashes: &[B256],
) -> Result<Vec<Vec<C::Receipt>>> {
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

async fn request_receipts69<C: ChainTypes>(
    peer: &NetworkPeer<C>,
    hashes: &[B256],
) -> Result<Vec<Vec<C::Receipt>>> {
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

async fn request_receipts70<C: ChainTypes>(
    peer: &NetworkPeer<C>,
    hashes: &[B256],
) -> Result<Vec<Vec<C::Receipt>>> {
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
pub async fn request_headers_batch<C: ChainTypes>(
    peer: &NetworkPeer<C>,
    start_block: u64,
    limit: usize,
) -> Result<Vec<Header>> {
    request_headers_by_number::<C>(peer.peer_id, start_block, limit, &peer.messages).await
}

/// Fetch a consecutive run of headers, transparently chunking requests.
///
/// Returns whatever the peer served (possibly short); callers validate.
///
/// # Errors
///
/// Returns an error if any underlying request fails.
pub async fn request_headers_chunked<C: ChainTypes>(
    peer: &NetworkPeer<C>,
    start_block: u64,
    count: usize,
) -> Result<Vec<Header>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let mut headers = Vec::with_capacity(count);
    let mut current = start_block;
    let mut remaining = count;
    while remaining > 0 {
        let batch = remaining.min(MAX_HEADERS_PER_REQUEST);
        let mut batch_headers = request_headers_batch(peer, current, batch).await?;
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
    Ok(headers)
}

/// Fetch receipts for blocks identified by hash, using the peer's ETH version.
///
/// # Errors
///
/// Returns an error if the P2P request times out or the peer disconnects.
pub async fn request_receipts<C: ChainTypes>(
    peer: &NetworkPeer<C>,
    hashes: &[B256],
) -> Result<Vec<Vec<C::Receipt>>> {
    match peer.eth_version {
        EthVersion::Eth70 => request_receipts70(peer, hashes).await,
        EthVersion::Eth69 => request_receipts69(peer, hashes).await,
        _ => request_receipts_legacy(peer, hashes).await,
    }
}

async fn request_bodies_chunked_partial_with_stats<C: ChainTypes>(
    peer: &NetworkPeer<C>,
    hashes: &[B256],
) -> Result<ChunkedResponse<BlockBody<C::SignedTx>>> {
    if hashes.is_empty() {
        return Ok(ChunkedResponse {
            results: Vec::new(),
            requests: 0,
        });
    }

    let mut results: Vec<Option<BlockBody<C::SignedTx>>> = vec![None; hashes.len()];
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

async fn request_receipts_chunked_partial_with_stats<C: ChainTypes>(
    peer: &NetworkPeer<C>,
    hashes: &[B256],
) -> Result<ChunkedResponse<Vec<C::Receipt>>> {
    if hashes.is_empty() {
        return Ok(ChunkedResponse {
            results: Vec::new(),
            requests: 0,
        });
    }

    let mut results: Vec<Option<Vec<C::Receipt>>> = vec![None; hashes.len()];
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

/// Validate a fetched body and receipts against the block header.
///
/// Recomputes the transaction, ommers, withdrawals, and receipt roots from
/// the fetched data and compares them to the header commitments. Receipt
/// blooms are recomputed from logs, never trusted from the peer.
fn validate_payload<C: ChainTypes>(
    header: &Header,
    body: &BlockBody<C::SignedTx>,
    receipts: &[C::Receipt],
) -> Result<(), &'static str> {
    if body.transactions.len() != receipts.len() {
        return Err("transaction/receipt count mismatch");
    }
    if proofs::calculate_transaction_root(&body.transactions) != header.transactions_root {
        return Err("transactions root mismatch");
    }
    if proofs::calculate_ommers_root(&body.ommers) != header.ommers_hash {
        return Err("ommers root mismatch");
    }
    if !C::withdrawals_valid(header, body) {
        return Err("withdrawals mismatch");
    }
    if C::receipts_root(receipts, header) != header.receipts_root {
        return Err("receipts root mismatch");
    }
    Ok(())
}

/// Fetch bodies and receipts for a set of headers from a peer.
///
/// Headers must already be fetched and sealed; this uses their hashes to
/// request the corresponding bodies and receipts in parallel, then validates
/// each payload against the header commitments before returning it.
pub async fn fetch_payloads_for_headers<C: ChainTypes>(
    peer: &NetworkPeer<C>,
    ordered_headers: Vec<SealedHeader>,
) -> Result<PayloadFetchOutcome<C>> {
    if ordered_headers.is_empty() {
        return Ok(PayloadFetchOutcome {
            payloads: Vec::new(),
            missing_blocks: Vec::new(),
            fetch_stats: FetchStageStats::default(),
        });
    }

    let hashes: Vec<B256> = ordered_headers.iter().map(SealedHeader::hash).collect();

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
    for (idx, sealed) in ordered_headers.into_iter().enumerate() {
        let number = sealed.header().number;
        let body = bodies.get_mut(idx).and_then(Option::take);
        let block_receipts = receipts.get_mut(idx).and_then(Option::take);

        match (body, block_receipts) {
            (Some(body), Some(block_receipts)) => {
                if let Err(reason) = validate_payload::<C>(sealed.header(), &body, &block_receipts)
                {
                    debug!(
                        peer_id = ?peer.peer_id,
                        block = number,
                        reason,
                        "payload validation failed; dropping block"
                    );
                    missing_blocks.push(number);
                    continue;
                }
                payloads.push(BlockPayload::new(
                    sealed.into_header(),
                    body,
                    block_receipts,
                ));
            }
            _ => {
                missing_blocks.push(number);
            }
        }
    }

    Ok(PayloadFetchOutcome {
        payloads,
        missing_blocks,
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

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::validate_payload;
    use crate::chain::EthereumChain;
    use crate::test_utils::{build_test_transaction, make_log, make_receipt};
    use alloy_consensus::proofs;
    use alloy_primitives::{Address, Bytes, B256};
    use reth_ethereum_primitives::{BlockBody, Receipt};
    use reth_primitives_traits::Header;

    fn validate(
        header: &Header,
        body: &BlockBody,
        receipts: &[Receipt],
    ) -> Result<(), &'static str> {
        validate_payload::<EthereumChain>(header, body, receipts)
    }

    /// Build a header whose roots match the given body and receipts.
    fn consistent_header_for(
        number: u64,
        parent_hash: B256,
        body: &BlockBody,
        receipts: &[Receipt],
    ) -> Header {
        Header {
            number,
            parent_hash,
            transactions_root: proofs::calculate_transaction_root(&body.transactions),
            ommers_hash: proofs::calculate_ommers_root(&body.ommers),
            withdrawals_root: body.calculate_withdrawals_root(),
            receipts_root: Receipt::calculate_receipt_root_no_memo(receipts),
            ..Default::default()
        }
    }

    /// Build a header whose roots match an empty body and empty receipts.
    fn consistent_header(number: u64, parent_hash: B256) -> Header {
        consistent_header_for(number, parent_hash, &BlockBody::default(), &[])
    }

    #[test]
    fn validate_payload_accepts_consistent_empty_payload() {
        let header = consistent_header(1, B256::ZERO);
        let body = BlockBody::default();
        assert!(validate(&header, &body, &[]).is_ok());
    }

    #[test]
    fn validate_payload_accepts_payload_with_tx_and_logs() {
        let body = BlockBody {
            transactions: vec![build_test_transaction()],
            ..Default::default()
        };
        let log = make_log(
            Address::repeat_byte(0x11),
            vec![B256::repeat_byte(0x22)],
            Bytes::from_static(&[0x01]),
        );
        let receipts = vec![make_receipt(vec![log])];
        let header = consistent_header_for(1, B256::ZERO, &body, &receipts);
        assert!(validate(&header, &body, &receipts).is_ok());
    }

    #[test]
    fn validate_payload_rejects_tampered_log() {
        let body = BlockBody {
            transactions: vec![build_test_transaction()],
            ..Default::default()
        };
        let log = make_log(
            Address::repeat_byte(0x11),
            vec![B256::repeat_byte(0x22)],
            Bytes::from_static(&[0x01]),
        );
        let header = consistent_header_for(1, B256::ZERO, &body, &[make_receipt(vec![log])]);

        // Peer swaps in a receipt with a fabricated log for the same tx.
        let forged_log = make_log(
            Address::repeat_byte(0x33),
            vec![B256::repeat_byte(0x44)],
            Bytes::from_static(&[0x02]),
        );
        let forged_receipts = vec![make_receipt(vec![forged_log])];
        let result = validate(&header, &body, &forged_receipts);
        assert_eq!(result, Err("receipts root mismatch"));
    }

    #[test]
    fn validate_payload_rejects_wrong_tx_root() {
        let header = Header {
            transactions_root: B256::ZERO,
            ..consistent_header(1, B256::ZERO)
        };
        let body = BlockBody::default();
        let result = validate(&header, &body, &[]);
        assert_eq!(result, Err("transactions root mismatch"));
    }

    #[test]
    fn validate_payload_rejects_count_mismatch() {
        let header = consistent_header(1, B256::ZERO);
        let body = BlockBody::default();
        let receipts = vec![make_receipt(vec![])];
        let result = validate(&header, &body, &receipts);
        assert_eq!(result, Err("transaction/receipt count mismatch"));
    }

    #[test]
    fn validate_payload_rejects_wrong_receipts_root() {
        let header = Header {
            receipts_root: B256::ZERO,
            ..consistent_header(1, B256::ZERO)
        };
        let body = BlockBody::default();
        let result = validate(&header, &body, &[]);
        assert_eq!(result, Err("receipts root mismatch"));
    }

    #[test]
    fn validate_payload_rejects_wrong_ommers_root() {
        let header = Header {
            ommers_hash: B256::ZERO,
            ..consistent_header(1, B256::ZERO)
        };
        let body = BlockBody::default();
        let result = validate(&header, &body, &[]);
        assert_eq!(result, Err("ommers root mismatch"));
    }

    #[test]
    fn validate_payload_rejects_wrong_withdrawals_root() {
        let header = Header {
            withdrawals_root: Some(B256::ZERO),
            ..consistent_header(1, B256::ZERO)
        };
        let body = BlockBody::default();
        let result = validate(&header, &body, &[]);
        assert_eq!(result, Err("withdrawals mismatch"));
    }
}
