//! Sync engine — orchestrates fetching blocks from peers.
//!
//! Peer feeder, ready set deduplication, quality-based peer selection,
//! and JoinSet task tracking.

use crate::config::IndexConfig;
use crate::p2p::{NetworkPeer, PeerPool};
use crate::sync::fetch::{run_fetch_task, FetchTaskContext, FetchTaskParams};
use crate::sync::scheduler::{
    PeerHealthConfig, PeerHealthTracker, PeerWorkScheduler, SchedulerConfig,
};
use crate::sync::BlockPayload;
use crate::{decode, filter};
use reth_network_api::PeerId;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};
use tracing::{debug, info, warn};

const MAX_CONCURRENT_FETCHES: usize = 32;
const PAYLOAD_CHANNEL_SIZE: usize = 256;

/// Outcome of a sync run.
pub struct SyncOutcome {
    pub blocks_fetched: u64,
    pub total_receipts: u64,
    pub events_matched: u64,
    pub events_decoded: u64,
    pub elapsed: Duration,
}

/// Run sync for a block range, fetching from the peer pool.
///
/// # Errors
///
/// Returns an error if the sync encounters an unrecoverable failure.
pub async fn run_sync(
    pool: Arc<PeerPool>,
    start_block: u64,
    end_block: u64,
    config: Arc<IndexConfig>,
) -> eyre::Result<SyncOutcome> {
    let started = Instant::now();
    let total_blocks = end_block.saturating_sub(start_block) + 1;

    info!(start_block, end_block, total_blocks, "starting sync");

    // Setup scheduler
    let sched_config = SchedulerConfig::default();
    let peer_health_config = PeerHealthConfig::from_scheduler_config(&sched_config);
    let peer_health = Arc::new(PeerHealthTracker::new(peer_health_config));
    let blocks: Vec<u64> = (start_block..=end_block).collect();
    let scheduler = Arc::new(PeerWorkScheduler::new_with_health(
        sched_config,
        blocks,
        Arc::clone(&peer_health),
    ));

    // Channels
    let (payload_tx, payload_rx) = mpsc::channel::<BlockPayload>(PAYLOAD_CHANNEL_SIZE);
    let (ready_tx, ready_rx) = mpsc::unbounded_channel::<NetworkPeer>();

    // Shutdown signal for peer feeder
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Spawn peer feeder
    let feeder_handle = spawn_peer_feeder(
        Arc::clone(&pool),
        ready_tx.clone(),
        shutdown_rx,
    );

    // Spawn payload consumer
    let consumer_handle = tokio::spawn(consume_payloads(payload_rx, config));

    // Main fetch loop
    let ctx = FetchLoopContext {
        scheduler: &scheduler,
        peer_health: &peer_health,
        pool: &pool,
        start_block,
        end_block,
        payload_tx: &payload_tx,
        ready_tx: &ready_tx,
    };
    run_fetch_loop(&ctx, ready_rx).await;

    // Shutdown
    let _ = shutdown_tx.send(true);
    let _ = feeder_handle.await;
    drop(payload_tx);

    let (blocks_fetched, total_receipts, events_matched, events_decoded) = consumer_handle
        .await
        .map_err(|e| eyre::eyre!("consumer task failed: {e}"))?;

    let elapsed = started.elapsed();
    Ok(SyncOutcome {
        blocks_fetched,
        total_receipts,
        events_matched,
        events_decoded,
        elapsed,
    })
}

// ── Peer feeder ──────────────────────────────────────────────────────

fn spawn_peer_feeder(
    pool: Arc<PeerPool>,
    ready_tx: mpsc::UnboundedSender<NetworkPeer>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut known: HashSet<PeerId> = HashSet::new();

        // Seed any already-connected peers immediately.
        for peer in pool.snapshot() {
            if known.insert(peer.peer_id) {
                let _ = ready_tx.send(peer);
            }
        }

        let mut ticker = tokio::time::interval(Duration::from_millis(200));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    for peer in pool.snapshot() {
                        if known.insert(peer.peer_id) {
                            let _ = ready_tx.send(peer);
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
    })
}

// ── Main fetch loop ──────────────────────────────────────────────────

/// Shared references for the fetch loop (reduces argument counts).
struct FetchLoopContext<'a> {
    scheduler: &'a Arc<PeerWorkScheduler>,
    peer_health: &'a Arc<PeerHealthTracker>,
    pool: &'a Arc<PeerPool>,
    start_block: u64,
    end_block: u64,
    payload_tx: &'a mpsc::Sender<BlockPayload>,
    ready_tx: &'a mpsc::UnboundedSender<NetworkPeer>,
}

async fn run_fetch_loop(
    ctx: &FetchLoopContext<'_>,
    mut ready_rx: mpsc::UnboundedReceiver<NetworkPeer>,
) {
    let fetch_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_FETCHES));
    let mut fetch_tasks: JoinSet<()> = JoinSet::new();
    let mut ready_peers: Vec<NetworkPeer> = Vec::new();
    let mut ready_set: HashSet<PeerId> = HashSet::new();
    let mut last_progress_check = Instant::now();
    let mut last_progress_completed = 0u64;

    loop {
        // Drain ready peers, refreshing head from pool
        drain_ready_peers(&mut ready_rx, ctx.pool, &mut ready_peers, &mut ready_set);

        if ready_peers.is_empty() {
            // Block until at least one peer arrives
            let Some(mut peer) = ready_rx.recv().await else {
                break;
            };
            if let Some(h) = ctx.pool.get_peer_head(peer.peer_id) {
                peer.head_number = h;
            }
            if ready_set.insert(peer.peer_id) {
                ready_peers.push(peer);
            }
            continue;
        }

        // Reap completed fetch tasks
        while fetch_tasks.try_join_next().is_some() {}

        // Progress check every 30s (stall detection)
        check_progress(
            ctx.scheduler,
            &mut last_progress_check,
            &mut last_progress_completed,
            ready_peers.len(),
        )
        .await;

        if ctx.scheduler.is_done().await {
            debug!("scheduler: all work complete");
            break;
        }

        // Try to acquire a semaphore permit (non-blocking)
        let Ok(permit) = fetch_semaphore.clone().try_acquire_owned() else {
            sleep(Duration::from_millis(10)).await;
            continue;
        };

        // Pick best peer and try to dispatch a fetch task
        dispatch_best_peer(
            ctx,
            &mut ready_peers,
            &mut ready_set,
            &mut fetch_tasks,
            permit,
        )
        .await;
    }

    // Drain remaining fetch tasks
    while fetch_tasks.join_next().await.is_some() {}
}

/// Pick the best peer, check health, get a batch, and spawn a fetch task.
async fn dispatch_best_peer(
    ctx: &FetchLoopContext<'_>,
    ready_peers: &mut Vec<NetworkPeer>,
    ready_set: &mut HashSet<PeerId>,
    fetch_tasks: &mut JoinSet<()>,
    permit: tokio::sync::OwnedSemaphorePermit,
) {
    // Pick best peer by quality score
    let best_idx = pick_best_ready_peer_index(ready_peers, ctx.peer_health).await;
    let mut peer = ready_peers.swap_remove(best_idx);
    ready_set.remove(&peer.peer_id);

    // Refresh head from pool (picks up re-probe updates)
    if let Some(h) = ctx.pool.get_peer_head(peer.peer_id) {
        peer.head_number = h;
    }

    // Pre-flight: cooldown and stale-head checks
    if let Some(action) = check_peer_eligibility(ctx, &peer).await {
        drop(permit);
        match action {
            PeerAction::Recycle(delay) => recycle_peer(ctx.ready_tx, peer, delay),
            PeerAction::RecycleImmediate => {
                let _ = ctx.ready_tx.send(peer);
            }
            PeerAction::Drop => {}
        }
        return;
    }

    // Head cap: use peer's head, or end_block if unprobed
    let head_cap = if peer.head_number == 0 {
        ctx.end_block
    } else {
        peer.head_number
    };

    let batch = ctx
        .scheduler
        .next_batch_for_peer(peer.peer_id, head_cap)
        .await;
    if batch.blocks.is_empty() {
        drop(permit);
        recycle_peer(ctx.ready_tx, peer, 50);
        return;
    }

    let block_count = batch.blocks.len();
    ctx.peer_health
        .record_assignment(peer.peer_id, block_count)
        .await;

    debug!(
        peer_id = ?peer.peer_id,
        blocks = block_count,
        range_start = batch.blocks.first().copied().unwrap_or(0),
        range_end = batch.blocks.last().copied().unwrap_or(0),
        mode = ?batch.mode,
        head_cap,
        "assigned batch"
    );

    let task_ctx = FetchTaskContext {
        scheduler: Arc::clone(ctx.scheduler),
        peer_health: Arc::clone(ctx.peer_health),
        payload_tx: ctx.payload_tx.clone(),
        ready_tx: ctx.ready_tx.clone(),
    };
    let params = FetchTaskParams {
        peer,
        blocks: batch.blocks,
        mode: batch.mode,
        permit,
    };

    fetch_tasks.spawn(run_fetch_task(task_ctx, params));
}

/// What to do with an ineligible peer.
enum PeerAction {
    /// Recycle with a delayed re-send.
    Recycle(u64),
    /// Send back to ready channel immediately (cooldown prevents re-assignment).
    RecycleImmediate,
    /// Drop the peer entirely (too stale to be useful).
    Drop,
}

/// Check if a peer is eligible for dispatch. Returns `None` if eligible,
/// or `Some(action)` if the peer should be skipped.
async fn check_peer_eligibility(
    ctx: &FetchLoopContext<'_>,
    peer: &NetworkPeer,
) -> Option<PeerAction> {
    // Cooling-down peers
    if ctx.peer_health.is_peer_cooling_down(peer.peer_id).await {
        return Some(PeerAction::Recycle(500));
    }

    // Stale-head detection: peer's probed head is below our work range
    if peer.head_number > 0 && peer.head_number < ctx.start_block {
        let gap = ctx.start_block.saturating_sub(peer.head_number);

        // Peers more than 10k blocks behind are useless — drop entirely
        if gap > 10_000 {
            debug!(
                peer_id = ?peer.peer_id,
                peer_head = peer.head_number,
                gap,
                "dropping truly stale peer"
            );
            return Some(PeerAction::Drop);
        }

        debug!(
            peer_id = ?peer.peer_id,
            peer_head = peer.head_number,
            start_block = ctx.start_block,
            "peer head below work range, cooling down for 120s"
        );
        ctx.peer_health
            .set_stale_head_cooldown(peer.peer_id, Duration::from_secs(120))
            .await;
        return Some(PeerAction::RecycleImmediate);
    }

    None
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Non-blocking drain of ready channel, refreshing peer heads from pool.
fn drain_ready_peers(
    ready_rx: &mut mpsc::UnboundedReceiver<NetworkPeer>,
    pool: &PeerPool,
    ready_peers: &mut Vec<NetworkPeer>,
    ready_set: &mut HashSet<PeerId>,
) {
    while let Ok(mut peer) = ready_rx.try_recv() {
        if let Some(h) = pool.get_peer_head(peer.peer_id) {
            peer.head_number = h;
        }
        if ready_set.insert(peer.peer_id) {
            ready_peers.push(peer);
        }
    }
}

/// Pick the best peer by quality score.
async fn pick_best_ready_peer_index(
    peers: &[NetworkPeer],
    peer_health: &PeerHealthTracker,
) -> usize {
    let mut best_idx = 0usize;
    let mut best_score = f64::NEG_INFINITY;
    let mut best_samples = 0u64;
    for (idx, peer) in peers.iter().enumerate() {
        let quality = peer_health.quality(peer.peer_id).await;
        // Exact equality for tie-breaking: prefer peer with more samples
        #[expect(clippy::float_cmp, reason = "exact equality needed for tie-breaking")]
        if quality.score > best_score
            || (quality.score == best_score && quality.samples > best_samples)
        {
            best_idx = idx;
            best_score = quality.score;
            best_samples = quality.samples;
        }
    }
    best_idx
}

fn recycle_peer(
    ready_tx: &mpsc::UnboundedSender<NetworkPeer>,
    peer: NetworkPeer,
    delay_ms: u64,
) {
    let tx = ready_tx.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(delay_ms)).await;
        let _ = tx.send(peer);
    });
}

async fn check_progress(
    scheduler: &PeerWorkScheduler,
    last_check: &mut Instant,
    last_completed: &mut u64,
    ready_count: usize,
) {
    if last_check.elapsed() < Duration::from_secs(30) {
        return;
    }
    let current_completed = scheduler.completed_count().await as u64;
    let pending = scheduler.pending_count().await;
    let inflight = scheduler.inflight_count().await;
    let escalation = scheduler.escalation_len().await;

    if current_completed == *last_completed && (pending > 0 || escalation > 0) {
        warn!(
            completed = current_completed,
            pending,
            inflight,
            escalation,
            ready_count,
            "stall detected: no progress in 30s"
        );
    } else {
        debug!(
            completed = current_completed,
            delta = current_completed.saturating_sub(*last_completed),
            pending,
            inflight,
            escalation,
            ready_count,
            "progress check"
        );
    }
    *last_completed = current_completed;
    *last_check = Instant::now();
}

async fn consume_payloads(
    mut payload_rx: mpsc::Receiver<BlockPayload>,
    config: Arc<IndexConfig>,
) -> (u64, u64, u64, u64) {
    let mut blocks_fetched = 0u64;
    let mut total_receipts = 0u64;
    let mut events_matched = 0u64;
    let mut events_decoded = 0u64;
    let mut last_log = Instant::now();

    while let Some(payload) = payload_rx.recv().await {
        blocks_fetched = blocks_fetched.saturating_add(1);
        total_receipts = total_receipts.saturating_add(payload.receipts.len() as u64);

        let (matched, decoded) = process_block_events(&payload, &config);
        events_matched = events_matched.saturating_add(matched);
        events_decoded = events_decoded.saturating_add(decoded);

        if last_log.elapsed() >= Duration::from_secs(2) {
            info!(
                blocks_fetched,
                total_receipts,
                events_matched,
                events_decoded,
                block_number = payload.header.number,
                "sync progress"
            );
            last_log = Instant::now();
        }
    }

    (blocks_fetched, total_receipts, events_matched, events_decoded)
}

/// Filter and decode events from a single block payload.
/// Returns (matched_count, decoded_count).
fn process_block_events(payload: &BlockPayload, config: &IndexConfig) -> (u64, u64) {
    let matched = filter::filter_block(payload, config);
    let matched_count = matched.len() as u64;
    let mut decoded_count = 0u64;

    for filtered_log in &matched {
        let Some(contract) = config.contract_for_address(&filtered_log.log.address) else {
            continue;
        };
        match decode::decode_log(filtered_log, contract) {
            Ok(decoded) => {
                decoded_count = decoded_count.saturating_add(1);
                debug!("{decoded}");
            }
            Err(e) => {
                warn!(
                    block = filtered_log.block_number,
                    tx_index = filtered_log.tx_index,
                    error = %e,
                    "failed to decode log"
                );
            }
        }
    }

    (matched_count, decoded_count)
}
