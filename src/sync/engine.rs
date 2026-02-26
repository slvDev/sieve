//! Sync engine — orchestrates fetching blocks from peers.
//!
//! Peer feeder, ready set deduplication, quality-based peer selection,
//! and JoinSet task tracking.

use crate::config::IndexConfig;
use crate::db::{self, Database};
use crate::handler::HandlerRegistry;
use crate::p2p::{NetworkPeer, PeerPool};
use crate::sync::fetch::{run_fetch_task, FetchTaskContext, FetchTaskParams};
use crate::sync::scheduler::{
    PeerHealthConfig, PeerHealthTracker, PeerWorkScheduler, SchedulerConfig,
};
use crate::sync::BlockPayload;
use crate::{decode, filter};

use eyre::WrapErr;
use reth_network_api::PeerId;
use reth_primitives_traits::SealedHeader;
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
#[derive(Debug)]
pub struct SyncOutcome {
    pub blocks_fetched: u64,
    pub total_receipts: u64,
    pub events_matched: u64,
    pub events_decoded: u64,
    pub events_stored: u64,
    pub elapsed: Duration,
}

/// Accumulated stats from the payload consumer.
#[derive(Debug, Default)]
struct ConsumerStats {
    blocks_fetched: u64,
    total_receipts: u64,
    events_matched: u64,
    events_decoded: u64,
    events_stored: u64,
}

/// Run sync for a block range, fetching from the peer pool.
///
/// The `stop_rx` watch channel allows external callers (follow loop, shutdown
/// handler) to signal an early stop. When `*stop_rx.borrow() == true`, the
/// fetch loop exits at the next iteration boundary.
///
/// # Errors
///
/// Returns an error if the sync encounters an unrecoverable failure.
pub async fn run_sync(
    pool: Arc<PeerPool>,
    start_block: u64,
    end_block: u64,
    config: Arc<IndexConfig>,
    db: Arc<Database>,
    handlers: Arc<HandlerRegistry>,
    stop_rx: watch::Receiver<bool>,
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

    // Local shutdown signal for the peer feeder (triggered when fetch loop exits)
    let (feeder_shutdown_tx, feeder_shutdown_rx) = watch::channel(false);

    // Spawn peer feeder
    let feeder_handle = spawn_peer_feeder(
        Arc::clone(&pool),
        ready_tx.clone(),
        feeder_shutdown_rx,
    );

    // Spawn payload consumer
    let consumer_handle = tokio::spawn(consume_payloads(payload_rx, config, db, handlers));

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
    run_fetch_loop(&ctx, ready_rx, &stop_rx).await;

    // Shutdown peer feeder and consumer
    let _ = feeder_shutdown_tx.send(true);
    let _ = feeder_handle.await;
    drop(payload_tx);

    let stats = consumer_handle
        .await
        .wrap_err("consumer task failed")?;

    let elapsed = started.elapsed();
    Ok(SyncOutcome {
        blocks_fetched: stats.blocks_fetched,
        total_receipts: stats.total_receipts,
        events_matched: stats.events_matched,
        events_decoded: stats.events_decoded,
        events_stored: stats.events_stored,
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
    stop_rx: &watch::Receiver<bool>,
) {
    let fetch_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_FETCHES));
    let mut fetch_tasks: JoinSet<()> = JoinSet::new();
    let mut ready_peers: Vec<NetworkPeer> = Vec::new();
    let mut ready_set: HashSet<PeerId> = HashSet::new();
    let mut last_progress_check = Instant::now();
    let mut last_progress_completed = 0u64;

    loop {
        // Check for external shutdown signal
        if *stop_rx.borrow() {
            debug!("fetch loop: stop signal received");
            break;
        }

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
    db: Arc<Database>,
    handlers: Arc<HandlerRegistry>,
) -> ConsumerStats {
    let mut stats = ConsumerStats::default();
    let mut last_log = Instant::now();

    let ctx = ProcessContext {
        config: &config,
        db: &db,
        handlers: &handlers,
    };

    while let Some(payload) = payload_rx.recv().await {
        stats.blocks_fetched = stats.blocks_fetched.saturating_add(1);
        stats.total_receipts = stats
            .total_receipts
            .saturating_add(payload.receipts.len() as u64);

        match process_block_events(&payload, &ctx).await {
            Ok((matched, decoded, stored)) => {
                stats.events_matched = stats.events_matched.saturating_add(matched);
                stats.events_decoded = stats.events_decoded.saturating_add(decoded);
                stats.events_stored = stats.events_stored.saturating_add(stored);
            }
            Err(e) => {
                warn!(
                    block = payload.header.number,
                    error = %e,
                    "failed to process block events"
                );
            }
        }

        if last_log.elapsed() >= Duration::from_secs(2) {
            info!(
                blocks_fetched = stats.blocks_fetched,
                total_receipts = stats.total_receipts,
                events_matched = stats.events_matched,
                events_decoded = stats.events_decoded,
                events_stored = stats.events_stored,
                block_number = payload.header.number,
                "sync progress"
            );
            last_log = Instant::now();
        }
    }

    stats
}

/// Shared references for the payload consumer (reduces argument counts).
struct ProcessContext<'a> {
    config: &'a IndexConfig,
    db: &'a Database,
    handlers: &'a HandlerRegistry,
}

/// Compute the sealed hash for a block header.
fn compute_block_hash(header: &reth_primitives_traits::Header) -> alloy_primitives::B256 {
    SealedHeader::seal_slow(header.clone()).hash()
}

/// Commit block hash and checkpoint for a block with no events to store.
async fn commit_block_bookkeeping(
    db_ref: &Database,
    block_number: u64,
    block_hash: &[u8],
) -> eyre::Result<()> {
    let mut tx = db_ref.begin().await?;
    db::store_block_hash(&mut tx, block_number, block_hash).await?;
    db::update_checkpoint(&mut tx, block_number).await?;
    tx.commit()
        .await
        .wrap_err_with(|| format!("failed to commit block {block_number}"))?;
    Ok(())
}

/// Filter, decode, and store events from a single block payload.
/// Returns (matched_count, decoded_count, stored_count).
async fn process_block_events(
    payload: &BlockPayload,
    ctx: &ProcessContext<'_>,
) -> eyre::Result<(u64, u64, u64)> {
    let block_hash = compute_block_hash(&payload.header);

    // Step 1: filter
    let matched = filter::filter_block(payload, ctx.config);
    let matched_count = matched.len() as u64;

    if matched.is_empty() {
        commit_block_bookkeeping(ctx.db, payload.header.number, block_hash.as_slice()).await?;
        return Ok((0, 0, 0));
    }

    // Step 2: decode (CPU work, no DB)
    let decoded_events = decode_matched_logs(&matched, ctx.config);
    let decoded_count = decoded_events.len() as u64;

    if decoded_events.is_empty() {
        commit_block_bookkeeping(ctx.db, payload.header.number, block_hash.as_slice()).await?;
        return Ok((matched_count, 0, 0));
    }

    // Step 3: store in one transaction per block
    let mut tx = ctx.db.begin().await?;
    let mut stored_count = 0u64;

    db::store_block_hash(&mut tx, payload.header.number, block_hash.as_slice()).await?;

    for event in &decoded_events {
        let dispatched = ctx.handlers.dispatch(event, &mut tx).await?;
        stored_count = stored_count.saturating_add(dispatched);
    }

    db::update_checkpoint(&mut tx, payload.header.number).await?;

    tx.commit()
        .await
        .wrap_err_with(|| format!("failed to commit block {}", payload.header.number))?;

    Ok((matched_count, decoded_count, stored_count))
}

/// Decode matched logs into events, logging any decode failures.
fn decode_matched_logs(
    matched: &[filter::FilteredLog],
    config: &crate::config::IndexConfig,
) -> Vec<decode::DecodedEvent> {
    let mut decoded_events = Vec::new();
    for filtered_log in matched {
        let Some(contract) = config.contract_for_address(&filtered_log.log.address) else {
            continue;
        };
        match decode::decode_log(filtered_log, contract) {
            Ok(decoded) => {
                debug!("{decoded}");
                decoded_events.push(decoded);
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
    decoded_events
}
