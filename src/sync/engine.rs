//! Sync engine — orchestrates fetching blocks from peers.
//!
//! Peer feeder, ready set deduplication, quality-based peer selection,
//! and JoinSet task tracking.

use crate::chain::ChainTypes;
use crate::config::IndexConfig;
use crate::config::Selector;
use crate::db::{self, Database};
use crate::decode::DecodedParam;
use crate::handler::{
    CallRegistry, DecodedCall, EventContext, HandlerRegistry, NativeTransfer, TransferRegistry,
};
use crate::metrics::SieveMetrics;
use crate::p2p::{NetworkPeer, PeerPool};
use crate::sync::fetch::{run_fetch_task, FetchTaskContext, FetchTaskParams};
use crate::sync::scheduler::{
    PeerHealthConfig, PeerHealthTracker, PeerWorkScheduler, SchedulerConfig,
};
use crate::sync::{BlockPayload, FetchItem, SkippedHeader, SyncContext};
use crate::toml_config::ResolvedFactory;
use crate::types::{BlockNumber, TxIndex};
use crate::{decode, filter};

use alloy_consensus::transaction::SignerRecoverable;
use alloy_consensus::transaction::TxHashRef;
use alloy_consensus::Transaction;
use alloy_consensus::TxReceipt;
use alloy_dyn_abi::JsonAbiExt;
use alloy_primitives::{Address, TxKind, B256};
use eyre::WrapErr;
use prometheus_client::metrics::gauge::Gauge;
use reth_network_api::PeerId;
use reth_primitives_traits::SealedHeader;
use sqlx::Postgres;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};
use tracing::{debug, info, instrument, warn};

const MAX_CONCURRENT_FETCHES: usize = 32;
const PAYLOAD_CHANNEL_SIZE: usize = 8192;

/// Maximum number of blocks to batch in a single DB transaction.
const BATCH_SIZE: usize = 256;

/// Channel buffer between processing workers and the DB writer.
const PROCESSED_CHANNEL_SIZE: usize = 8192;

/// How long to wait for more payloads before flushing a partial batch.
const BATCH_FLUSH_TIMEOUT: Duration = Duration::from_millis(500);

/// RAII guard that increments an atomic counter and a Prometheus gauge
/// on creation, and decrements both on drop.
struct ActiveTaskGuard {
    counter: Arc<AtomicUsize>,
    gauge: Gauge,
}

impl ActiveTaskGuard {
    fn new(counter: &Arc<AtomicUsize>, gauge: Gauge) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        gauge.inc();
        Self {
            counter: Arc::clone(counter),
            gauge,
        }
    }
}

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
        self.gauge.dec();
    }
}

/// Outcome of a sync run.
#[derive(Debug, Default)]
pub struct SyncOutcome {
    pub blocks_fetched: u64,
    pub total_receipts: u64,
    pub events_matched: u64,
    pub events_decoded: u64,
    pub events_stored: u64,
    pub transfers_stored: u64,
    pub calls_stored: u64,
    pub elapsed: Duration,
}

impl SyncOutcome {
    /// Fold another outcome into this one (used by windowed backfills).
    pub const fn accumulate(&mut self, other: &Self) {
        self.blocks_fetched = self.blocks_fetched.saturating_add(other.blocks_fetched);
        self.total_receipts = self.total_receipts.saturating_add(other.total_receipts);
        self.events_matched = self.events_matched.saturating_add(other.events_matched);
        self.events_decoded = self.events_decoded.saturating_add(other.events_decoded);
        self.events_stored = self.events_stored.saturating_add(other.events_stored);
        self.transfers_stored = self.transfers_stored.saturating_add(other.transfers_stored);
        self.calls_stored = self.calls_stored.saturating_add(other.calls_stored);
        self.elapsed = self.elapsed.saturating_add(other.elapsed);
    }
}

/// Accumulated stats from the payload consumer.
#[derive(Debug, Default)]
struct ConsumerStats {
    blocks_fetched: u64,
    total_receipts: u64,
    events_matched: u64,
    events_decoded: u64,
    events_stored: u64,
    transfers_stored: u64,
    calls_stored: u64,
}

/// Outcome of processing events from a single block.
#[derive(Debug)]
struct ProcessOutcome {
    block_number: u64,
    block_timestamp: u64,
    matched: u64,
    decoded: u64,
    stored: u64,
    transfers: u64,
    calls: u64,
    /// Per-table insert counts: `(table_name, event_name, count)`.
    table_counts: Vec<(String, String, u64)>,
    /// Per-event decoded payloads for streaming (only populated when streams are configured).
    event_payloads: Vec<crate::stream::EventPayload>,
}

/// CPU-processed block ready for DB storage.
///
/// Created by [`prepare_block`], consumed by [`flush_batch`].
/// Keeps the original [`BlockPayload`] for transfer/call scanning.
struct ProcessedBlock<C: ChainTypes> {
    block_number: BlockNumber,
    block_hash: B256,
    factory_discoveries: Vec<filter::FactoryDiscovery>,
    decoded_events: Vec<decode::DecodedEvent>,
    matched_count: u64,
    receipt_count: u64,
    payload: BlockPayload<C>,
}

/// Item flowing from the processing workers to the DB writer.
enum ProcessedItem<C: ChainTypes> {
    /// Fully processed block ready for storage.
    Block(Box<ProcessedBlock<C>>),
    /// Bloom-skipped block: hash record only.
    Skipped(SkippedHeader),
}

impl<C: ChainTypes> ProcessedItem<C> {
    const fn number(&self) -> u64 {
        match self {
            Self::Block(block) => block.block_number.as_u64(),
            Self::Skipped(skipped) => skipped.number,
        }
    }

    const fn hash(&self) -> B256 {
        match self {
            Self::Block(block) => block.block_hash,
            Self::Skipped(skipped) => skipped.hash,
        }
    }

    const fn parent_hash(&self) -> B256 {
        match self {
            Self::Block(block) => block.payload.header().parent_hash,
            Self::Skipped(skipped) => skipped.parent_hash,
        }
    }
}

/// Reorders out-of-order processed items so the DB writer only ever
/// commits (and notifies) a contiguous, anchored prefix. Nothing above the
/// contiguous frontier is inserted, so no unverified row is observable via
/// the API or dispatched to webhooks/queues.
struct ReorderBuffer<C: ChainTypes> {
    /// Next block number expected by the contiguous prefix.
    next: u64,
    /// Blocks received ahead of the contiguous frontier.
    pending: std::collections::BTreeMap<u64, ProcessedItem<C>>,
}

/// Hard cap on buffered out-of-order items before the run is aborted.
///
/// The scheduler's look-ahead is normally far smaller; hitting this means
/// one block is persistently unfetchable while later blocks stream in, and
/// aborting (checkpoint intact) is safer than growing without bound.
const MAX_REORDER_PENDING: usize = 16_384;

impl<C: ChainTypes> ReorderBuffer<C> {
    const fn new(start: u64) -> Self {
        Self {
            next: start,
            pending: std::collections::BTreeMap::new(),
        }
    }

    /// Buffer an item; ignores duplicates below the contiguous frontier.
    fn push(&mut self, item: ProcessedItem<C>) {
        if item.number() >= self.next {
            self.pending.insert(item.number(), item);
        } else {
            debug!(
                block = item.number(),
                next = self.next,
                "dropping duplicate item below contiguous frontier"
            );
        }
    }

    /// Drain the contiguous run starting at `next` into `batch`.
    fn drain_contiguous(&mut self, batch: &mut Vec<ProcessedItem<C>>) {
        while let Some(item) = self.pending.remove(&self.next) {
            self.next = self.next.saturating_add(1);
            batch.push(item);
        }
    }

    /// Number of buffered out-of-order items.
    fn buffered(&self) -> usize {
        self.pending.len()
    }
}

// Compile-time size assertions for hot types (reth pattern).
#[cfg(target_pointer_width = "64")]
const _: [(); 72] = [(); core::mem::size_of::<SyncOutcome>()];
// ProcessOutcome size varies with table_counts vec — skip assertion.

/// Run sync for a block range, fetching from the peer pool.
///
/// The `stop_rx` watch channel (inside `ctx`) allows external callers
/// (follow loop, shutdown handler) to signal an early stop.
///
/// # Errors
///
/// Returns an error if the sync encounters an unrecoverable failure.
#[instrument(skip_all, fields(start_block = start_block.as_u64(), end_block = end_block.as_u64()))]
pub async fn run_sync<C: ChainTypes>(
    start_block: BlockNumber,
    end_block: BlockNumber,
    ctx: SyncContext<C>,
) -> eyre::Result<SyncOutcome> {
    let started = Instant::now();
    let total_blocks = end_block.as_u64().saturating_sub(start_block.as_u64()) + 1;

    info!(
        start_block = start_block.as_u64(),
        end_block = end_block.as_u64(),
        total_blocks,
        "starting sync"
    );

    // Setup scheduler
    let sched_config = SchedulerConfig::default();
    let peer_health_config = PeerHealthConfig::from_scheduler_config(&sched_config);
    let peer_health = Arc::new(PeerHealthTracker::new(peer_health_config));
    let blocks: Vec<u64> = (start_block.as_u64()..=end_block.as_u64()).collect();
    let scheduler = Arc::new(PeerWorkScheduler::new_with_health(
        sched_config,
        blocks,
        Arc::clone(&peer_health),
    ));

    // Channels
    let (payload_tx, payload_rx) = mpsc::channel::<FetchItem<C>>(PAYLOAD_CHANNEL_SIZE);
    let (processed_tx, processed_rx) = mpsc::channel::<ProcessedItem<C>>(PROCESSED_CHANNEL_SIZE);
    let (ready_tx, ready_rx) = mpsc::unbounded_channel::<NetworkPeer<C>>();

    // Local shutdown signal for the peer feeder (triggered when fetch loop exits)
    let (feeder_shutdown_tx, feeder_shutdown_rx) = watch::channel(false);

    // Spawn peer feeder
    let feeder_handle =
        spawn_peer_feeder(Arc::clone(&ctx.pool), ready_tx.clone(), feeder_shutdown_rx);

    // Spawn N parallel processing workers (CPU: filter + decode)
    let mut worker_set = spawn_processing_workers(
        payload_rx,
        processed_tx,
        Arc::clone(&ctx.config),
        Arc::clone(&ctx.factories),
    );

    // Abort signal: fired by the DB writer on fatal integrity errors so the
    // fetch loop stops promptly instead of draining the whole range.
    let (abort_tx, abort_rx) = watch::channel(false);

    // Spawn DB writer (reads ProcessedBlocks, batches, commits)
    let consumer_handle = tokio::spawn(consume_payloads(
        processed_rx,
        Arc::clone(&ctx.config),
        Arc::clone(&ctx.db),
        Arc::clone(&ctx.handlers),
        Arc::clone(&ctx.metrics),
        Arc::clone(&ctx.pool),
        Arc::clone(&ctx.transfer_handlers),
        Arc::clone(&ctx.call_handlers),
        Arc::clone(&ctx.event_table_map),
        ctx.stream_dispatcher.clone(),
        ctx.is_backfill,
        Arc::clone(&ctx.receipt_tables),
        total_blocks,
        started,
        ctx.verbose,
        ctx.stop_rx.clone(),
        abort_tx,
        start_block.as_u64(),
        end_block.as_u64(),
    ));

    // Main fetch loop
    let active_tasks = Arc::new(AtomicUsize::new(0));
    let fetch_ctx = FetchLoopContext {
        scheduler: &scheduler,
        peer_health: &peer_health,
        pool: &ctx.pool,
        active_tasks: &active_tasks,
        metrics: &ctx.metrics,
        start_block,
        end_block,
        payload_tx: &payload_tx,
        ready_tx: &ready_tx,
        bloom_filter: &ctx.bloom_filter,
        head_seen_rx: &ctx.head_seen_rx,
    };
    run_fetch_loop(&fetch_ctx, ready_rx, &ctx.stop_rx, &abort_rx).await;

    // Shutdown: feeder → workers → DB writer
    let _ = feeder_shutdown_tx.send(true);
    let _ = feeder_handle.await;
    drop(payload_tx); // signals workers (payload_rx returns None)
                      // Wait for workers to drain. A panicked worker may have lost blocks —
                      // that must fail the run, never be silently ignored. (All worker
                      // processed_tx clones dropping lets the DB writer see channel close.)
    let mut worker_failure: Option<eyre::Report> = None;
    while let Some(joined) = worker_set.join_next().await {
        if let Err(join_err) = joined {
            worker_failure = Some(eyre::eyre!("processing worker failed: {join_err}"));
        }
    }

    let stats = match consumer_handle.await {
        Ok(Ok(stats)) => stats,
        Ok(Err(err)) => return Err(finish_rejected_run(&ctx.db, &ctx.config, err).await),
        Err(join_err) => {
            let err = eyre::eyre!("consumer task failed: {join_err}");
            return Err(finish_rejected_run(&ctx.db, &ctx.config, err).await);
        }
    };

    if let Some(err) = worker_failure {
        return Err(finish_rejected_run(&ctx.db, &ctx.config, err).await);
    }

    let elapsed = started.elapsed();
    Ok(SyncOutcome {
        blocks_fetched: stats.blocks_fetched,
        total_receipts: stats.total_receipts,
        events_matched: stats.events_matched,
        events_decoded: stats.events_decoded,
        events_stored: stats.events_stored,
        transfers_stored: stats.transfers_stored,
        calls_stored: stats.calls_stored,
        elapsed,
    })
}

// ── Peer feeder ──────────────────────────────────────────────────────

fn spawn_peer_feeder<C: ChainTypes>(
    pool: Arc<PeerPool<C>>,
    ready_tx: mpsc::UnboundedSender<NetworkPeer<C>>,
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
struct FetchLoopContext<'a, C: ChainTypes> {
    scheduler: &'a Arc<PeerWorkScheduler>,
    peer_health: &'a Arc<PeerHealthTracker>,
    pool: &'a Arc<PeerPool<C>>,
    active_tasks: &'a Arc<AtomicUsize>,
    metrics: &'a Arc<SieveMetrics>,
    start_block: BlockNumber,
    end_block: BlockNumber,
    payload_tx: &'a mpsc::Sender<FetchItem<C>>,
    ready_tx: &'a mpsc::UnboundedSender<NetworkPeer<C>>,
    bloom_filter: &'a Option<Arc<crate::filter::BloomFilter>>,
    head_seen_rx: &'a Option<watch::Receiver<u64>>,
}

/// Mutable state carried across fetch loop iterations.
struct FetchLoopState<C: ChainTypes> {
    fetch_tasks: JoinSet<()>,
    ready_peers: Vec<NetworkPeer<C>>,
    ready_set: HashSet<PeerId>,
    last_progress_check: Instant,
    last_progress_completed: u64,
}

#[instrument(skip_all)]
async fn run_fetch_loop<C: ChainTypes>(
    ctx: &FetchLoopContext<'_, C>,
    mut ready_rx: mpsc::UnboundedReceiver<NetworkPeer<C>>,
    stop_rx: &watch::Receiver<bool>,
    abort_rx: &watch::Receiver<bool>,
) {
    let fetch_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_FETCHES));
    let mut state = FetchLoopState::<C> {
        fetch_tasks: JoinSet::new(),
        ready_peers: Vec::new(),
        ready_set: HashSet::new(),
        last_progress_check: Instant::now(),
        last_progress_completed: 0,
    };

    loop {
        if *stop_rx.borrow() {
            debug!("fetch loop: stop signal received");
            break;
        }
        if *abort_rx.borrow() {
            debug!("fetch loop: consumer abort signal received");
            break;
        }

        drain_ready_peers(
            &mut ready_rx,
            ctx.pool,
            &mut state.ready_peers,
            &mut state.ready_set,
        );

        if state.ready_peers.is_empty() {
            if !await_first_peer(
                &mut ready_rx,
                ctx.pool,
                &mut state.ready_peers,
                &mut state.ready_set,
                abort_rx,
            )
            .await
            {
                break;
            }
            continue;
        }

        if !try_dispatch_iteration(ctx, &fetch_semaphore, &mut state).await {
            break;
        }
    }

    while state.fetch_tasks.join_next().await.is_some() {}
}

/// Run one dispatch iteration: reap tasks, check progress, acquire permit,
/// dispatch. Returns `false` if the scheduler is done.
async fn try_dispatch_iteration<C: ChainTypes>(
    ctx: &FetchLoopContext<'_, C>,
    semaphore: &Arc<Semaphore>,
    state: &mut FetchLoopState<C>,
) -> bool {
    while state.fetch_tasks.try_join_next().is_some() {}

    check_progress(
        ctx.scheduler,
        ctx.active_tasks,
        ctx.pool,
        ctx.metrics,
        &mut state.last_progress_check,
        &mut state.last_progress_completed,
        state.ready_peers.len(),
    )
    .await;

    if ctx.scheduler.is_done().await {
        debug!("scheduler: all work complete");
        return false;
    }

    let Ok(permit) = semaphore.clone().try_acquire_owned() else {
        sleep(Duration::from_millis(10)).await;
        return true;
    };

    dispatch_best_peer(
        ctx,
        &mut state.ready_peers,
        &mut state.ready_set,
        &mut state.fetch_tasks,
        permit,
    )
    .await;

    true
}

/// Block until the first peer arrives, add it to the ready set.
/// Returns `false` if the channel closed.
async fn await_first_peer<C: ChainTypes>(
    ready_rx: &mut mpsc::UnboundedReceiver<NetworkPeer<C>>,
    pool: &PeerPool<C>,
    ready_peers: &mut Vec<NetworkPeer<C>>,
    ready_set: &mut HashSet<PeerId>,
    abort_rx: &watch::Receiver<bool>,
) -> bool {
    let mut abort_rx = abort_rx.clone();
    let received = tokio::select! {
        peer = ready_rx.recv() => peer,
        _ = abort_rx.changed() => return false,
    };
    let Some(mut peer) = received else {
        return false;
    };
    if let Some(h) = pool.get_peer_head(peer.peer_id) {
        peer.head_number = h;
    }
    if ready_set.insert(peer.peer_id) {
        ready_peers.push(peer);
    }
    true
}

/// Pick the best peer, check health, get a batch, and spawn a fetch task.
async fn dispatch_best_peer<C: ChainTypes>(
    ctx: &FetchLoopContext<'_, C>,
    ready_peers: &mut Vec<NetworkPeer<C>>,
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

    // Head cap: in follow mode use global observed head; otherwise per-peer head
    let head_cap = if let Some(rx) = ctx.head_seen_rx {
        let observed = *rx.borrow();
        if observed > 0 {
            observed
        } else {
            ctx.end_block.as_u64()
        }
    } else if peer.head_number == 0 {
        ctx.end_block.as_u64()
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
        pool: Arc::clone(ctx.pool),
        payload_tx: ctx.payload_tx.clone(),
        ready_tx: ctx.ready_tx.clone(),
        bloom_filter: ctx.bloom_filter.clone(),
    };
    let params = FetchTaskParams {
        peer,
        blocks: batch.blocks,
        mode: batch.mode,
        permit,
    };

    let counter = Arc::clone(ctx.active_tasks);
    let gauge = ctx.metrics.active_fetches.clone();
    fetch_tasks.spawn(async move {
        let _guard = ActiveTaskGuard::new(&counter, gauge);
        run_fetch_task(task_ctx, params).await;
    });
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
async fn check_peer_eligibility<C: ChainTypes>(
    ctx: &FetchLoopContext<'_, C>,
    peer: &NetworkPeer<C>,
) -> Option<PeerAction> {
    // Cooling-down peers
    if ctx.peer_health.is_peer_cooling_down(peer.peer_id).await {
        return Some(PeerAction::Recycle(500));
    }

    if let Some(action) = check_history_range(ctx, peer).await {
        return Some(action);
    }

    // Stale-head detection: peer's probed head is below our work range.
    // Skip in follow mode — the head tracker verifies blocks exist on the network,
    // and per-peer heads are stale (probed once at connect). Let fetches fail instead.
    if ctx.head_seen_rx.is_none()
        && peer.head_number > 0
        && peer.head_number < ctx.start_block.as_u64()
    {
        let gap = ctx.start_block.as_u64().saturating_sub(peer.head_number);

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
            start_block = ctx.start_block.as_u64(),
            "peer head below work range, cooling down for 120s"
        );
        ctx.peer_health
            .set_stale_head_cooldown(peer.peer_id, Duration::from_secs(120))
            .await;
        return Some(PeerAction::RecycleImmediate);
    }

    None
}

/// History-range check: a peer that has pruned history below the front of
/// the work queue (advertised via the eth/69 Status earliest block) cannot
/// serve the next batch. Cool it down briefly — it becomes useful again
/// once the queue front passes its earliest block.
async fn check_history_range<C: ChainTypes>(
    ctx: &FetchLoopContext<'_, C>,
    peer: &NetworkPeer<C>,
) -> Option<PeerAction> {
    let earliest = peer.earliest_block?;
    if earliest == 0 {
        return None;
    }
    let lowest_pending = ctx.scheduler.lowest_pending().await?;
    if earliest <= lowest_pending {
        return None;
    }
    debug!(
        peer_id = ?peer.peer_id,
        peer_earliest = earliest,
        lowest_pending,
        "peer pruned history below work queue, cooling down"
    );
    ctx.peer_health
        .set_stale_head_cooldown(peer.peer_id, Duration::from_secs(30))
        .await;
    Some(PeerAction::RecycleImmediate)
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Non-blocking drain of ready channel, refreshing peer heads from pool.
fn drain_ready_peers<C: ChainTypes>(
    ready_rx: &mut mpsc::UnboundedReceiver<NetworkPeer<C>>,
    pool: &PeerPool<C>,
    ready_peers: &mut Vec<NetworkPeer<C>>,
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
async fn pick_best_ready_peer_index<C: ChainTypes>(
    peers: &[NetworkPeer<C>],
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

fn recycle_peer<C: ChainTypes>(
    ready_tx: &mpsc::UnboundedSender<NetworkPeer<C>>,
    peer: NetworkPeer<C>,
    delay_ms: u64,
) {
    let tx = ready_tx.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(delay_ms)).await;
        let _ = tx.send(peer);
    });
}

async fn check_progress<C: ChainTypes>(
    scheduler: &PeerWorkScheduler,
    active_tasks: &AtomicUsize,
    pool: &PeerPool<C>,
    metrics: &SieveMetrics,
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
    let active = active_tasks.load(Ordering::Relaxed);

    // Update Prometheus gauges
    metrics.pending_blocks.set(pending as i64);
    metrics.connected_peers.set(pool.len() as i64);

    if current_completed == *last_completed && (pending > 0 || escalation > 0) {
        warn!(
            completed = current_completed,
            pending,
            inflight,
            escalation,
            active,
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
            active,
            ready_count,
            "progress check"
        );
    }
    *last_completed = current_completed;
    *last_check = Instant::now();
}

// ── Processing workers ───────────────────────────────────────────────

/// Spawn N parallel workers that read raw payloads, run CPU-bound
/// filter+decode, and send `ProcessedBlock`s to the DB writer.
#[expect(
    clippy::needless_pass_by_value,
    reason = "Arc/Sender are cloned into spawned tasks"
)]
fn spawn_processing_workers<C: ChainTypes>(
    payload_rx: mpsc::Receiver<FetchItem<C>>,
    processed_tx: mpsc::Sender<ProcessedItem<C>>,
    config: Arc<IndexConfig>,
    factories: Arc<Vec<ResolvedFactory>>,
) -> JoinSet<()> {
    let num_workers = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);

    info!(num_workers, "spawning block processing workers");

    let payload_rx = Arc::new(tokio::sync::Mutex::new(payload_rx));
    let mut workers = JoinSet::new();

    for _ in 0..num_workers {
        let rx = Arc::clone(&payload_rx);
        let tx = processed_tx.clone();
        let cfg = Arc::clone(&config);
        let facts = Arc::clone(&factories);

        workers.spawn(async move {
            loop {
                let item = {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                };
                let Some(item) = item else { break };
                let processed = match item {
                    FetchItem::Payload(payload) => {
                        ProcessedItem::Block(Box::new(prepare_block(*payload, &cfg, &facts)))
                    }
                    FetchItem::Skipped(skipped) => ProcessedItem::Skipped(skipped),
                };
                if tx.send(processed).await.is_err() {
                    break;
                }
            }
        });
    }

    workers
}

// ── DB writer (payload consumer) ────────────────────────────────────

#[expect(
    clippy::too_many_arguments,
    reason = "grouping these into a struct would add complexity without benefit"
)]
#[instrument(skip_all)]
async fn consume_payloads<C: ChainTypes>(
    mut processed_rx: mpsc::Receiver<ProcessedItem<C>>,
    config: Arc<IndexConfig>,
    db: Arc<Database>,
    handlers: Arc<HandlerRegistry>,
    metrics: Arc<SieveMetrics>,
    pool: Arc<PeerPool<C>>,
    transfer_handlers: Arc<TransferRegistry>,
    call_handlers: Arc<CallRegistry>,
    event_table_map: Arc<HashMap<String, (String, String)>>,
    stream_dispatcher: Option<Arc<crate::stream::StreamDispatcher>>,
    is_backfill: bool,
    receipt_tables: Arc<HashSet<String>>,
    total_blocks: u64,
    started_at: Instant,
    verbose: bool,
    stop_rx: watch::Receiver<bool>,
    abort_tx: watch::Sender<bool>,
    start_block: u64,
    end_block: u64,
) -> eyre::Result<ConsumerStats> {
    let mut stats = ConsumerStats::default();
    let mut last_log = Instant::now();
    let mut max_indexed_block: u64 = 0;
    let mut batch: Vec<ProcessedItem<C>> = Vec::with_capacity(BATCH_SIZE);
    let mut reorder = ReorderBuffer::new(start_block);

    let ctx = ProcessContext {
        config: &config,
        db: &db,
        handlers: &handlers,
        transfer_handlers: &transfer_handlers,
        call_handlers: &call_handlers,
        event_table_map: &event_table_map,
        has_streams: stream_dispatcher.is_some(),
        receipt_tables: &receipt_tables,
    };

    loop {
        // Block indefinitely when batch is empty; use timeout when non-empty
        let processed = if batch.is_empty() {
            processed_rx.recv().await
        } else {
            match tokio::time::timeout(BATCH_FLUSH_TIMEOUT, processed_rx.recv()).await {
                Ok(processed) => processed,
                Err(_timeout) => {
                    flush_batch(
                        &mut batch,
                        &ctx,
                        &mut stats,
                        &metrics,
                        &mut max_indexed_block,
                        stream_dispatcher.as_ref(),
                        is_backfill,
                        &abort_tx,
                    )
                    .await?;
                    continue;
                }
            }
        };

        // Channel closed: flush remaining batch and exit
        let Some(processed) = processed else {
            if !batch.is_empty() {
                flush_batch(
                    &mut batch,
                    &ctx,
                    &mut stats,
                    &metrics,
                    &mut max_indexed_block,
                    stream_dispatcher.as_ref(),
                    is_backfill,
                    &abort_tx,
                )
                .await?;
            }
            // The channel closed. Unless an external stop cut the run
            // short, the contiguous frontier must have reached the end of
            // the range — anything else means blocks were lost in flight
            // (e.g. a panicked worker) and success must not be reported.
            if !*stop_rx.borrow() && reorder.next != end_block.saturating_add(1) {
                let _ = abort_tx.send(true);
                return Err(eyre::eyre!(
                    "sync range incomplete: contiguous frontier stopped at {} but the range \
                     ends at {end_block} ({} items still buffered out of order)",
                    reorder.next,
                    reorder.buffered()
                ));
            }
            break;
        };

        if let ProcessedItem::Block(block) = &processed {
            stats.blocks_fetched = stats.blocks_fetched.saturating_add(1);
            stats.total_receipts = stats.total_receipts.saturating_add(block.receipt_count);
        }

        buffer_item(&mut reorder, &mut batch, processed, &abort_tx)?;

        if batch.len() >= BATCH_SIZE {
            flush_batch(
                &mut batch,
                &ctx,
                &mut stats,
                &metrics,
                &mut max_indexed_block,
                stream_dispatcher.as_ref(),
                is_backfill,
                &abort_tx,
            )
            .await?;
        }

        log_sync_progress(
            &stats,
            &mut last_log,
            total_blocks,
            &pool,
            started_at,
            verbose,
            &stop_rx,
        );
    }

    Ok(stats)
}

/// Shared references for the DB writer (reduces argument counts).
struct ProcessContext<'a> {
    config: &'a IndexConfig,
    db: &'a Database,
    handlers: &'a HandlerRegistry,
    transfer_handlers: &'a TransferRegistry,
    call_handlers: &'a CallRegistry,
    /// Maps `"contract:event"` key → `(table_name, event_name)`.
    event_table_map: &'a HashMap<String, (String, String)>,
    /// Whether streams are configured (gates event payload collection).
    has_streams: bool,
    /// Table names with `include_receipts = true` (for streaming enrichment).
    receipt_tables: &'a HashSet<String>,
}

/// Log sync progress every 2 seconds.
fn log_sync_progress<C: ChainTypes>(
    stats: &ConsumerStats,
    last_log: &mut Instant,
    total_blocks: u64,
    pool: &PeerPool<C>,
    started_at: Instant,
    verbose: bool,
    stop_rx: &watch::Receiver<bool>,
) {
    if *stop_rx.borrow() || last_log.elapsed() < Duration::from_secs(2) {
        return;
    }
    if verbose {
        info!(
            blocks_fetched = stats.blocks_fetched,
            total_receipts = stats.total_receipts,
            events_matched = stats.events_matched,
            events_decoded = stats.events_decoded,
            events_stored = stats.events_stored,
            transfers_stored = stats.transfers_stored,
            calls_stored = stats.calls_stored,
            "sync progress"
        );
    } else {
        crate::ui::print_sync_progress(stats.blocks_fetched, total_blocks, pool.len(), started_at);
    }
    *last_log = Instant::now();
}

/// Compute the sealed hash for a block header.
fn compute_block_hash(header: &reth_primitives_traits::Header) -> alloy_primitives::B256 {
    SealedHeader::seal_slow(header.clone()).hash()
}

/// CPU-only block processing: factory pre-scan, filter, and decode.
///
/// Factory children are registered in-memory immediately (so subsequent
/// blocks in the same batch can match them). DB persistence is deferred
/// to [`flush_batch`].
fn prepare_block<C: ChainTypes>(
    payload: BlockPayload<C>,
    config: &IndexConfig,
    factories: &[ResolvedFactory],
) -> ProcessedBlock<C> {
    let block_number = BlockNumber::new(payload.header().number);
    let block_hash = compute_block_hash(payload.header());
    let receipt_count = payload.receipts().len() as u64;

    // Factory pre-scan: register children in-memory (no DB write)
    let factory_discoveries = if factories.is_empty() {
        Vec::new()
    } else {
        let discoveries = filter::scan_factory_events(&payload, factories);
        for d in &discoveries {
            register_factory_child_in_memory(config, d);
        }
        discoveries
    };

    // Filter + decode
    let matched = filter::filter_block(&payload, config);
    let matched_count = matched.len() as u64;
    let decoded_events = decode_matched_logs(&matched, config);

    ProcessedBlock {
        block_number,
        block_hash,
        factory_discoveries,
        decoded_events,
        matched_count,
        receipt_count,
        payload,
    }
}

/// Register a factory child in-memory only (no DB write).
fn register_factory_child_in_memory(config: &IndexConfig, discovery: &filter::FactoryDiscovery) {
    let Some(contract_idx) = config
        .contracts
        .iter()
        .position(|c| c.name == discovery.child_contract_name)
    else {
        warn!(
            factory = %discovery.child_contract_name,
            "factory child references unknown contract"
        );
        return;
    };

    if config.register_factory_child(discovery.child_address, contract_idx) {
        info!(
            factory = %discovery.child_contract_name,
            child = ?discovery.child_address,
            block = discovery.block_number,
            "registered new factory child"
        );
    }
}

/// Flush a batch of processed blocks to the database in a single transaction.
///
/// Opens one Postgres transaction, stores all block hashes, events,
/// transfers, calls, and factory children. Updates the checkpoint once
/// with the maximum block number (using GREATEST). Dispatches stream
/// notifications after commit.
#[expect(
    clippy::too_many_arguments,
    reason = "grouping these into a struct would add complexity without benefit"
)]
async fn flush_batch<C: ChainTypes>(
    batch: &mut Vec<ProcessedItem<C>>,
    ctx: &ProcessContext<'_>,
    stats: &mut ConsumerStats,
    metrics: &SieveMetrics,
    max_indexed_block: &mut u64,
    stream_dispatcher: Option<&Arc<crate::stream::StreamDispatcher>>,
    is_backfill: bool,
    abort_tx: &watch::Sender<bool>,
) -> eyre::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let batch_len = batch.len() as u64;

    // Integrity gate: every adjacency whose other side is known (same batch
    // or already stored) must link. A violation means a peer served fork
    // data (or a reorg raced the fetch) — abort the run instead of storing.
    // Speculative in-memory state (factory children) is discarded by
    // `run_sync` once the workers have stopped.
    match verify_batch_anchors(batch, ctx.db).await? {
        AnchorCheck::Ok => {}
        AnchorCheck::ParentMismatch {
            block,
            actual,
            expected,
        } => {
            let _ = abort_tx.send(true);
            return Err(eyre::eyre!(
                "block {block} parent-hash mismatch: header claims parent {actual} but chain has \
                 {expected} — refusing to store batch (fork data from peer or concurrent reorg)"
            ));
        }
        AnchorCheck::ChildMismatch {
            parent_block,
            child_block,
        } => {
            // A previously committed descendant does not link to the newly
            // verified parent. Roll stored state back to the last committed
            // contiguous checkpoint (the batch itself is uncommitted, so
            // the target must not include any of its blocks), then abort so
            // the retry refetches everything above it.
            let _ = abort_tx.send(true);
            let committed = batch
                .iter()
                .map(ProcessedItem::number)
                .min()
                .unwrap_or(child_block)
                .saturating_sub(1);
            rollback_committed_to(ctx, committed).await?;
            return Err(eyre::eyre!(
                "stored block {child_block} does not link to verified parent {parent_block}; \
                 rolled back to committed checkpoint {committed} (fork data from peer or \
                 concurrent reorg)"
            ));
        }
    }

    // The batch is a contiguous run by construction (reorder buffer), so
    // committing it moves the checkpoint to its last block. Nothing beyond
    // the contiguous prefix is ever committed or notified.
    let checkpoint_to = batch.iter().map(ProcessedItem::number).max();

    match flush_batch_inner(batch, ctx, checkpoint_to).await {
        Ok(outcomes) => {
            update_batch_stats(stats, metrics, max_indexed_block, &outcomes, batch_len);
            dispatch_batch_notifications(stream_dispatcher, outcomes, is_backfill);
        }
        Err(err) => {
            let _ = abort_tx.send(true);
            return Err(err.wrap_err(format!("failed to flush batch of {batch_len} blocks")));
        }
    }
    batch.clear();
    Ok(())
}

/// Buffer an item and drain the contiguous prefix into the flush batch.
///
/// Only the contiguous prefix ever reaches the flush batch: rows and
/// notifications for out-of-order blocks must not be published until every
/// predecessor is committed and anchored.
///
/// # Errors
///
/// Returns an error (and fires the abort signal) if the out-of-order
/// buffer exceeds [`MAX_REORDER_PENDING`].
fn buffer_item<C: ChainTypes>(
    reorder: &mut ReorderBuffer<C>,
    batch: &mut Vec<ProcessedItem<C>>,
    processed: ProcessedItem<C>,
    abort_tx: &watch::Sender<bool>,
) -> eyre::Result<()> {
    reorder.push(processed);
    reorder.drain_contiguous(batch);
    if reorder.buffered() > MAX_REORDER_PENDING {
        let _ = abort_tx.send(true);
        return Err(eyre::eyre!(
            "reorder buffer exceeded {MAX_REORDER_PENDING} items waiting for block {}; \
             aborting run (a block appears unfetchable)",
            reorder.next
        ));
    }
    Ok(())
}

/// Roll back all indexed state above `block` in one transaction (used when
/// a committed descendant fails adjacency verification).
async fn rollback_committed_to(ctx: &ProcessContext<'_>, block: u64) -> eyre::Result<()> {
    let target = BlockNumber::new(block);
    let mut tx = ctx.db.begin().await?;
    ctx.handlers.rollback_all(target, &mut tx).await?;
    ctx.transfer_handlers.rollback_all(target, &mut tx).await?;
    ctx.call_handlers.rollback_all(target, &mut tx).await?;
    db::rollback_factory_children(&mut tx, target, ctx.config).await?;
    db::rollback_to(&mut tx, target).await?;
    tx.commit()
        .await
        .wrap_err("failed to commit adjacency rollback")?;
    warn!(block, "rolled back stored state above verified parent");
    Ok(())
}

/// Clean up after a sync run whose consumer failed (rejected batch or
/// panic): discard speculative in-memory factory state by reloading the
/// committed set from the database.
///
/// Must only be called once the processing workers are joined — they
/// register factory children speculatively, and a late registration would
/// survive the rebuild. Returns the error to propagate: the original one,
/// or a non-retriable [`crate::sync::FactoryStateError`] if the rebuild
/// itself failed and speculative state may still be live.
async fn finish_rejected_run(
    db: &Database,
    config: &IndexConfig,
    err: eyre::Report,
) -> eyre::Report {
    match db::rebuild_factory_children(db, config).await {
        Ok(children) => {
            debug!(
                children,
                "restored committed factory children after rejected batch"
            );
            err
        }
        Err(rebuild_err) => crate::sync::FactoryStateError::report(&err, &rebuild_err),
    }
}

/// Verify parent-hash linkage for every block in the batch.
///
/// Builds a map of known canonical hashes from the batch itself plus any
/// previously stored hashes for parents outside the batch, then checks each
/// block's `parent_hash` against it. Parents with no known hash (e.g.
/// bloom-skipped blocks that never stored one) are skipped.
///
/// # Errors
///
/// Returns an error on the first linkage violation, or if the stored-hash
/// lookup fails.
async fn verify_batch_anchors<C: ChainTypes>(
    batch: &[ProcessedItem<C>],
    db: &Database,
) -> eyre::Result<AnchorCheck> {
    let mut known_hashes: HashMap<u64, B256> =
        batch.iter().map(|b| (b.number(), b.hash())).collect();
    let mut known_parents: HashMap<u64, B256> = batch
        .iter()
        .map(|b| (b.number(), b.parent_hash()))
        .collect();

    let triples: Vec<(u64, B256, B256)> = batch
        .iter()
        .map(|b| (b.number(), b.hash(), b.parent_hash()))
        .collect();

    // Resolve neighbors outside the batch from previously stored rows:
    // the parent's hash (for the parent-direction check) and the child's
    // parent hash (for the child-direction check).
    for &(number, _, _) in &triples {
        if let Some(parent) = number.checked_sub(1) {
            if let std::collections::hash_map::Entry::Vacant(entry) = known_hashes.entry(parent) {
                if let Some(hash) = db.get_block_hash(BlockNumber::new(parent)).await? {
                    entry.insert(hash);
                }
            }
        }
        if let Some(child) = number.checked_add(1) {
            if let std::collections::hash_map::Entry::Vacant(entry) = known_parents.entry(child) {
                if let Some(parent_hash) =
                    db::get_block_parent_hash(db, BlockNumber::new(child)).await?
                {
                    entry.insert(parent_hash);
                }
            }
        }
    }

    Ok(find_anchor_violation(
        &triples,
        &known_hashes,
        &known_parents,
    ))
}

/// Outcome of batch adjacency verification.
#[derive(Debug, PartialEq, Eq)]
enum AnchorCheck {
    /// All known adjacencies link.
    Ok,
    /// A batch block's parent hash contradicts the known parent hash.
    ParentMismatch {
        block: u64,
        actual: B256,
        expected: B256,
    },
    /// A known child's parent hash contradicts a batch block's hash.
    ChildMismatch { parent_block: u64, child_block: u64 },
}

/// Check every adjacency of the batch in both directions.
///
/// `triples` holds `(number, hash, parent_hash)` for each batch item in any
/// order; `known_hashes`/`known_parents` map block number → hash / parent
/// hash for every block whose value is known (batch plus stored neighbors).
/// Unknown neighbors are skipped — they are checked when their side flushes.
fn find_anchor_violation(
    triples: &[(u64, B256, B256)],
    known_hashes: &HashMap<u64, B256>,
    known_parents: &HashMap<u64, B256>,
) -> AnchorCheck {
    for &(number, hash, parent_hash) in triples {
        if let Some(parent_number) = number.checked_sub(1) {
            if let Some(&expected) = known_hashes.get(&parent_number) {
                if parent_hash != expected {
                    return AnchorCheck::ParentMismatch {
                        block: number,
                        actual: parent_hash,
                        expected,
                    };
                }
            }
        }
        if let Some(child_number) = number.checked_add(1) {
            if let Some(&child_parent) = known_parents.get(&child_number) {
                if child_parent != hash {
                    return AnchorCheck::ChildMismatch {
                        parent_block: number,
                        child_block: child_number,
                    };
                }
            }
        }
    }
    AnchorCheck::Ok
}

/// Inner flush: open one transaction, store all blocks, commit.
///
/// Phase 0: batch store all block hashes in one UNNEST query.
/// Phase 1: per-block preparation — factory children, build event contexts,
/// scan transfers/calls, compute outcomes.
/// Phase 2: batch insert — multi-row INSERT all events/transfers/calls.
/// Phase 3: checkpoint + commit.
async fn flush_batch_inner<C: ChainTypes>(
    batch: &[ProcessedItem<C>],
    ctx: &ProcessContext<'_>,
    checkpoint_to: Option<u64>,
) -> eyre::Result<Vec<ProcessOutcome>> {
    let mut tx = ctx.db.begin().await?;
    let mut outcomes = Vec::with_capacity(batch.len());
    let mut all_events: Vec<(&decode::DecodedEvent, EventContext)> = Vec::new();
    let mut all_transfers: Vec<(NativeTransfer, EventContext)> = Vec::new();
    let mut all_calls: Vec<(DecodedCall, EventContext)> = Vec::new();

    // Batch store all block hashes (payload AND bloom-skipped) in one
    // UNNEST query, including parent hashes for adjacency verification.
    let block_numbers: Vec<i64> = batch.iter().map(|b| b.number() as i64).collect();
    let block_hashes: Vec<Vec<u8>> = batch.iter().map(|b| b.hash().as_slice().to_vec()).collect();
    let parent_hashes: Vec<Vec<u8>> = batch
        .iter()
        .map(|b| b.parent_hash().as_slice().to_vec())
        .collect();
    db::store_block_hashes_batch(&mut tx, &block_numbers, &block_hashes, &parent_hashes).await?;

    // Phase 1: per-block preparation (payload blocks only)
    for item in batch {
        let ProcessedItem::Block(block) = item else {
            continue;
        };
        let outcome = prepare_block_outcome(
            block,
            ctx,
            &mut tx,
            &mut all_events,
            &mut all_transfers,
            &mut all_calls,
        )
        .await?;
        outcomes.push(outcome);
    }

    // Phase 2: batch inserts
    let event_refs: Vec<(&decode::DecodedEvent, &EventContext)> =
        all_events.iter().map(|(e, c)| (*e, c)).collect();
    ctx.handlers.batch_dispatch(&event_refs, &mut tx).await?;
    if !all_transfers.is_empty() {
        let transfer_refs: Vec<(&NativeTransfer, &EventContext)> =
            all_transfers.iter().map(|(t, c)| (t, c)).collect();
        ctx.transfer_handlers
            .batch_dispatch(&transfer_refs, &mut tx)
            .await?;
    }
    if !all_calls.is_empty() {
        let call_refs: Vec<(&DecodedCall, &EventContext)> =
            all_calls.iter().map(|(c, ctx)| (c, ctx)).collect();
        ctx.call_handlers
            .batch_dispatch(&call_refs, &mut tx)
            .await?;
    }

    // Phase 3: checkpoint (contiguous prefix only) + commit
    if let Some(checkpoint) = checkpoint_to {
        db::update_checkpoint(&mut tx, BlockNumber::new(checkpoint)).await?;
    }

    tx.commit()
        .await
        .wrap_err("failed to commit batch transaction")?;

    Ok(outcomes)
}

/// Prepare one block's outcome and accumulate events/transfers/calls for batch insert.
///
/// Stores factory children (small, per-block). Accumulates events, transfers,
/// and calls into the shared vecs for Phase 2 batch insert. Block hashes are
/// stored in bulk by the caller via `store_block_hashes_batch`.
async fn prepare_block_outcome<'a, C: ChainTypes>(
    block: &'a ProcessedBlock<C>,
    ctx: &ProcessContext<'_>,
    tx: &mut sqlx::Transaction<'_, Postgres>,
    all_events: &mut Vec<(&'a decode::DecodedEvent, EventContext)>,
    all_transfers: &mut Vec<(NativeTransfer, EventContext)>,
    all_calls: &mut Vec<(DecodedCall, EventContext)>,
) -> eyre::Result<ProcessOutcome> {
    // Block hashes are batched in flush_batch_inner via store_block_hashes_batch.

    // Persist factory discoveries within the batch transaction
    for d in &block.factory_discoveries {
        db::store_factory_child(tx, &d.child_contract_name, &d.child_address, d.block_number)
            .await?;
    }

    let mut stored_count = 0u64;
    let mut table_counts: HashMap<String, (String, u64)> = HashMap::new();
    let mut event_payloads: Vec<crate::stream::EventPayload> = Vec::new();
    let mut sender_cache: HashMap<TxIndex, Address> = HashMap::new();

    // Events: count matches and accumulate for batch insert
    for event in &block.decoded_events {
        let event_context =
            build_event_context(&block.payload, block.block_hash, event, &mut sender_cache)?;
        let dispatched = ctx
            .handlers
            .matching_count(&event.contract_name, &event.event_name);
        if dispatched > 0 {
            track_event_table(ctx.event_table_map, event, dispatched, &mut table_counts);
            if ctx.has_streams {
                collect_event_payload(
                    ctx.event_table_map,
                    event,
                    &event_context,
                    ctx.receipt_tables,
                    &mut event_payloads,
                );
            }
            all_events.push((event, event_context));
        }
        stored_count = stored_count.saturating_add(dispatched);
    }

    // Transfers: scan and accumulate for batch insert
    let transfer_count = accumulate_transfers(
        &block.payload,
        block.block_hash,
        &mut sender_cache,
        ctx,
        all_transfers,
        &mut table_counts,
        &mut event_payloads,
    )?;

    // Calls: scan and accumulate for batch insert
    let call_count = accumulate_calls(
        &block.payload,
        block.block_hash,
        &mut sender_cache,
        ctx,
        all_calls,
        &mut table_counts,
        &mut event_payloads,
    )?;

    let table_counts = table_counts
        .into_iter()
        .map(|(table, (event, count))| (table, event, count))
        .collect();

    Ok(ProcessOutcome {
        block_number: block.block_number.as_u64(),
        block_timestamp: block.payload.header().timestamp,
        matched: block.matched_count,
        decoded: block.decoded_events.len() as u64,
        stored: stored_count,
        transfers: transfer_count,
        calls: call_count,
        table_counts,
        event_payloads,
    })
}

/// Update consumer stats and Prometheus metrics after a successful batch flush.
fn update_batch_stats(
    stats: &mut ConsumerStats,
    metrics: &SieveMetrics,
    max_indexed_block: &mut u64,
    outcomes: &[ProcessOutcome],
    batch_len: u64,
) {
    metrics.blocks_indexed.inc_by(batch_len);
    for outcome in outcomes {
        stats.events_matched = stats.events_matched.saturating_add(outcome.matched);
        stats.events_decoded = stats.events_decoded.saturating_add(outcome.decoded);
        stats.events_stored = stats.events_stored.saturating_add(outcome.stored);
        stats.transfers_stored = stats.transfers_stored.saturating_add(outcome.transfers);
        stats.calls_stored = stats.calls_stored.saturating_add(outcome.calls);
        metrics.events_matched.inc_by(outcome.matched);
        metrics.events_stored.inc_by(outcome.stored);
        metrics.transfers_stored.inc_by(outcome.transfers);
        metrics.calls_stored.inc_by(outcome.calls);

        if outcome.block_number > *max_indexed_block {
            *max_indexed_block = outcome.block_number;
            metrics.indexed_block.set(outcome.block_number as i64);
            metrics.last_block_timestamp.store(
                outcome.block_timestamp,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
    }
}

/// Dispatch stream notifications for all blocks in a flushed batch.
///
/// Takes ownership of `outcomes` to avoid cloning `event_payloads`.
fn dispatch_batch_notifications(
    stream_dispatcher: Option<&Arc<crate::stream::StreamDispatcher>>,
    outcomes: Vec<ProcessOutcome>,
    is_backfill: bool,
) {
    let Some(dispatcher) = stream_dispatcher else {
        return;
    };
    for outcome in outcomes {
        if !outcome.table_counts.is_empty() {
            let tables = outcome
                .table_counts
                .into_iter()
                .map(|(name, event, count)| crate::stream::TableNotification { name, event, count })
                .collect();
            dispatcher.send(
                crate::stream::BlockNotification {
                    block_number: outcome.block_number,
                    block_timestamp: outcome.block_timestamp,
                    tables,
                },
                is_backfill,
            );
        }
        if !outcome.event_payloads.is_empty() {
            dispatcher.send_events(outcome.event_payloads, is_backfill);
        }
    }
}

/// Track an event's table in the table_counts map.
fn track_event_table(
    event_table_map: &HashMap<String, (String, String)>,
    event: &decode::DecodedEvent,
    dispatched: u64,
    table_counts: &mut HashMap<String, (String, u64)>,
) {
    let key = format!("{}:{}", event.contract_name, event.event_name);
    if let Some((table, event_name)) = event_table_map.get(&key) {
        let entry = table_counts
            .entry(table.clone())
            .or_insert_with(|| (event_name.clone(), 0));
        entry.1 = entry.1.saturating_add(dispatched);
    }
}

/// Build an [`EventPayload`] from a decoded event and push it to the payloads vec.
fn collect_event_payload(
    event_table_map: &HashMap<String, (String, String)>,
    event: &decode::DecodedEvent,
    context: &EventContext,
    receipt_tables: &HashSet<String>,
    payloads: &mut Vec<crate::stream::EventPayload>,
) {
    let key = format!("{}:{}", event.contract_name, event.event_name);
    let Some((table, event_name)) = event_table_map.get(&key) else {
        return;
    };

    let mut data = serde_json::Map::new();
    for param in event.indexed.iter().chain(event.body.iter()) {
        data.insert(
            param.name.clone(),
            crate::stream::dyn_sol_to_json(&param.value),
        );
    }

    let include = receipt_tables.contains(table);
    payloads.push(crate::stream::EventPayload {
        table: table.clone(),
        event: event_name.clone(),
        contract_name: event.contract_name.clone(),
        contract: Address::to_checksum(&event.contract_address, None),
        block_number: event.block_number.as_u64(),
        block_timestamp: event.block_timestamp,
        tx_hash: format!("{:#x}", event.tx_hash),
        log_index: Some(event.log_index.as_u32()),
        tx_index: event.tx_index.as_u32(),
        tx_from: Some(Address::to_checksum(&context.tx_from, None)),
        tx_value: include.then(|| context.tx_value.to_string()),
        tx_gas_price: include.then_some(context.tx_gas_price as u64),
        gas_used: include.then_some(context.tx_gas_used),
        nonce: include.then_some(context.tx_nonce),
        cumulative_gas_used: include.then_some(context.cumulative_gas_used),
        status: include.then_some(context.tx_status),
        data,
    });
}

/// Build an [`EventPayload`] from a native transfer.
fn build_transfer_payload(
    transfer: &NativeTransfer,
    context: &EventContext,
    table_name: &str,
    include: bool,
) -> crate::stream::EventPayload {
    let mut data = serde_json::Map::new();
    data.insert(
        "from_address".to_string(),
        serde_json::Value::String(Address::to_checksum(&transfer.from_address, None)),
    );
    data.insert(
        "to_address".to_string(),
        serde_json::Value::String(Address::to_checksum(&transfer.to_address, None)),
    );
    data.insert(
        "value".to_string(),
        serde_json::Value::String(transfer.value.to_string()),
    );

    crate::stream::EventPayload {
        table: table_name.to_string(),
        event: "transfer".to_string(),
        contract_name: String::new(),
        contract: "0x0000000000000000000000000000000000000000".to_string(),
        block_number: transfer.block_number.as_u64(),
        block_timestamp: context.block_timestamp,
        tx_hash: format!("{:#x}", transfer.tx_hash),
        log_index: None,
        tx_index: transfer.tx_index.as_u32(),
        tx_from: Some(Address::to_checksum(&context.tx_from, None)),
        tx_value: include.then(|| context.tx_value.to_string()),
        tx_gas_price: include.then_some(context.tx_gas_price as u64),
        gas_used: include.then_some(context.tx_gas_used),
        nonce: include.then_some(context.tx_nonce),
        cumulative_gas_used: include.then_some(context.cumulative_gas_used),
        status: include.then_some(context.tx_status),
        data,
    }
}

/// Build an [`EventPayload`] from a decoded function call.
fn build_call_payload(
    call: &DecodedCall,
    context: &EventContext,
    table_name: &str,
    include: bool,
) -> crate::stream::EventPayload {
    let mut data = serde_json::Map::new();
    for param in &call.params {
        data.insert(
            param.name.clone(),
            crate::stream::dyn_sol_to_json(&param.value),
        );
    }

    crate::stream::EventPayload {
        table: table_name.to_string(),
        event: call.function_name.clone(),
        contract_name: call.contract_name.clone(),
        contract: Address::to_checksum(&call.contract_address, None),
        block_number: call.block_number.as_u64(),
        block_timestamp: context.block_timestamp,
        tx_hash: format!("{:#x}", call.tx_hash),
        log_index: None,
        tx_index: call.tx_index.as_u32(),
        tx_from: Some(Address::to_checksum(&context.tx_from, None)),
        tx_value: include.then(|| context.tx_value.to_string()),
        tx_gas_price: include.then_some(context.tx_gas_price as u64),
        gas_used: include.then_some(context.tx_gas_used),
        nonce: include.then_some(context.tx_nonce),
        cumulative_gas_used: include.then_some(context.cumulative_gas_used),
        status: include.then_some(context.tx_status),
        data,
    }
}

/// Scan native transfers and accumulate for batch insert (no DB writes).
fn accumulate_transfers<C: ChainTypes>(
    payload: &BlockPayload<C>,
    block_hash: B256,
    sender_cache: &mut HashMap<TxIndex, Address>,
    ctx: &ProcessContext<'_>,
    all_transfers: &mut Vec<(NativeTransfer, EventContext)>,
    table_counts: &mut HashMap<String, (String, u64)>,
    event_payloads: &mut Vec<crate::stream::EventPayload>,
) -> eyre::Result<u64> {
    if ctx.transfer_handlers.is_empty() {
        return Ok(0);
    }
    scan_transfers(
        payload,
        block_hash,
        sender_cache,
        ctx.transfer_handlers,
        ctx.has_streams,
        all_transfers,
        event_payloads,
        table_counts,
        ctx.receipt_tables,
    )
}

/// Scan function calls and accumulate for batch insert (no DB writes).
fn accumulate_calls<C: ChainTypes>(
    payload: &BlockPayload<C>,
    block_hash: B256,
    sender_cache: &mut HashMap<TxIndex, Address>,
    ctx: &ProcessContext<'_>,
    all_calls: &mut Vec<(DecodedCall, EventContext)>,
    table_counts: &mut HashMap<String, (String, u64)>,
    event_payloads: &mut Vec<crate::stream::EventPayload>,
) -> eyre::Result<u64> {
    if ctx.call_handlers.is_empty() {
        return Ok(0);
    }
    scan_calls(
        payload,
        block_hash,
        sender_cache,
        ctx.config,
        ctx.call_handlers,
        ctx.has_streams,
        all_calls,
        event_payloads,
        table_counts,
        ctx.receipt_tables,
    )
}

/// Compute per-transaction gas used from cumulative gas values.
///
/// For the first transaction in a block, `gas_used == cumulative_gas_used`.
/// For subsequent transactions, `gas_used = cumulative[i] - cumulative[i-1]`.
fn compute_gas_used<R: TxReceipt>(receipts: &[R], tx_idx: usize) -> u64 {
    let cumulative = receipts
        .get(tx_idx)
        .map_or(0, TxReceipt::cumulative_gas_used);
    if tx_idx == 0 {
        cumulative
    } else {
        cumulative.saturating_sub(
            receipts
                .get(tx_idx - 1)
                .map_or(0, TxReceipt::cumulative_gas_used),
        )
    }
}

/// Build an [`EventContext`] from a block payload for a given decoded event.
///
/// Caches sender recovery per `tx_index` to avoid redundant ECDSA work
/// when multiple events originate from the same transaction.
fn build_event_context<C: ChainTypes>(
    payload: &BlockPayload<C>,
    block_hash: B256,
    event: &decode::DecodedEvent,
    sender_cache: &mut HashMap<TxIndex, Address>,
) -> eyre::Result<EventContext> {
    let tx_signed = payload
        .body()
        .transactions
        .get(event.tx_index.as_u32() as usize)
        .ok_or_else(|| {
            eyre::eyre!(
                "tx_index {} out of range for block {}",
                event.tx_index,
                payload.header().number
            )
        })?;

    let tx_from = if let Some(&cached) = sender_cache.get(&event.tx_index) {
        cached
    } else {
        let sender = tx_signed
            .recover_signer_unchecked()
            .wrap_err("failed to recover tx sender")?;
        sender_cache.insert(event.tx_index, sender);
        sender
    };

    let tx_to = match tx_signed.kind() {
        TxKind::Call(addr) => Some(addr),
        TxKind::Create => None,
    };

    let tx_idx_usize = event.tx_index.as_u32() as usize;
    let cumulative = payload
        .receipts()
        .get(tx_idx_usize)
        .map_or(0, TxReceipt::cumulative_gas_used);
    let gas_used = compute_gas_used(payload.receipts(), tx_idx_usize);

    Ok(EventContext {
        block_timestamp: payload.header().timestamp,
        block_hash,
        tx_from,
        tx_to,
        tx_value: tx_signed.value(),
        tx_gas_price: tx_signed.effective_gas_price(payload.header().base_fee_per_gas),
        tx_gas_used: gas_used,
        tx_nonce: tx_signed.nonce(),
        cumulative_gas_used: cumulative,
        tx_status: true,
    })
}

/// Scan block transactions for native ETH transfers (no DB writes).
///
/// Iterates all transactions in the block body, skipping zero-value,
/// contract-creation, and reverted transactions. Accumulates matching
/// transfers for batch insert.
///
/// Returns the number of transfers matched.
#[expect(
    clippy::too_many_arguments,
    reason = "per-table counting adds table_counts"
)]
fn scan_transfers<C: ChainTypes>(
    payload: &BlockPayload<C>,
    block_hash: B256,
    sender_cache: &mut HashMap<TxIndex, Address>,
    transfer_handlers: &TransferRegistry,
    has_streams: bool,
    all_transfers: &mut Vec<(NativeTransfer, EventContext)>,
    event_payloads: &mut Vec<crate::stream::EventPayload>,
    table_counts: &mut HashMap<String, (String, u64)>,
    receipt_tables: &HashSet<String>,
) -> eyre::Result<u64> {
    let block_number = BlockNumber::new(payload.header().number);
    let receipts = payload.receipts();
    let mut count = 0u64;

    for (tx_idx, tx_signed) in payload.body().transactions.iter().enumerate() {
        let tx_index = TxIndex::from_usize(tx_idx);
        if tx_signed.value().is_zero() {
            continue;
        }
        if !receipts.get(tx_idx).is_some_and(TxReceipt::status) {
            continue;
        }
        let to_address = match tx_signed.kind() {
            TxKind::Call(addr) => addr,
            TxKind::Create => continue,
        };

        let from_address = if let Some(&cached) = sender_cache.get(&tx_index) {
            cached
        } else {
            let sender = tx_signed
                .recover_signer_unchecked()
                .wrap_err("failed to recover tx sender for transfer")?;
            sender_cache.insert(tx_index, sender);
            sender
        };

        let transfer = NativeTransfer {
            block_number,
            tx_hash: *tx_signed.tx_hash(),
            tx_index,
            from_address,
            to_address,
            value: tx_signed.value(),
        };

        let matched_tables = transfer_handlers.matched_table_names(&transfer);
        if matched_tables.is_empty() {
            continue;
        }

        let context = EventContext {
            block_timestamp: payload.header().timestamp,
            block_hash,
            tx_from: from_address,
            tx_to: Some(to_address),
            tx_value: tx_signed.value(),
            tx_gas_price: tx_signed.effective_gas_price(payload.header().base_fee_per_gas),
            tx_gas_used: compute_gas_used(receipts, tx_idx),
            tx_nonce: tx_signed.nonce(),
            cumulative_gas_used: receipts
                .get(tx_idx)
                .map_or(0, TxReceipt::cumulative_gas_used),
            tx_status: true,
        };

        let dispatched = matched_tables.len() as u64;
        for table_name in &matched_tables {
            let entry = table_counts
                .entry((*table_name).to_string())
                .or_insert_with(|| ("transfer".to_string(), 0));
            entry.1 = entry.1.saturating_add(1);
            if has_streams {
                let include = receipt_tables.contains(*table_name);
                event_payloads.push(build_transfer_payload(
                    &transfer, &context, table_name, include,
                ));
            }
        }

        all_transfers.push((transfer, context));
        count = count.saturating_add(dispatched);
    }

    Ok(count)
}

/// Scan block transactions for function calls (no DB writes).
///
/// Iterates all transactions in the block body, matching calldata selectors
/// against configured functions. Accumulates matching calls for batch insert.
///
/// Returns the number of calls matched.
#[expect(
    clippy::too_many_arguments,
    reason = "per-table counting adds table_counts"
)]
fn scan_calls<C: ChainTypes>(
    payload: &BlockPayload<C>,
    block_hash: B256,
    sender_cache: &mut HashMap<TxIndex, Address>,
    config: &IndexConfig,
    call_handlers: &CallRegistry,
    has_streams: bool,
    all_calls: &mut Vec<(DecodedCall, EventContext)>,
    event_payloads: &mut Vec<crate::stream::EventPayload>,
    table_counts: &mut HashMap<String, (String, u64)>,
    receipt_tables: &HashSet<String>,
) -> eyre::Result<u64> {
    let block_number = BlockNumber::new(payload.header().number);
    let receipts = payload.receipts();
    let mut count = 0u64;

    for (tx_idx, tx_signed) in payload.body().transactions.iter().enumerate() {
        let tx_index = TxIndex::from_usize(tx_idx);
        let input = tx_signed.input();

        if input.len() < 4 {
            continue;
        }

        let to_address = match tx_signed.kind() {
            TxKind::Call(addr) => addr,
            TxKind::Create => continue,
        };

        let Some(contract) = config.contract_for_address(&to_address) else {
            continue;
        };

        let selector = Selector::from_slice(&input[..4]);

        let Some(function) = contract.functions.get(&selector) else {
            continue;
        };

        if !receipts.get(tx_idx).is_some_and(TxReceipt::status) {
            continue;
        }

        let decoded_values = match function.abi_decode_input(&input[4..]) {
            Ok(values) => values,
            Err(e) => {
                warn!(
                    block = block_number.as_u64(),
                    tx_index = tx_index.as_u32(),
                    function = %function.name,
                    error = %e,
                    "failed to decode function call"
                );
                continue;
            }
        };

        let params: Vec<DecodedParam> = function
            .inputs
            .iter()
            .zip(decoded_values)
            .map(|(input_param, value)| DecodedParam {
                name: input_param.name.clone(),
                solidity_type: input_param.ty.clone(),
                value,
            })
            .collect();

        let from_address = if let Some(&cached) = sender_cache.get(&tx_index) {
            cached
        } else {
            let sender = tx_signed
                .recover_signer_unchecked()
                .wrap_err("failed to recover tx sender for call")?;
            sender_cache.insert(tx_index, sender);
            sender
        };

        let decoded_call = DecodedCall {
            function_name: function.name.clone(),
            contract_name: contract.name.clone(),
            params,
            block_number,
            tx_hash: *tx_signed.tx_hash(),
            tx_index,
            contract_address: to_address,
        };

        let matched_tables = call_handlers
            .matched_table_names(&decoded_call.contract_name, &decoded_call.function_name);
        if matched_tables.is_empty() {
            continue;
        }

        let context = EventContext {
            block_timestamp: payload.header().timestamp,
            block_hash,
            tx_from: from_address,
            tx_to: Some(to_address),
            tx_value: tx_signed.value(),
            tx_gas_price: tx_signed.effective_gas_price(payload.header().base_fee_per_gas),
            tx_gas_used: compute_gas_used(receipts, tx_idx),
            tx_nonce: tx_signed.nonce(),
            cumulative_gas_used: receipts
                .get(tx_idx)
                .map_or(0, TxReceipt::cumulative_gas_used),
            tx_status: true,
        };

        count = count.saturating_add(track_call_tables(
            &matched_tables,
            &decoded_call,
            &context,
            has_streams,
            event_payloads,
            table_counts,
            receipt_tables,
        ));

        all_calls.push((decoded_call, context));
    }

    Ok(count)
}

/// Track per-table call counts and optionally build stream payloads.
fn track_call_tables(
    matched_tables: &[&str],
    decoded_call: &DecodedCall,
    context: &EventContext,
    has_streams: bool,
    event_payloads: &mut Vec<crate::stream::EventPayload>,
    table_counts: &mut HashMap<String, (String, u64)>,
    receipt_tables: &HashSet<String>,
) -> u64 {
    for table_name in matched_tables {
        let entry = table_counts
            .entry((*table_name).to_string())
            .or_insert_with(|| (decoded_call.function_name.clone(), 0));
        entry.1 = entry.1.saturating_add(1);
        if has_streams {
            let include = receipt_tables.contains(*table_name);
            event_payloads.push(build_call_payload(
                decoded_call,
                context,
                table_name,
                include,
            ));
        }
    }
    matched_tables.len() as u64
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
                    block = filtered_log.block_number.as_u64(),
                    tx_index = filtered_log.tx_index.as_u32(),
                    error = %e,
                    "failed to decode log"
                );
            }
        }
    }
    decoded_events
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_receipt(cumulative: u64) -> reth_ethereum_primitives::Receipt {
        reth_ethereum_primitives::Receipt {
            cumulative_gas_used: cumulative,
            ..Default::default()
        }
    }

    #[test]
    fn compute_gas_used_first_tx() {
        let receipts = vec![make_receipt(21_000)];
        assert_eq!(compute_gas_used(&receipts, 0), 21_000);
    }

    #[test]
    fn anchor_violation_none_for_linked_blocks() {
        let h1 = B256::repeat_byte(0x01);
        let h2 = B256::repeat_byte(0x02);
        let known_hashes: HashMap<u64, B256> = [(10, h1), (11, h2)].into_iter().collect();
        let known_parents: HashMap<u64, B256> = std::iter::once((11, h1)).collect();
        // Block 11 claims parent h1 (hash of block 10) — linked.
        let triples = vec![(11u64, h2, h1)];
        assert_eq!(
            find_anchor_violation(&triples, &known_hashes, &known_parents),
            AnchorCheck::Ok
        );
    }

    #[test]
    fn anchor_violation_detected_against_stored_parent() {
        let stored_parent = B256::repeat_byte(0x01);
        let forged_parent = B256::repeat_byte(0xEE);
        let known_hashes: HashMap<u64, B256> = std::iter::once((10, stored_parent)).collect();
        let known_parents: HashMap<u64, B256> = HashMap::new();
        let triples = vec![(11u64, B256::repeat_byte(0x02), forged_parent)];
        assert_eq!(
            find_anchor_violation(&triples, &known_hashes, &known_parents),
            AnchorCheck::ParentMismatch {
                block: 11,
                actual: forged_parent,
                expected: stored_parent,
            }
        );
    }

    #[test]
    fn anchor_violation_detected_against_stored_child() {
        // A stored child (block 12) claims a parent hash that does not
        // match the newly verified block 11 — the stored segment is bad.
        let my_hash = B256::repeat_byte(0x02);
        let child_parent = B256::repeat_byte(0xEE);
        let known_hashes: HashMap<u64, B256> = std::iter::once((11, my_hash)).collect();
        let known_parents: HashMap<u64, B256> = std::iter::once((12, child_parent)).collect();
        let triples = vec![(11u64, my_hash, B256::repeat_byte(0x01))];
        assert_eq!(
            find_anchor_violation(&triples, &known_hashes, &known_parents),
            AnchorCheck::ChildMismatch {
                parent_block: 11,
                child_block: 12,
            }
        );
    }

    #[test]
    fn anchor_violation_skips_unknown_neighbors() {
        let known_hashes: HashMap<u64, B256> = HashMap::new();
        let known_parents: HashMap<u64, B256> = HashMap::new();
        // Neither neighbor of block 11 is known — no check possible.
        let triples = vec![(11u64, B256::repeat_byte(0x02), B256::repeat_byte(0xEE))];
        assert_eq!(
            find_anchor_violation(&triples, &known_hashes, &known_parents),
            AnchorCheck::Ok
        );
    }

    #[test]
    fn anchor_violation_skips_genesis() {
        let known_hashes: HashMap<u64, B256> = HashMap::new();
        let known_parents: HashMap<u64, B256> = HashMap::new();
        let triples = vec![(0u64, B256::repeat_byte(0x01), B256::ZERO)];
        assert_eq!(
            find_anchor_violation(&triples, &known_hashes, &known_parents),
            AnchorCheck::Ok
        );
    }

    fn skipped(number: u64) -> ProcessedItem<crate::chain::EthereumChain> {
        ProcessedItem::Skipped(crate::sync::SkippedHeader {
            number,
            hash: B256::repeat_byte(0x01),
            parent_hash: B256::repeat_byte(0x02),
        })
    }

    #[test]
    fn reorder_buffer_releases_contiguous_prefix_only() {
        let mut buffer = ReorderBuffer::new(10);
        let mut batch = Vec::new();

        // Out-of-order blocks beyond a hole stay buffered.
        buffer.push(skipped(12));
        buffer.push(skipped(13));
        buffer.drain_contiguous(&mut batch);
        assert!(batch.is_empty());
        assert_eq!(buffer.buffered(), 2);

        // Hole filled: the whole run drains in order.
        buffer.push(skipped(10));
        buffer.push(skipped(11));
        buffer.drain_contiguous(&mut batch);
        let numbers: Vec<u64> = batch.iter().map(ProcessedItem::number).collect();
        assert_eq!(numbers, vec![10, 11, 12, 13]);
        assert_eq!(buffer.buffered(), 0);

        // Duplicates below the frontier are dropped.
        buffer.push(skipped(9));
        assert_eq!(buffer.buffered(), 0);

        // Next contiguous block flows straight through.
        batch.clear();
        buffer.push(skipped(14));
        buffer.drain_contiguous(&mut batch);
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn compute_gas_used_second_tx() {
        let receipts = vec![make_receipt(21_000), make_receipt(63_000)];
        assert_eq!(compute_gas_used(&receipts, 1), 42_000);
    }

    #[test]
    fn compute_gas_used_third_tx() {
        let receipts = vec![
            make_receipt(50_000),
            make_receipt(120_000),
            make_receipt(200_000),
        ];
        assert_eq!(compute_gas_used(&receipts, 0), 50_000);
        assert_eq!(compute_gas_used(&receipts, 1), 70_000);
        assert_eq!(compute_gas_used(&receipts, 2), 80_000);
    }

    #[test]
    fn compute_gas_used_out_of_bounds() {
        let receipts = vec![make_receipt(21_000)];
        assert_eq!(compute_gas_used(&receipts, 5), 0);
    }

    #[test]
    fn compute_gas_used_empty_receipts() {
        let receipts: Vec<reth_ethereum_primitives::Receipt> = vec![];
        assert_eq!(compute_gas_used(&receipts, 0), 0);
    }
}
