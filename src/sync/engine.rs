//! Sync engine — orchestrates fetching blocks from peers.
//!
//! Peer feeder, ready set deduplication, quality-based peer selection,
//! and JoinSet task tracking.

use crate::config::IndexConfig;
use crate::db::{self, Database};
use crate::config::Selector;
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
use crate::sync::{BlockPayload, SyncContext};
use crate::toml_config::ResolvedFactory;
use crate::types::{BlockNumber, TxIndex};
use crate::{decode, filter};

use alloy_consensus::transaction::SignerRecoverable;
use alloy_consensus::Transaction;
use alloy_dyn_abi::JsonAbiExt;
use alloy_primitives::{Address, B256, TxKind};
use eyre::WrapErr;
use sqlx::Postgres;
use prometheus_client::metrics::gauge::Gauge;
use reth_network_api::PeerId;
use reth_primitives_traits::SealedHeader;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};
use tracing::{debug, info, instrument, warn};

const MAX_CONCURRENT_FETCHES: usize = 32;
const PAYLOAD_CHANNEL_SIZE: usize = 256;

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
#[derive(Debug)]
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
pub async fn run_sync(
    start_block: BlockNumber,
    end_block: BlockNumber,
    ctx: SyncContext,
) -> eyre::Result<SyncOutcome> {
    let started = Instant::now();
    let total_blocks = end_block.as_u64().saturating_sub(start_block.as_u64()) + 1;

    info!(start_block = start_block.as_u64(), end_block = end_block.as_u64(), total_blocks, "starting sync");

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
    let (payload_tx, payload_rx) = mpsc::channel::<BlockPayload>(PAYLOAD_CHANNEL_SIZE);
    let (ready_tx, ready_rx) = mpsc::unbounded_channel::<NetworkPeer>();

    // Local shutdown signal for the peer feeder (triggered when fetch loop exits)
    let (feeder_shutdown_tx, feeder_shutdown_rx) = watch::channel(false);

    // Spawn peer feeder
    let feeder_handle = spawn_peer_feeder(
        Arc::clone(&ctx.pool),
        ready_tx.clone(),
        feeder_shutdown_rx,
    );

    // Spawn payload consumer
    let consumer_handle = tokio::spawn(consume_payloads(
        payload_rx,
        Arc::clone(&ctx.config),
        Arc::clone(&ctx.db),
        Arc::clone(&ctx.handlers),
        Arc::clone(&ctx.metrics),
        Arc::clone(&ctx.factories),
        Arc::clone(&ctx.transfer_handlers),
        Arc::clone(&ctx.call_handlers),
        Arc::clone(&ctx.event_table_map),
        ctx.stream_dispatcher.clone(),
        ctx.is_backfill,
        Arc::clone(&ctx.receipt_tables),
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
    };
    run_fetch_loop(&fetch_ctx, ready_rx, &ctx.stop_rx).await;

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
        transfers_stored: stats.transfers_stored,
        calls_stored: stats.calls_stored,
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
    active_tasks: &'a Arc<AtomicUsize>,
    metrics: &'a Arc<SieveMetrics>,
    start_block: BlockNumber,
    end_block: BlockNumber,
    payload_tx: &'a mpsc::Sender<BlockPayload>,
    ready_tx: &'a mpsc::UnboundedSender<NetworkPeer>,
}

/// Mutable state carried across fetch loop iterations.
struct FetchLoopState {
    fetch_tasks: JoinSet<()>,
    ready_peers: Vec<NetworkPeer>,
    ready_set: HashSet<PeerId>,
    last_progress_check: Instant,
    last_progress_completed: u64,
}

#[instrument(skip_all)]
async fn run_fetch_loop(
    ctx: &FetchLoopContext<'_>,
    mut ready_rx: mpsc::UnboundedReceiver<NetworkPeer>,
    stop_rx: &watch::Receiver<bool>,
) {
    let fetch_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_FETCHES));
    let mut state = FetchLoopState {
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
async fn try_dispatch_iteration(
    ctx: &FetchLoopContext<'_>,
    semaphore: &Arc<Semaphore>,
    state: &mut FetchLoopState,
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
async fn await_first_peer(
    ready_rx: &mut mpsc::UnboundedReceiver<NetworkPeer>,
    pool: &PeerPool,
    ready_peers: &mut Vec<NetworkPeer>,
    ready_set: &mut HashSet<PeerId>,
) -> bool {
    let Some(mut peer) = ready_rx.recv().await else {
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
        payload_tx: ctx.payload_tx.clone(),
        ready_tx: ctx.ready_tx.clone(),
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
async fn check_peer_eligibility(
    ctx: &FetchLoopContext<'_>,
    peer: &NetworkPeer,
) -> Option<PeerAction> {
    // Cooling-down peers
    if ctx.peer_health.is_peer_cooling_down(peer.peer_id).await {
        return Some(PeerAction::Recycle(500));
    }

    // Stale-head detection: peer's probed head is below our work range
    if peer.head_number > 0 && peer.head_number < ctx.start_block.as_u64() {
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
    active_tasks: &AtomicUsize,
    pool: &PeerPool,
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

#[expect(clippy::too_many_arguments, reason = "grouping these into a struct would add complexity without benefit")]
#[instrument(skip_all)]
async fn consume_payloads(
    mut payload_rx: mpsc::Receiver<BlockPayload>,
    config: Arc<IndexConfig>,
    db: Arc<Database>,
    handlers: Arc<HandlerRegistry>,
    metrics: Arc<SieveMetrics>,
    factories: Arc<Vec<ResolvedFactory>>,
    transfer_handlers: Arc<TransferRegistry>,
    call_handlers: Arc<CallRegistry>,
    event_table_map: Arc<HashMap<String, (String, String)>>,
    stream_dispatcher: Option<Arc<crate::stream::StreamDispatcher>>,
    is_backfill: bool,
    receipt_tables: Arc<HashSet<String>>,
) -> ConsumerStats {
    let mut stats = ConsumerStats::default();
    let mut last_log = Instant::now();
    let mut max_indexed_block: u64 = 0;

    let ctx = ProcessContext {
        config: &config,
        db: &db,
        handlers: &handlers,
        factories: &factories,
        transfer_handlers: &transfer_handlers,
        call_handlers: &call_handlers,
        event_table_map: &event_table_map,
        has_streams: stream_dispatcher.is_some(),
        receipt_tables: &receipt_tables,
    };

    while let Some(payload) = payload_rx.recv().await {
        stats.blocks_fetched = stats.blocks_fetched.saturating_add(1);
        stats.total_receipts = stats
            .total_receipts
            .saturating_add(payload.receipts().len() as u64);

        match process_block_events(&payload, &ctx).await {
            Ok(outcome) => {
                stats.events_matched = stats.events_matched.saturating_add(outcome.matched);
                stats.events_decoded = stats.events_decoded.saturating_add(outcome.decoded);
                stats.events_stored = stats.events_stored.saturating_add(outcome.stored);
                stats.transfers_stored = stats.transfers_stored.saturating_add(outcome.transfers);
                stats.calls_stored = stats.calls_stored.saturating_add(outcome.calls);

                // Update Prometheus counters
                metrics.blocks_indexed.inc();
                metrics.events_matched.inc_by(outcome.matched);
                metrics.events_stored.inc_by(outcome.stored);
                metrics.transfers_stored.inc_by(outcome.transfers);
                metrics.calls_stored.inc_by(outcome.calls);

                // Only advance the gauge forward (payloads arrive out of order)
                let block_num = payload.header().number;
                if block_num > max_indexed_block {
                    max_indexed_block = block_num;
                    metrics.indexed_block.set(block_num as i64);
                }

                // Dispatch stream notifications
                if let Some(ref dispatcher) = stream_dispatcher {
                    if !outcome.table_counts.is_empty() {
                        let tables = outcome
                            .table_counts
                            .into_iter()
                            .map(|(name, event, count)| {
                                crate::stream::TableNotification { name, event, count }
                            })
                            .collect();
                        dispatcher.send(
                            crate::stream::BlockNotification {
                                block_number: block_num,
                                block_timestamp: payload.header().timestamp,
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
            Err(e) => {
                warn!(
                    block = payload.header().number,
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
                transfers_stored = stats.transfers_stored,
                calls_stored = stats.calls_stored,
                block_number = payload.header().number,
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
    factories: &'a [ResolvedFactory],
    transfer_handlers: &'a TransferRegistry,
    call_handlers: &'a CallRegistry,
    /// Maps `"contract:event"` key → `(table_name, event_name)`.
    event_table_map: &'a HashMap<String, (String, String)>,
    /// Whether streams are configured (gates event payload collection).
    has_streams: bool,
    /// Table names with `include_receipts = true` (for streaming enrichment).
    receipt_tables: &'a HashSet<String>,
}

/// Compute the sealed hash for a block header.
fn compute_block_hash(header: &reth_primitives_traits::Header) -> alloy_primitives::B256 {
    SealedHeader::seal_slow(header.clone()).hash()
}

/// Commit block hash and checkpoint for a block with no events to store.
async fn commit_block_bookkeeping(
    db_ref: &Database,
    block_number: BlockNumber,
    block_hash: &[u8],
) -> eyre::Result<()> {
    let mut tx = db_ref.begin().await?;
    db::store_block_hash(&mut tx, block_number, block_hash).await?;
    db::update_checkpoint(&mut tx, block_number).await?;
    tx.commit()
        .await
        .wrap_err_with(|| format!("failed to commit block {}", block_number.as_u64()))?;
    Ok(())
}

/// An empty `ProcessOutcome` for early returns.
const fn empty_outcome(matched: u64) -> ProcessOutcome {
    ProcessOutcome { matched, decoded: 0, stored: 0, transfers: 0, calls: 0, table_counts: vec![], event_payloads: vec![] }
}

/// Filter, decode, and store events from a single block payload.
#[instrument(skip_all, fields(block_number = payload.header().number))]
async fn process_block_events(
    payload: &BlockPayload,
    ctx: &ProcessContext<'_>,
) -> eyre::Result<ProcessOutcome> {
    let block_number = BlockNumber::new(payload.header().number);
    let block_hash = compute_block_hash(payload.header());

    // Step 0: factory pre-scan (discovers new child contracts)
    if !ctx.factories.is_empty() {
        let discoveries = filter::scan_factory_events(payload, ctx.factories);
        for d in &discoveries {
            register_factory_child(ctx, d).await?;
        }
    }

    // Step 1: filter
    let matched = filter::filter_block(payload, ctx.config);
    let matched_count = matched.len() as u64;
    let has_transfers = !ctx.transfer_handlers.is_empty();
    let has_calls = !ctx.call_handlers.is_empty();

    if matched.is_empty() && !has_transfers && !has_calls {
        commit_block_bookkeeping(ctx.db, block_number, block_hash.as_slice()).await?;
        return Ok(empty_outcome(0));
    }

    // Step 2: decode (CPU work, no DB)
    let decoded_events = decode_matched_logs(&matched, ctx.config);
    let decoded_count = decoded_events.len() as u64;

    if decoded_events.is_empty() && !has_transfers && !has_calls {
        commit_block_bookkeeping(ctx.db, block_number, block_hash.as_slice()).await?;
        return Ok(empty_outcome(matched_count));
    }

    // Step 3: store in one transaction per block
    let mut tx = ctx.db.begin().await?;
    let mut stored_count = 0u64;
    let mut table_counts: HashMap<String, (String, u64)> = HashMap::new();
    let mut event_payloads: Vec<crate::stream::EventPayload> = Vec::new();

    db::store_block_hash(&mut tx, block_number, block_hash.as_slice()).await?;

    let mut sender_cache: HashMap<TxIndex, Address> = HashMap::new();

    for event in &decoded_events {
        let event_context =
            build_event_context(payload, block_hash, event, &mut sender_cache)?;
        let dispatched = ctx.handlers.dispatch(event, &event_context, &mut tx).await?;
        if dispatched > 0 {
            track_event_table(ctx.event_table_map, event, dispatched, &mut table_counts);
            if ctx.has_streams {
                collect_event_payload(ctx.event_table_map, event, &event_context, ctx.receipt_tables, &mut event_payloads);
            }
        }
        stored_count = stored_count.saturating_add(dispatched);
    }

    let transfer_count = store_transfers(
        payload, block_hash, &mut sender_cache, ctx, &mut tx, &mut table_counts,
        &mut event_payloads,
    ).await?;

    let call_count = store_calls(
        payload, block_hash, &mut sender_cache, ctx, &mut tx, &mut table_counts,
        &mut event_payloads,
    ).await?;

    db::update_checkpoint(&mut tx, block_number).await?;
    tx.commit()
        .await
        .wrap_err_with(|| format!("failed to commit block {}", block_number.as_u64()))?;

    let table_counts = table_counts
        .into_iter()
        .map(|(table, (event, count))| (table, event, count))
        .collect();

    Ok(ProcessOutcome {
        matched: matched_count,
        decoded: decoded_count,
        stored: stored_count,
        transfers: transfer_count,
        calls: call_count,
        table_counts,
        event_payloads,
    })
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

/// Scan and store native transfers, updating table_counts and event_payloads.
async fn store_transfers(
    payload: &BlockPayload,
    block_hash: B256,
    sender_cache: &mut HashMap<TxIndex, Address>,
    ctx: &ProcessContext<'_>,
    tx: &mut sqlx::Transaction<'_, Postgres>,
    table_counts: &mut HashMap<String, (String, u64)>,
    event_payloads: &mut Vec<crate::stream::EventPayload>,
) -> eyre::Result<u64> {
    if ctx.transfer_handlers.is_empty() {
        return Ok(0);
    }
    let count = scan_and_store_transfers(
        payload, block_hash, sender_cache, ctx.transfer_handlers, tx,
        ctx.has_streams, event_payloads, table_counts, ctx.receipt_tables,
    ).await?;
    Ok(count)
}

/// Scan and store function calls, updating table_counts and event_payloads.
async fn store_calls(
    payload: &BlockPayload,
    block_hash: B256,
    sender_cache: &mut HashMap<TxIndex, Address>,
    ctx: &ProcessContext<'_>,
    tx: &mut sqlx::Transaction<'_, Postgres>,
    table_counts: &mut HashMap<String, (String, u64)>,
    event_payloads: &mut Vec<crate::stream::EventPayload>,
) -> eyre::Result<u64> {
    if ctx.call_handlers.is_empty() {
        return Ok(0);
    }
    let count = scan_and_store_calls(
        payload, block_hash, sender_cache, ctx.config, ctx.call_handlers, tx,
        ctx.has_streams, event_payloads, table_counts, ctx.receipt_tables,
    ).await?;
    Ok(count)
}

/// Compute per-transaction gas used from cumulative gas values.
///
/// For the first transaction in a block, `gas_used == cumulative_gas_used`.
/// For subsequent transactions, `gas_used = cumulative[i] - cumulative[i-1]`.
fn compute_gas_used(receipts: &[reth_ethereum_primitives::Receipt], tx_idx: usize) -> u64 {
    let cumulative = receipts.get(tx_idx).map_or(0, |r| r.cumulative_gas_used);
    if tx_idx == 0 {
        cumulative
    } else {
        cumulative.saturating_sub(
            receipts.get(tx_idx - 1).map_or(0, |r| r.cumulative_gas_used),
        )
    }
}

/// Build an [`EventContext`] from a block payload for a given decoded event.
///
/// Caches sender recovery per `tx_index` to avoid redundant ECDSA work
/// when multiple events originate from the same transaction.
fn build_event_context(
    payload: &BlockPayload,
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
    let cumulative = payload.receipts().get(tx_idx_usize).map_or(0, |r| r.cumulative_gas_used);
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

/// Register a factory-discovered child contract in both config and database.
async fn register_factory_child(
    ctx: &ProcessContext<'_>,
    discovery: &filter::FactoryDiscovery,
) -> eyre::Result<()> {
    // Find the contract index by name
    let Some(contract_idx) = ctx
        .config
        .contracts
        .iter()
        .position(|c| c.name == discovery.child_contract_name)
    else {
        warn!(
            factory = %discovery.child_contract_name,
            "factory child references unknown contract"
        );
        return Ok(());
    };

    // Register in config (returns false if already known)
    if ctx
        .config
        .register_factory_child(discovery.child_address, contract_idx)
    {
        // Persist to database
        let mut tx = ctx.db.begin().await?;
        db::store_factory_child(
            &mut tx,
            &discovery.child_contract_name,
            &discovery.child_address,
            discovery.block_number,
        )
        .await?;
        tx.commit()
            .await
            .wrap_err("failed to commit factory child registration")?;

        info!(
            factory = %discovery.child_contract_name,
            child = ?discovery.child_address,
            block = discovery.block_number,
            "registered new factory child"
        );
    }

    Ok(())
}

/// Scan block transactions for native ETH transfers and store them.
///
/// Iterates all transactions in the block body, skipping zero-value,
/// contract-creation, and reverted transactions. Uses the shared sender cache.
///
/// Returns the number of transfers stored.
#[expect(clippy::too_many_arguments, reason = "per-table counting adds table_counts")]
async fn scan_and_store_transfers(
    payload: &BlockPayload,
    block_hash: B256,
    sender_cache: &mut HashMap<TxIndex, Address>,
    transfer_handlers: &TransferRegistry,
    tx: &mut sqlx::Transaction<'_, Postgres>,
    has_streams: bool,
    event_payloads: &mut Vec<crate::stream::EventPayload>,
    table_counts: &mut HashMap<String, (String, u64)>,
    receipt_tables: &HashSet<String>,
) -> eyre::Result<u64> {
    let block_number = BlockNumber::new(payload.header().number);
    let receipts = payload.receipts();
    let mut count = 0u64;

    for (tx_idx, tx_signed) in payload.body().transactions.iter().enumerate() {
        let tx_index = TxIndex::from_usize(tx_idx);
        // Skip zero-value transactions (no ETH transfer)
        if tx_signed.value().is_zero() {
            continue;
        }

        // Skip reverted transactions (value not actually transferred)
        if !receipts.get(tx_idx).is_some_and(|r| r.success) {
            continue;
        }

        // Skip contract creations (no recipient)
        let to_address = match tx_signed.kind() {
            TxKind::Call(addr) => addr,
            TxKind::Create => continue,
        };

        // Recover sender (use cache)
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

        let context = EventContext {
            block_timestamp: payload.header().timestamp,
            block_hash,
            tx_from: from_address,
            tx_to: Some(to_address),
            tx_value: tx_signed.value(),
            tx_gas_price: tx_signed.effective_gas_price(payload.header().base_fee_per_gas),
            tx_gas_used: compute_gas_used(receipts, tx_idx),
            tx_nonce: tx_signed.nonce(),
            cumulative_gas_used: receipts.get(tx_idx).map_or(0, |r| r.cumulative_gas_used),
            tx_status: true,
        };

        let dispatched = transfer_handlers.dispatch(&transfer, &context, tx).await?;
        if dispatched > 0 {
            for table_name in transfer_handlers.matched_table_names(&transfer) {
                let entry = table_counts
                    .entry(table_name.to_string())
                    .or_insert_with(|| ("transfer".to_string(), 0));
                entry.1 = entry.1.saturating_add(1);
                if has_streams {
                    let include = receipt_tables.contains(table_name);
                    event_payloads.push(build_transfer_payload(&transfer, &context, table_name, include));
                }
            }
        }
        count = count.saturating_add(dispatched);
    }

    Ok(count)
}

/// Scan block transactions for function calls and store them.
///
/// Iterates all transactions in the block body, matching calldata selectors
/// against configured functions. Skips contract creations, short calldata,
/// and reverted transactions. Uses `JsonAbiExt::abi_decode_input` to decode
/// the calldata arguments.
///
/// Returns the number of calls stored.
#[expect(clippy::too_many_arguments, reason = "per-table counting adds table_counts")]
async fn scan_and_store_calls(
    payload: &BlockPayload,
    block_hash: B256,
    sender_cache: &mut HashMap<TxIndex, Address>,
    config: &IndexConfig,
    call_handlers: &CallRegistry,
    tx: &mut sqlx::Transaction<'_, Postgres>,
    has_streams: bool,
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

        // Skip transactions with no calldata or too short for a selector
        if input.len() < 4 {
            continue;
        }

        // Skip contract creations (no recipient)
        let to_address = match tx_signed.kind() {
            TxKind::Call(addr) => addr,
            TxKind::Create => continue,
        };

        // Look up contract config for this address
        let Some(contract) = config.contract_for_address(&to_address) else {
            continue;
        };

        // Extract 4-byte function selector
        let selector = Selector::from_slice(&input[..4]);

        // Look up function ABI
        let Some(function) = contract.functions.get(&selector) else {
            continue;
        };

        // Skip reverted transactions (call had no effect)
        if !receipts.get(tx_idx).is_some_and(|r| r.success) {
            continue;
        }

        // Decode function arguments (calldata without 4-byte selector prefix)
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

        // Build decoded params from function inputs + decoded values
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

        // Recover sender (use cache)
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

        let context = EventContext {
            block_timestamp: payload.header().timestamp,
            block_hash,
            tx_from: from_address,
            tx_to: Some(to_address),
            tx_value: tx_signed.value(),
            tx_gas_price: tx_signed.effective_gas_price(payload.header().base_fee_per_gas),
            tx_gas_used: compute_gas_used(receipts, tx_idx),
            tx_nonce: tx_signed.nonce(),
            cumulative_gas_used: receipts.get(tx_idx).map_or(0, |r| r.cumulative_gas_used),
            tx_status: true,
        };

        let dispatched = call_handlers.dispatch(&decoded_call, &context, tx).await?;
        if dispatched > 0 {
            for table_name in call_handlers.matched_table_names(
                &decoded_call.contract_name,
                &decoded_call.function_name,
            ) {
                let entry = table_counts
                    .entry(table_name.to_string())
                    .or_insert_with(|| (decoded_call.function_name.clone(), 0));
                entry.1 = entry.1.saturating_add(1);
                if has_streams {
                    let include = receipt_tables.contains(table_name);
                    event_payloads.push(build_call_payload(&decoded_call, &context, table_name, include));
                }
            }
        }
        count = count.saturating_add(dispatched);
    }

    Ok(count)
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
