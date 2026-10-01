//! Sync engine — orchestrates fetching blocks from peers.
//!
//! Peer feeder, ready set deduplication, quality-based peer selection,
//! and JoinSet task tracking.

use crate::chain::ChainTypes;
use crate::metrics::SieveMetrics;
use crate::p2p::{NetworkPeer, PeerPool};
use crate::sync::canonical::{
    establish_canonical_chain, CanonicalChain, QuorumPolicy, CANONICAL_SEGMENT_BLOCKS,
};
use crate::sync::fetch::{run_fetch_task, FetchTaskContext, FetchTaskParams};
use crate::sync::ingestion::{IngestionContext, IngestionPipeline};
use crate::sync::scheduler::{
    PeerHealthConfig, PeerHealthTracker, PeerWorkScheduler, SchedulerConfig,
};
use crate::sync::validation::AuthenticatedSegment;
use crate::sync::{FetchItem, SyncContext};
use crate::types::BlockNumber;
use alloy_primitives::B256;
use prometheus_client::metrics::gauge::Gauge;
use reth_network_api::PeerId;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{sleep, Instant};
use tracing::{debug, info, instrument, warn};

const MAX_CONCURRENT_FETCHES: usize = 32;

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

// Compile-time size assertions for hot types (reth pattern).
#[cfg(target_pointer_width = "64")]
const _: [(); 72] = [(); core::mem::size_of::<SyncOutcome>()];
// ProcessOutcome size varies with table_counts vec — skip assertion.

/// Verify the committed frontier by quorum before the API can serve any
/// row — and recover from a reorg that happened while stopped.
///
/// Must run BEFORE the API binds. The state machine:
/// - Fresh database → nothing to verify (the first segment establishes
///   the frontier).
/// - Existing state with a valid canonical marker → re-check the frontier
///   hash against a peer quorum. Match → verified, proceed. Divergence →
///   a reorg occurred while stopped (normal near an OP unsafe tip): find
///   the common ancestor via the SAME quorum's voters and roll back to
///   it (checkpoint + marker move atomically), then proceed. This is the
///   difference between a legitimate reorg and a poisoned database.
/// - Existing state without a marker, or an inconsistent marker → refused
///   inside [`committed_frontier_status`].
///
/// # Errors
///
/// Returns an error when the frontier cannot be verified or recovered.
pub async fn verify_or_recover_frontier<C: ChainTypes>(
    ctx: &SyncContext<C>,
    policy: &QuorumPolicy,
) -> eyre::Result<()> {
    verify_or_recover_frontier_with_archive(ctx, policy, None).await
}

/// Reauthenticate the retained archive boundary before verifying any P2P tail.
/// An archive-backed database without its reader fails closed before API startup.
pub async fn verify_or_recover_frontier_with_archive<C: ChainTypes>(
    ctx: &SyncContext<C>,
    policy: &QuorumPolicy,
    archive_reader: Option<&dyn super::validation::ArchiveRecoveryReader>,
) -> eyre::Result<()> {
    let status = crate::sync::canonical::committed_frontier_status(&ctx.db).await?;
    if let Some(archive) = crate::db::verification::archive_frontier(&ctx.db).await? {
        eyre::ensure!(
            C::NAME == "base",
            "archive provenance requires Base mainnet"
        );
        let reader = archive_reader.ok_or_else(|| eyre::eyre!("archive-backed database requires its pinned retained-header reader before startup; archive runtime is not enabled"))?;
        super::validation::verify_archive_recovery(&archive, reader)?;
    }
    let (checkpoint, stored) = match status {
        crate::sync::canonical::FrontierStatus::Fresh
        | crate::sync::canonical::FrontierStatus::Archive(_) => return Ok(()),
        crate::sync::canonical::FrontierStatus::Committed { checkpoint, hash } => {
            (checkpoint, hash)
        }
    };

    let Some(winner) = crate::p2p::until_stopped(
        &ctx.stop_rx,
        crate::sync::canonical::quorum_at(&ctx.pool, checkpoint, policy),
    )
    .await?
    else {
        return Ok(());
    };
    if winner.hash == stored {
        debug!(checkpoint, "committed frontier verified by quorum");
        return Ok(());
    }

    // Previously verified, now divergent → reorg while stopped.
    recover_startup_reorg(ctx, checkpoint, stored, &winner).await
}

/// Roll back a checkpoint that diverged from the canonical chain while
/// stopped, authorized by the quorum that detected the divergence.
async fn recover_startup_reorg<C: ChainTypes>(
    ctx: &SyncContext<C>,
    checkpoint: u64,
    stored: B256,
    winner: &crate::sync::canonical::QuorumWinner<C>,
) -> eyre::Result<()> {
    warn!(
        checkpoint,
        stored = %stored,
        canonical = %winner.hash,
        "startup: checkpoint diverged from canonical (reorg while stopped); rolling back"
    );
    let ancestor =
        crate::sync::reorg::find_common_ancestor(&ctx.db, &winner.voters, checkpoint, winner.hash)
            .await?
            .ok_or_else(|| {
                eyre::eyre!(
                    "checkpoint {checkpoint} diverged from the canonical chain, but no peer served \
                     a valid divergent chain to locate the common ancestor; retry with more peers \
                     or use a fresh database"
                )
            })?;
    crate::sync::follow::rollback_to_ancestor(
        &ctx.db,
        &ctx.handlers,
        &ctx.transfer_handlers,
        &ctx.call_handlers,
        &ctx.config,
        ancestor,
    )
    .await?;
    info!(ancestor, "startup reorg recovery complete");
    Ok(())
}

/// Run a block range in canonical segments: establish the canonical
/// header chain for each bounded segment via strict peer quorum, verify
/// its seam with committed state, then (and only then) run the sync
/// pipeline over it. Nothing is fetched, committed, or notified for a
/// segment whose canonical chain could not be established — a forged
/// header chain from a hostile peer therefore never reaches the
/// database or any stream sink.
///
/// This is the ONLY sync entry point for both historical windows and
/// follow-mode epochs.
///
/// # Errors
///
/// Returns an error if canonicalization fails (no quorum, no honest
/// serving peer, or committed state that does not connect to the
/// canonical chain — a poisoned database) or the sync itself fails.
pub async fn run_canonical_segments<C: ChainTypes>(
    start_block: BlockNumber,
    end_block: BlockNumber,
    ctx: SyncContext<C>,
) -> eyre::Result<SyncOutcome> {
    let policy = QuorumPolicy::default();
    let mut total = SyncOutcome::default();
    let mut seg_start = start_block.as_u64();
    let end = end_block.as_u64();

    // An existing database MUST have a verifiable left seam for every
    // segment; only a fresh database's very first block may lack one.
    let mut require_seam = ctx.db.has_indexed_state().await?;

    while seg_start <= end {
        if *ctx.stop_rx.borrow() {
            break;
        }
        let seg_end = end.min(seg_start.saturating_add(CANONICAL_SEGMENT_BLOCKS - 1));

        // Seam anchor: the stored hash directly below the segment. On a
        // resumed database this is the committed checkpoint block — a
        // poisoned checkpoint fails the seam and is refused inside
        // `establish_canonical_chain`.
        let frontier = match seg_start.checked_sub(1) {
            Some(parent) => ctx.db.get_block_hash(BlockNumber::new(parent)).await?,
            None => None,
        };
        let Some(canonical) = crate::p2p::until_stopped(
            &ctx.stop_rx,
            establish_canonical_chain(
                &ctx.pool,
                seg_start,
                seg_end,
                frontier,
                require_seam,
                &policy,
            ),
        )
        .await?
        else {
            break;
        };

        let outcome = run_sync(
            BlockNumber::new(seg_start),
            BlockNumber::new(seg_end),
            ctx.clone(),
            Arc::new(canonical),
        )
        .await?;
        total.accumulate(&outcome);

        if *ctx.stop_rx.borrow() {
            break;
        }
        // Never advance past a segment that is not fully committed.
        let checkpoint = ctx
            .db
            .last_checkpoint()
            .await?
            .map_or(0, BlockNumber::as_u64);
        if checkpoint < seg_end {
            return Err(eyre::eyre!(
                "canonical segment {seg_start}..={seg_end} ended with checkpoint at \
                 {checkpoint}; refusing to advance past an incomplete segment"
            ));
        }
        // After the first committed segment, every following segment has a
        // real stored predecessor, so the seam is now mandatory.
        require_seam = true;
        seg_start = seg_end.saturating_add(1);
    }

    Ok(total)
}

/// Run sync for a block range, fetching from the peer pool.
///
/// All headers come from `canonical` (the quorum-verified chain for this
/// exact range) — peers only serve bodies and receipts, fetched by
/// canonical hash and verified against canonical roots.
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
    canonical: Arc<CanonicalChain>,
) -> eyre::Result<SyncOutcome> {
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

    let pipeline = IngestionPipeline::start(
        IngestionContext::from_sync(&ctx),
        AuthenticatedSegment::peer(Arc::clone(&canonical)),
    )
    .await?;
    let payload_tx = pipeline.sender();
    let abort_rx = pipeline.abort_receiver();
    let (ready_tx, ready_rx) = mpsc::unbounded_channel::<NetworkPeer<C>>();
    let (feeder_shutdown_tx, feeder_shutdown_rx) = watch::channel(false);
    let feeder_handle =
        spawn_peer_feeder(Arc::clone(&ctx.pool), ready_tx.clone(), feeder_shutdown_rx);

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
        canonical: &canonical,
    };
    run_fetch_loop(&fetch_ctx, ready_rx, &ctx.stop_rx, &abort_rx).await;

    // Shutdown: feeder → workers → DB writer
    let _ = feeder_shutdown_tx.send(true);
    let _ = feeder_handle.await;
    drop(payload_tx); // closes the sequencer input, which closes the workers

    pipeline.finish().await
}

// ── Peer feeder ──────────────────────────────────────────────────────

fn spawn_peer_feeder<C: ChainTypes>(
    pool: Arc<PeerPool<C>>,
    ready_tx: mpsc::UnboundedSender<NetworkPeer<C>>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut known = HashMap::new();

        // The first tick is immediate, seeding already-connected sessions.
        let mut ticker = tokio::time::interval(Duration::from_millis(200));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let mut current = HashSet::new();
                    for peer in pool.snapshot() {
                        if peer.messages.to_session_tx.is_closed() {
                            continue;
                        }
                        current.insert(peer.peer_id);
                        let sender = &peer.messages.to_session_tx;
                        if !known.get(&peer.peer_id).is_some_and(|old: &tokio::sync::mpsc::Sender<_>| old.same_channel(sender)) {
                            known.insert(peer.peer_id, sender.clone());
                            let _ = ready_tx.send(peer);
                        }
                    }
                    known.retain(|peer_id, _| current.contains(peer_id));
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
    canonical: &'a Arc<CanonicalChain>,
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
                stop_rx,
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
    stop_rx: &watch::Receiver<bool>,
) -> bool {
    let mut abort_rx = abort_rx.clone();
    let mut stop_rx = stop_rx.clone();
    if *stop_rx.borrow() || *abort_rx.borrow() {
        return false;
    }
    let received = tokio::select! {
        biased;
        _ = stop_rx.wait_for(|stopped| *stopped) => return false,
        _ = abort_rx.wait_for(|aborted| *aborted) => return false,
        peer = ready_rx.recv() => peer,
    };
    let Some(peer) = received else {
        return false;
    };
    let Some(peer) = pool.get_peer(peer.peer_id) else {
        return true;
    };
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
    let queued = ready_peers.swap_remove(best_idx);
    ready_set.remove(&queued.peer_id);

    // Refresh the entire session: a reconnect replaces its sender and Status.
    let Some(peer) = ctx.pool.get_peer(queued.peer_id) else {
        return;
    };

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
        canonical: Arc::clone(ctx.canonical),
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

/// Non-blocking drain of ready channel, resolving current sessions from the pool.
fn drain_ready_peers<C: ChainTypes>(
    ready_rx: &mut mpsc::UnboundedReceiver<NetworkPeer<C>>,
    pool: &PeerPool<C>,
    ready_peers: &mut Vec<NetworkPeer<C>>,
    ready_set: &mut HashSet<PeerId>,
) {
    while let Ok(queued) = ready_rx.try_recv() {
        let Some(peer) = pool.get_peer(queued.peer_id) else {
            continue;
        };
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

#[cfg(test)]
#[expect(clippy::panic_in_result_fn, reason = "test assertions")]
mod peer_tests {
    use super::*;
    use crate::chain::EthereumChain;
    use reth_network_api::{PeerRequest, PeerRequestSender};

    type PeerReceiver = mpsc::Receiver<PeerRequest<<EthereumChain as ChainTypes>::Net>>;

    fn peer() -> (NetworkPeer<EthereumChain>, PeerReceiver) {
        let peer_id = PeerId::repeat_byte(1);
        let (tx, rx) = mpsc::channel(8);
        (
            NetworkPeer {
                peer_id,
                eth_version: reth_eth_wire::EthVersion::Eth68,
                messages: PeerRequestSender::new(peer_id, tx),
                head_number: 100,
                earliest_block: Some(0),
                last_success: Instant::now(),
            },
            rx,
        )
    }

    #[tokio::test]
    async fn shutdown_cancels_waiting_for_fetchable_peers() -> eyre::Result<()> {
        let pool = PeerPool::<EthereumChain>::new_empty();
        let (_ready_tx, mut ready_rx) = mpsc::unbounded_channel();
        let (_abort_tx, abort_rx) = watch::channel(false);
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut ready = Vec::new();
        let mut ready_set = HashSet::new();
        let waiting = await_first_peer(
            &mut ready_rx,
            &pool,
            &mut ready,
            &mut ready_set,
            &abort_rx,
            &stop_rx,
        );
        let stop = async {
            tokio::task::yield_now().await;
            stop_tx.send(true)
        };
        let (continued, stopped) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(waiting, stop)
        })
        .await?;
        stopped?;
        assert!(!continued);
        assert!(ready.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn already_stopped_or_aborted_fetch_wait_does_not_dispatch_ready_peer() -> eyre::Result<()>
    {
        for (stopped, aborted) in [(true, false), (false, true)] {
            let (peer, connection) = peer();
            let pool = PeerPool::fixture(vec![peer.clone()]);
            let (ready_tx, mut ready_rx) = mpsc::unbounded_channel();
            ready_tx.send(peer)?;
            let (_stop_tx, stop_rx) = watch::channel(stopped);
            let (_abort_tx, abort_rx) = watch::channel(aborted);
            let mut ready = Vec::new();
            let mut ready_set = HashSet::new();
            assert!(
                !await_first_peer(
                    &mut ready_rx,
                    &pool,
                    &mut ready,
                    &mut ready_set,
                    &abort_rx,
                    &stop_rx
                )
                .await
            );
            assert!(ready.is_empty());
            drop(connection);
        }
        Ok(())
    }

    #[tokio::test]
    async fn feeder_forwards_reconnected_session_even_without_observing_disconnect(
    ) -> eyre::Result<()> {
        let (old, old_connection) = peer();
        let pool = Arc::new(PeerPool::fixture(vec![old]));
        let (ready_tx, mut ready_rx) = mpsc::unbounded_channel();
        let (stop_tx, stop_rx) = watch::channel(false);
        let feeder = spawn_peer_feeder(Arc::clone(&pool), ready_tx, stop_rx);
        let first = tokio::time::timeout(Duration::from_secs(2), ready_rx.recv())
            .await?
            .ok_or_else(|| eyre::eyre!("feeder closed"))?;
        let (mut fresh, fresh_connection) = peer();
        fresh.head_number = 200;
        fresh.earliest_block = Some(50);
        let fresh_sender = fresh.messages.to_session_tx.clone();
        // Replace between feeder ticks, so there is never an empty snapshot.
        pool.add_peer(fresh);
        drop(old_connection);
        let replacement = tokio::time::timeout(Duration::from_secs(2), ready_rx.recv())
            .await?
            .ok_or_else(|| eyre::eyre!("feeder closed"))?;
        assert!(first.messages.to_session_tx.is_closed());
        assert!(replacement
            .messages
            .to_session_tx
            .same_channel(&fresh_sender));
        assert_eq!(replacement.head_number, 200);
        assert_eq!(replacement.earliest_block, Some(50));
        assert_eq!(pool.len(), 1);
        // An unchanged session must not be dispatched repeatedly by the feeder.
        assert!(
            tokio::time::timeout(Duration::from_millis(250), ready_rx.recv())
                .await
                .is_err()
        );
        stop_tx.send(true)?;
        feeder.await?;
        drop(fresh_connection);
        Ok(())
    }

    #[tokio::test]
    async fn ready_queue_uses_current_session_and_ignores_removed_or_closed_peers(
    ) -> eyre::Result<()> {
        let (old, old_connection) = peer();
        let pool = PeerPool::fixture(vec![old.clone()]);
        let (fresh, fresh_connection) = peer();
        let sender = fresh.messages.to_session_tx.clone();
        pool.add_peer(fresh);
        let (ready_tx, mut ready_rx) = mpsc::unbounded_channel();
        let mut ready = Vec::new();
        let mut ready_set = HashSet::new();
        ready_tx.send(old.clone())?;
        drain_ready_peers(&mut ready_rx, &pool, &mut ready, &mut ready_set);
        assert_eq!(ready.len(), 1);
        assert!(ready[0].messages.to_session_tx.same_channel(&sender));
        ready.clear();
        ready_set.clear();
        drop(fresh_connection);
        ready_tx.send(old.clone())?;
        drain_ready_peers(&mut ready_rx, &pool, &mut ready, &mut ready_set);
        assert!(ready.is_empty());
        pool.remove_peer(old.peer_id);
        ready_tx.send(old)?;
        drain_ready_peers(&mut ready_rx, &pool, &mut ready, &mut ready_set);
        assert!(ready.is_empty());
        drop(old_connection);
        Ok(())
    }
}
