//! Head-following mode.
//!
//! After historical sync completes, follows new blocks in real-time.
//! Each "epoch" discovers the current chain head via P2P, runs reorg
//! preflight, then syncs the gap. Sleeps near the tip to avoid busy-looping.

use crate::chain::ChainTypes;
use crate::db::{self, Database};
use crate::handler::{CallRegistry, HandlerRegistry, TransferRegistry};
use crate::metrics::SieveMetrics;
use crate::p2p::{discover_head_p2p, PeerPool};
use crate::stream::StreamDispatcher;
use crate::sync::reorg;
use crate::sync::{run_canonical_segments, ReorgCheck, SyncContext};
use crate::toml_config::ResolvedFactory;
use crate::types::BlockNumber;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::{debug, info, instrument, warn};

/// Shared state for the follow loop, reducing argument counts.
struct FollowContext<C: ChainTypes> {
    pool: Arc<PeerPool<C>>,
    start_block: BlockNumber,
    config: Arc<crate::config::IndexConfig>,
    db: Arc<Database>,
    handlers: Arc<HandlerRegistry>,
    metrics: Arc<SieveMetrics>,
    stop_rx: watch::Receiver<bool>,
    factories: Arc<Vec<ResolvedFactory>>,
    transfer_handlers: Arc<TransferRegistry>,
    call_handlers: Arc<CallRegistry>,
    stream_dispatcher: Option<Arc<StreamDispatcher>>,
    event_table_map: Arc<HashMap<String, (String, String)>>,
    receipt_tables: Arc<std::collections::HashSet<String>>,
    bloom_filter: Option<Arc<crate::filter::BloomFilter>>,
    head_seen_rx: watch::Receiver<u64>,
    verbose: bool,
    worker_count: usize,
}

/// Run the follow loop: discover head, preflight reorg, sync gap, repeat.
///
/// Exits when `stop_rx` signals shutdown.
///
/// # Errors
///
/// Returns an error on unrecoverable failures (DB errors, deep reorgs).
#[instrument(skip_all, fields(start_block = start_block.as_u64()))]
pub async fn run_follow_loop<C: ChainTypes>(
    start_block: BlockNumber,
    ctx: SyncContext<C>,
) -> eyre::Result<()> {
    info!(start_block = start_block.as_u64(), "entering follow mode");

    let baseline = ctx.db.last_checkpoint().await?.map_or_else(
        || start_block.as_u64().saturating_sub(1),
        BlockNumber::as_u64,
    );
    let (head_seen_tx, head_seen_rx) = watch::channel(0u64);
    tokio::spawn(run_head_tracker(
        Arc::clone(&ctx.pool),
        head_seen_tx,
        ctx.stop_rx.clone(),
        baseline,
    ));

    let fctx = FollowContext::<C> {
        pool: ctx.pool,
        start_block,
        config: ctx.config,
        db: ctx.db,
        handlers: ctx.handlers,
        metrics: ctx.metrics,
        stop_rx: ctx.stop_rx,
        factories: ctx.factories,
        transfer_handlers: ctx.transfer_handlers,
        call_handlers: ctx.call_handlers,
        stream_dispatcher: ctx.stream_dispatcher,
        event_table_map: ctx.event_table_map,
        receipt_tables: ctx.receipt_tables,
        bloom_filter: ctx.bloom_filter,
        head_seen_rx,
        verbose: ctx.verbose,
        worker_count: ctx.worker_count,
    };

    let mut last_heartbeat = Instant::now();
    let mut phase = FollowPhase::Discovering;
    let mut sync_failures = 0u32;

    // Background spinner for the discovering phase (80ms tick)
    let mut spinner_stop = if fctx.verbose {
        None
    } else {
        Some(spawn_discovering_spinner(Arc::clone(&fctx.pool)))
    };

    while !is_stopped(&fctx.stop_rx) {
        let evicted = fctx.pool.evict_stale(Duration::from_secs(120));
        if evicted > 0 {
            info!(evicted, peers = fctx.pool.len(), "evicted stale peers");
        }
        if last_heartbeat.elapsed() >= Duration::from_secs(30) {
            print_heartbeat(&fctx, &phase).await?;
            last_heartbeat = Instant::now();
        }
        if run_follow_epoch(&fctx, &mut phase, &mut spinner_stop, &mut sync_failures).await? {
            break;
        }
    }

    info!("follow loop exited");
    Ok(())
}

/// Spawn a background task that animates the "discovering" spinner at 80ms.
///
/// Returns the sender to stop the spinner (send `true` to cancel).
fn spawn_discovering_spinner<C: ChainTypes>(pool: Arc<PeerPool<C>>) -> watch::Sender<bool> {
    let (tx, mut rx) = watch::channel(false);
    tokio::spawn(async move {
        let mut spinner = crate::ui::Spinner::new();
        loop {
            crate::ui::print_discovering(pool.len(), spinner.frame());
            tokio::select! {
                () = tokio::time::sleep(Duration::from_millis(80)) => {}
                _ = rx.changed() => break,
            }
        }
        crate::ui::clear_line();
    });
    tx
}

/// Print the periodic heartbeat based on current phase.
async fn print_heartbeat<C: ChainTypes>(
    ctx: &FollowContext<C>,
    phase: &FollowPhase,
) -> eyre::Result<()> {
    match phase {
        FollowPhase::Discovering => {
            // Spinner background task handles pretty output
            if ctx.verbose {
                info!(peers = ctx.pool.len(), "discovering chain head");
            }
        }
        FollowPhase::Following => {
            if ctx.verbose {
                info!(
                    peers = ctx.pool.len(),
                    "follow mode: waiting for new blocks"
                );
            } else {
                let checkpoint = ctx
                    .db
                    .last_checkpoint()
                    .await?
                    .map_or(ctx.start_block.as_u64(), BlockNumber::as_u64);
                let ts = ctx
                    .metrics
                    .last_block_timestamp
                    .load(std::sync::atomic::Ordering::Relaxed);
                crate::ui::print_follow_status(checkpoint, ctx.pool.len(), ts, None);
            }
        }
    }
    Ok(())
}

/// Maximum number of blocks a single follow epoch may sync.
///
/// Bounds the engine's per-epoch allocation against inflated peer-reported
/// heads; larger gaps are covered by consecutive epochs.
const MAX_EPOCH_BLOCKS: u64 = 50_000;

/// Maximum consecutive failed sync epochs before the follow loop gives up.
///
/// A single failure is retried (the failing block is usually re-fetched
/// from a different peer, and a raced reorg is handled by the next
/// preflight); persistent failures indicate an unrecoverable problem.
const MAX_SYNC_FAILURES: u32 = 5;

/// Run a single follow epoch. Returns `true` if the loop should exit.
async fn run_follow_epoch<C: ChainTypes>(
    ctx: &FollowContext<C>,
    phase: &mut FollowPhase,
    spinner_stop: &mut Option<watch::Sender<bool>>,
    sync_failures: &mut u32,
) -> eyre::Result<bool> {
    match discover_gap(ctx).await? {
        EpochAction::Wait => Ok(wait_or_stop(&ctx.stop_rx, Duration::from_secs(1)).await),
        EpochAction::Reorg => Ok(false),
        EpochAction::Sync { next_block, head } => {
            // Stop discovering spinner before sync starts
            if let Some(tx) = spinner_stop.take() {
                let _ = tx.send(true);
                // Brief yield to let spinner task clear the line
                tokio::task::yield_now().await;
            }
            let gap = match sync_epoch(ctx, next_block, head).await {
                Ok(gap) => gap,
                Err(err) => return handle_sync_failure(ctx, err, sync_failures).await,
            };
            *sync_failures = 0;
            *phase = FollowPhase::Following;
            if gap <= 2 {
                ctx.metrics.is_ready.store(true, Ordering::Relaxed);
            }
            let should_exit =
                gap <= 2 && wait_or_stop(&ctx.stop_rx, Duration::from_millis(500)).await;
            Ok(should_exit)
        }
    }
}

/// Handle a failed sync epoch: retry with a cap on consecutive failures.
///
/// Returns `Ok(should_exit)` when the failure is retriable, or the wrapped
/// error once [`MAX_SYNC_FAILURES`] consecutive epochs have failed.
/// A [`crate::sync::FactoryStateError`] is never retried: it means the
/// in-memory factory state may still be poisoned, and a subsequent sync
/// could succeed against that state and advance the checkpoint.
async fn handle_sync_failure<C: ChainTypes>(
    ctx: &FollowContext<C>,
    err: eyre::Report,
    sync_failures: &mut u32,
) -> eyre::Result<bool> {
    if err
        .downcast_ref::<crate::sync::FactoryStateError>()
        .is_some()
    {
        return Err(err.wrap_err("factory state could not be restored; refusing to retry"));
    }
    *sync_failures += 1;
    if *sync_failures >= MAX_SYNC_FAILURES {
        return Err(err.wrap_err(format!(
            "sync epoch failed {MAX_SYNC_FAILURES} consecutive times"
        )));
    }
    warn!(
        error = ?err,
        attempt = *sync_failures,
        max_attempts = MAX_SYNC_FAILURES,
        "sync epoch failed; retrying next epoch"
    );
    Ok(wait_or_stop(&ctx.stop_rx, Duration::from_secs(1)).await)
}

/// Check if the stop signal has been received.
fn is_stopped(stop_rx: &watch::Receiver<bool>) -> bool {
    let stopped = *stop_rx.borrow();
    if stopped {
        info!("follow loop: shutdown signal received");
    }
    stopped
}

/// What the follow loop should do this epoch.
enum EpochAction {
    /// No new blocks or at tip — wait before retrying.
    Wait,
    /// Reorg detected and rolled back — restart the loop immediately.
    Reorg,
    /// New blocks available — sync from `next_block` to `head`.
    Sync { next_block: u64, head: u64 },
}

/// Follow loop phase for pretty UI.
enum FollowPhase {
    /// Still trying to discover the chain head from peers.
    Discovering,
    /// Caught up to tip, waiting for new blocks.
    Following,
}

/// Discover the current chain head and determine the epoch action.
async fn discover_gap<C: ChainTypes>(ctx: &FollowContext<C>) -> eyre::Result<EpochAction> {
    let baseline = ctx.db.last_checkpoint().await?.map_or_else(
        || ctx.start_block.as_u64().saturating_sub(1),
        BlockNumber::as_u64,
    );

    let Some(observed_head) = discover_head_p2p(&ctx.pool, baseline, 3, 1024).await? else {
        debug!("no new blocks discovered, waiting");
        return Ok(EpochAction::Wait);
    };

    // If the gap fills most of the 1024-block probe window, the true head
    // is likely further. Use peer-reported heads to size the full gap so
    // run_sync processes it in one fast batch instead of many small epochs.
    let effective_head = if observed_head >= baseline + 1000 {
        ctx.pool
            .best_peer_head()
            .map_or(observed_head, |peer_head| observed_head.max(peer_head))
    } else {
        observed_head
    };

    // Clamp the per-epoch gap: peer heads are peer-influenced data, and the
    // sync engine materializes the block range. A genuinely large gap is
    // simply covered by successive epochs; a lying peer can waste at most
    // one bounded epoch.
    let effective_head = effective_head.min(baseline.saturating_add(MAX_EPOCH_BLOCKS));

    ctx.metrics.chain_head.set(effective_head as i64);

    let next_block = baseline + 1;
    if next_block > effective_head {
        debug!(next_block, effective_head, "at tip, waiting");
        return Ok(EpochAction::Wait);
    }

    if should_rollback_reorg(
        &ctx.db,
        &ctx.pool,
        &ctx.handlers,
        &ctx.transfer_handlers,
        &ctx.call_handlers,
        &ctx.config,
        baseline,
        ctx.start_block.as_u64(),
        &ctx.stop_rx,
    )
    .await?
    {
        return Ok(EpochAction::Reorg);
    }

    Ok(EpochAction::Sync {
        next_block,
        head: effective_head,
    })
}

/// Run one sync epoch, returning the gap size.
async fn sync_epoch<C: ChainTypes>(
    ctx: &FollowContext<C>,
    next_block: u64,
    head: u64,
) -> eyre::Result<u64> {
    let gap = head.saturating_sub(next_block) + 1;
    info!(next_block, head, gap, "follow epoch: syncing gap");

    let sync_ctx = SyncContext::<C> {
        pool: Arc::clone(&ctx.pool),
        config: Arc::clone(&ctx.config),
        db: Arc::clone(&ctx.db),
        handlers: Arc::clone(&ctx.handlers),
        metrics: Arc::clone(&ctx.metrics),
        stop_rx: ctx.stop_rx.clone(),
        factories: Arc::clone(&ctx.factories),
        transfer_handlers: Arc::clone(&ctx.transfer_handlers),
        call_handlers: Arc::clone(&ctx.call_handlers),
        stream_dispatcher: ctx.stream_dispatcher.clone(),
        event_table_map: Arc::clone(&ctx.event_table_map),
        is_backfill: false,
        receipt_tables: Arc::clone(&ctx.receipt_tables),
        bloom_filter: ctx.bloom_filter.clone(),
        head_seen_rx: Some(ctx.head_seen_rx.clone()),
        verbose: ctx.verbose,
        worker_count: ctx.worker_count,
    };

    let outcome = run_canonical_segments(
        BlockNumber::new(next_block),
        BlockNumber::new(head),
        sync_ctx,
    )
    .await?;

    if !is_stopped(&ctx.stop_rx) {
        if ctx.verbose {
            info!(
                blocks = outcome.blocks_fetched,
                events_stored = outcome.events_stored,
                elapsed_ms = outcome.elapsed.as_millis() as u64,
                "follow epoch complete"
            );
        } else {
            let ts = ctx
                .metrics
                .last_block_timestamp
                .load(std::sync::atomic::Ordering::Relaxed);
            crate::ui::print_follow_status(head, ctx.pool.len(), ts, None);
        }
    }

    Ok(gap)
}

/// Run reorg preflight and rollback if needed.
/// Returns `true` if a reorg was handled (caller should `continue` the loop)
/// or if we should wait and retry. Returns `false` to proceed with sync.
#[expect(
    clippy::too_many_arguments,
    reason = "follow loop passes individual fields"
)]
async fn should_rollback_reorg<C: ChainTypes>(
    db: &Database,
    pool: &PeerPool<C>,
    handlers: &HandlerRegistry,
    transfer_handlers: &TransferRegistry,
    call_handlers: &CallRegistry,
    config: &crate::config::IndexConfig,
    baseline: u64,
    start_block: u64,
    stop_rx: &watch::Receiver<bool>,
) -> eyre::Result<bool> {
    if baseline < start_block {
        return Ok(false);
    }

    let policy = crate::sync::canonical::QuorumPolicy::default();
    let Some(check) =
        crate::p2p::until_stopped(stop_rx, reorg::preflight_reorg(db, pool, baseline, &policy))
            .await?
    else {
        return Ok(true);
    };
    match check {
        ReorgCheck::NoReorg => Ok(false),
        ReorgCheck::Inconclusive => {
            debug!("reorg check inconclusive, retrying");
            wait_or_stop(stop_rx, Duration::from_secs(1)).await;
            Ok(true)
        }
        ReorgCheck::ReorgDetected {
            anchors,
            expected_tip,
        } => {
            execute_rollback(
                db,
                handlers,
                transfer_handlers,
                call_handlers,
                config,
                &anchors,
                expected_tip,
                baseline,
            )
            .await?;
            Ok(true)
        }
    }
}

/// Find the common ancestor and roll back all indexed data above it.
///
/// Skips the rollback (returning `Ok`) when no anchor peer supplies a
/// valid divergent chain — the next epoch's preflight will retry.
#[expect(
    clippy::too_many_arguments,
    reason = "follow loop passes individual fields"
)]
async fn execute_rollback<C: ChainTypes>(
    db: &Database,
    handlers: &HandlerRegistry,
    transfer_handlers: &TransferRegistry,
    call_handlers: &CallRegistry,
    config: &crate::config::IndexConfig,
    anchors: &[crate::p2p::NetworkPeer<C>],
    expected_tip: alloy_primitives::B256,
    baseline: u64,
) -> eyre::Result<()> {
    warn!(block = baseline, "reorg detected, finding common ancestor");
    let Some(ancestor) = reorg::find_common_ancestor(db, anchors, baseline, expected_tip).await?
    else {
        warn!("no anchor peer provided a valid divergent chain; retrying next epoch");
        return Ok(());
    };

    rollback_to_ancestor(
        db,
        handlers,
        transfer_handlers,
        call_handlers,
        config,
        ancestor,
    )
    .await
}

/// Roll back all indexed data above `ancestor` in one DB transaction.
///
/// Shared by the follow-loop reorg path and the startup reorg-recovery
/// path — `db::rollback_to` moves the checkpoint AND the verified frontier
/// atomically, so both callers keep the marker consistent.
pub async fn rollback_to_ancestor(
    db: &Database,
    handlers: &HandlerRegistry,
    transfer_handlers: &TransferRegistry,
    call_handlers: &CallRegistry,
    config: &crate::config::IndexConfig,
    ancestor: u64,
) -> eyre::Result<()> {
    info!(ancestor, "rolling back to common ancestor");
    let ancestor_block = BlockNumber::new(ancestor);
    let mut tx = db.begin().await?;
    db::verification::ensure_rollback_above_archive(&mut tx, ancestor_block).await?;
    handlers.rollback_all(ancestor_block, &mut tx).await?;
    transfer_handlers
        .rollback_all(ancestor_block, &mut tx)
        .await?;
    call_handlers.rollback_all(ancestor_block, &mut tx).await?;
    db::rollback_factory_children(&mut tx, ancestor_block, config).await?;
    db::rollback_to(&mut tx, ancestor_block).await?;
    tx.commit().await?;
    Ok(())
}

/// Background task that continuously probes P2P peers for the chain head.
///
/// Broadcasts the highest confirmed head via `head_seen_tx` so the fetch
/// loop can use it as head_cap instead of stale per-peer heads.
async fn run_head_tracker<C: ChainTypes>(
    pool: Arc<PeerPool<C>>,
    head_seen_tx: watch::Sender<u64>,
    mut stop_rx: watch::Receiver<bool>,
    initial_baseline: u64,
) {
    let mut last_head = initial_baseline;
    loop {
        tokio::select! {
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() { break; }
            }
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
        if *stop_rx.borrow() {
            break;
        }
        match discover_head_p2p(&pool, last_head, 3, 64).await {
            Ok(Some(head)) if head > last_head => {
                last_head = head;
                let _ = head_seen_tx.send(head);
            }
            _ => {}
        }
    }
}

/// Sleep for `duration`, but return early if `stop_rx` signals shutdown.
/// Returns `true` if shutdown was signaled.
async fn wait_or_stop(stop_rx: &watch::Receiver<bool>, duration: Duration) -> bool {
    let mut rx = stop_rx.clone();
    tokio::select! {
        () = tokio::time::sleep(duration) => false,
        _ = rx.changed() => *rx.borrow(),
    }
}
