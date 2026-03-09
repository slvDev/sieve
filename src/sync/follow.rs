//! Head-following mode.
//!
//! After historical sync completes, follows new blocks in real-time.
//! Each "epoch" discovers the current chain head via P2P, runs reorg
//! preflight, then syncs the gap. Sleeps near the tip to avoid busy-looping.

use crate::db::{self, Database};
use crate::handler::{CallRegistry, HandlerRegistry, TransferRegistry};
use crate::metrics::SieveMetrics;
use crate::p2p::{discover_head_p2p, PeerPool};
use crate::stream::StreamDispatcher;
use crate::sync::reorg;
use crate::sync::{run_sync, ReorgCheck, SyncContext};
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
struct FollowContext {
    pool: Arc<PeerPool>,
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
}

/// Run the follow loop: discover head, preflight reorg, sync gap, repeat.
///
/// Exits when `stop_rx` signals shutdown.
///
/// # Errors
///
/// Returns an error on unrecoverable failures (DB errors, deep reorgs).
#[instrument(skip_all, fields(start_block = start_block.as_u64()))]
pub async fn run_follow_loop(start_block: BlockNumber, ctx: SyncContext) -> eyre::Result<()> {
    info!(start_block = start_block.as_u64(), "entering follow mode");

    let fctx = FollowContext {
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
    };

    let mut last_heartbeat = Instant::now();

    while !is_stopped(&fctx.stop_rx) {
        let evicted = fctx.pool.evict_stale(Duration::from_secs(120));
        if evicted > 0 {
            info!(evicted, peers = fctx.pool.len(), "evicted stale peers");
        }
        if last_heartbeat.elapsed() >= Duration::from_secs(30) {
            info!(
                peers = fctx.pool.len(),
                "follow mode: waiting for new blocks"
            );
            last_heartbeat = Instant::now();
        }
        if run_follow_epoch(&fctx).await? {
            break;
        }
    }

    info!("follow loop exited");
    Ok(())
}

/// Run a single follow epoch. Returns `true` if the loop should exit.
async fn run_follow_epoch(ctx: &FollowContext) -> eyre::Result<bool> {
    match discover_gap(ctx).await? {
        EpochAction::Wait => Ok(wait_or_stop(&ctx.stop_rx, Duration::from_secs(1)).await),
        EpochAction::Reorg => Ok(false),
        EpochAction::Sync { next_block, head } => {
            let gap = sync_epoch(ctx, next_block, head).await?;
            if gap <= 2 {
                ctx.metrics.is_ready.store(true, Ordering::Relaxed);
            }
            let should_exit =
                gap <= 2 && wait_or_stop(&ctx.stop_rx, Duration::from_millis(500)).await;
            Ok(should_exit)
        }
    }
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

/// Discover the current chain head and determine the epoch action.
async fn discover_gap(ctx: &FollowContext) -> eyre::Result<EpochAction> {
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
async fn sync_epoch(ctx: &FollowContext, next_block: u64, head: u64) -> eyre::Result<u64> {
    let gap = head.saturating_sub(next_block) + 1;
    info!(next_block, head, gap, "follow epoch: syncing gap");

    let sync_ctx = SyncContext {
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
    };

    let outcome = run_sync(
        BlockNumber::new(next_block),
        BlockNumber::new(head),
        sync_ctx,
    )
    .await?;

    info!(
        blocks = outcome.blocks_fetched,
        events_stored = outcome.events_stored,
        elapsed_ms = outcome.elapsed.as_millis() as u64,
        "follow epoch complete"
    );

    Ok(gap)
}

/// Run reorg preflight and rollback if needed.
/// Returns `true` if a reorg was handled (caller should `continue` the loop)
/// or if we should wait and retry. Returns `false` to proceed with sync.
#[expect(
    clippy::too_many_arguments,
    reason = "follow loop passes individual fields"
)]
async fn should_rollback_reorg(
    db: &Database,
    pool: &PeerPool,
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

    match reorg::preflight_reorg(db, pool, baseline).await? {
        ReorgCheck::NoReorg => Ok(false),
        ReorgCheck::Inconclusive => {
            debug!("reorg check inconclusive, retrying");
            wait_or_stop(stop_rx, Duration::from_secs(1)).await;
            Ok(true)
        }
        ReorgCheck::ReorgDetected { anchor } => {
            execute_rollback(
                db,
                handlers,
                transfer_handlers,
                call_handlers,
                config,
                &anchor,
                baseline,
            )
            .await?;
            Ok(true)
        }
    }
}

/// Find the common ancestor and roll back all indexed data above it.
async fn execute_rollback(
    db: &Database,
    handlers: &HandlerRegistry,
    transfer_handlers: &TransferRegistry,
    call_handlers: &CallRegistry,
    config: &crate::config::IndexConfig,
    anchor: &crate::p2p::NetworkPeer,
    baseline: u64,
) -> eyre::Result<()> {
    warn!(block = baseline, "reorg detected, finding common ancestor");
    let ancestor = reorg::find_common_ancestor(db, anchor, baseline).await?;

    info!(ancestor, "rolling back to common ancestor");
    let ancestor_block = BlockNumber::new(ancestor);
    let mut tx = db.begin().await?;
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

/// Sleep for `duration`, but return early if `stop_rx` signals shutdown.
/// Returns `true` if shutdown was signaled.
async fn wait_or_stop(stop_rx: &watch::Receiver<bool>, duration: Duration) -> bool {
    let mut rx = stop_rx.clone();
    tokio::select! {
        () = tokio::time::sleep(duration) => false,
        _ = rx.changed() => *rx.borrow(),
    }
}
