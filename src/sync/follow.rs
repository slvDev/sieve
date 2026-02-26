//! Head-following mode.
//!
//! After historical sync completes, follows new blocks in real-time.
//! Each "epoch" discovers the current chain head via P2P, runs reorg
//! preflight, then syncs the gap. Sleeps near the tip to avoid busy-looping.

use crate::config::IndexConfig;
use crate::db::{self, Database};
use crate::handler::HandlerRegistry;
use crate::p2p::{discover_head_p2p, PeerPool};
use crate::sync::engine::run_sync;
use crate::sync::reorg::{self, ReorgCheck};

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// Run the follow loop: discover head, preflight reorg, sync gap, repeat.
///
/// Exits when `stop_rx` signals shutdown.
///
/// # Errors
///
/// Returns an error on unrecoverable failures (DB errors, deep reorgs).
pub async fn run_follow_loop(
    pool: Arc<PeerPool>,
    start_block: u64,
    config: Arc<IndexConfig>,
    db: Arc<Database>,
    handlers: Arc<HandlerRegistry>,
    stop_rx: watch::Receiver<bool>,
) -> eyre::Result<()> {
    info!(start_block, "entering follow mode");

    loop {
        if *stop_rx.borrow() {
            info!("follow loop: shutdown signal received");
            break;
        }

        let baseline = db
            .last_checkpoint()
            .await?
            .unwrap_or_else(|| start_block.saturating_sub(1));

        let Some(observed_head) = discover_head_p2p(&pool, baseline, 3, 1024).await? else {
            debug!("no new blocks discovered, waiting");
            if wait_or_stop(&stop_rx, Duration::from_secs(1)).await {
                break;
            }
            continue;
        };

        let next_block = baseline + 1;
        if next_block > observed_head {
            debug!(next_block, observed_head, "at tip, waiting");
            if wait_or_stop(&stop_rx, Duration::from_secs(1)).await {
                break;
            }
            continue;
        }

        if should_rollback_reorg(&db, &pool, &handlers, baseline, start_block, &stop_rx).await? {
            continue;
        }

        let gap = observed_head.saturating_sub(next_block) + 1;
        info!(next_block, observed_head, gap, "follow epoch: syncing gap");

        let outcome = run_sync(
            Arc::clone(&pool),
            next_block,
            observed_head,
            Arc::clone(&config),
            Arc::clone(&db),
            Arc::clone(&handlers),
            stop_rx.clone(),
        )
        .await?;

        info!(
            blocks = outcome.blocks_fetched,
            events_stored = outcome.events_stored,
            elapsed_ms = outcome.elapsed.as_millis() as u64,
            "follow epoch complete"
        );

        // Near tip: back off to avoid busy-looping
        if gap <= 2 && wait_or_stop(&stop_rx, Duration::from_millis(500)).await {
            break;
        }
    }

    info!("follow loop exited");
    Ok(())
}

/// Run reorg preflight and rollback if needed.
/// Returns `true` if a reorg was handled (caller should `continue` the loop)
/// or if we should wait and retry. Returns `false` to proceed with sync.
async fn should_rollback_reorg(
    db: &Database,
    pool: &PeerPool,
    handlers: &HandlerRegistry,
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
            warn!(block = baseline, "reorg detected, finding common ancestor");
            let ancestor = reorg::find_common_ancestor(db, &anchor, baseline).await?;

            info!(ancestor, "rolling back to common ancestor");
            let mut tx = db.begin().await?;
            handlers.rollback_all(ancestor, &mut tx).await?;
            db::rollback_to(&mut tx, ancestor).await?;
            tx.commit().await?;

            Ok(true)
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
