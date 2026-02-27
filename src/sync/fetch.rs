//! Fetch logic and task execution.

use crate::p2p::{fetch_payloads_for_peer, NetworkPeer, PayloadFetchOutcome};
use crate::sync::scheduler::{PeerHealthTracker, PeerWorkScheduler};
use crate::sync::{BlockPayload, FetchMode};
use eyre::{eyre, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, OwnedSemaphorePermit};
use tracing::instrument;

// ── FetchIngestOutcome ───────────────────────────────────────────────

#[derive(Debug)]
pub struct FetchIngestOutcome {
    pub payloads: Vec<BlockPayload>,
    pub missing_blocks: Vec<u64>,
    #[expect(dead_code, reason = "populated during fetch for future metrics/logging")]
    pub fetch_stats: crate::p2p::FetchStageStats,
}

/// Fetch full block payloads for a consecutive batch of blocks.
///
/// # Errors
///
/// Returns an error if the block batch is not consecutive or the fetch fails.
pub async fn fetch_ingest_batch(peer: &NetworkPeer, blocks: &[u64]) -> Result<FetchIngestOutcome> {
    if blocks.is_empty() {
        return Ok(FetchIngestOutcome {
            payloads: Vec::new(),
            missing_blocks: Vec::new(),
            fetch_stats: crate::p2p::FetchStageStats::default(),
        });
    }
    ensure_consecutive(blocks)?;
    let start = blocks[0];
    let end = blocks[blocks.len() - 1];
    let PayloadFetchOutcome {
        payloads,
        missing_blocks,
        fetch_stats,
    } = fetch_payloads_for_peer(peer, start..=end).await?;
    Ok(FetchIngestOutcome {
        payloads,
        missing_blocks,
        fetch_stats,
    })
}

fn ensure_consecutive(blocks: &[u64]) -> Result<()> {
    for idx in 1..blocks.len() {
        if blocks[idx] != blocks[idx - 1].saturating_add(1) {
            return Err(eyre!("block batch is not consecutive"));
        }
    }
    Ok(())
}

// ── Fetch task types ─────────────────────────────────────────────────

/// Shared context for fetch tasks (references to pipeline state).
pub struct FetchTaskContext {
    pub scheduler: Arc<PeerWorkScheduler>,
    pub peer_health: Arc<PeerHealthTracker>,
    pub payload_tx: mpsc::Sender<BlockPayload>,
    pub ready_tx: mpsc::UnboundedSender<NetworkPeer>,
}

/// Parameters for a single fetch task invocation.
pub struct FetchTaskParams {
    pub peer: NetworkPeer,
    pub blocks: Vec<u64>,
    pub mode: FetchMode,
    pub permit: OwnedSemaphorePermit,
}

// ── Fetch task execution ─────────────────────────────────────────────

/// Execute a single fetch task for a batch of blocks from a peer.
#[instrument(skip_all, fields(peer_id = ?params.peer.peer_id, blocks = params.blocks.len()))]
pub async fn run_fetch_task(ctx: FetchTaskContext, params: FetchTaskParams) {
    let FetchTaskParams {
        peer,
        blocks,
        mode,
        permit,
    } = params;
    let assigned_blocks = blocks.len();
    let _permit = permit;

    let fetch_started = tokio::time::Instant::now();
    let result = fetch_ingest_batch(&peer, &blocks).await;
    let fetch_elapsed = fetch_started.elapsed();

    match result {
        Ok(outcome) => {
            handle_fetch_success(&ctx, &peer, &blocks, outcome, fetch_elapsed, mode).await;
        }
        Err(err) => {
            handle_fetch_error(&ctx, &peer, &blocks, err, fetch_elapsed, mode).await;
        }
    }

    ctx.peer_health
        .finish_assignment(peer.peer_id, assigned_blocks)
        .await;

    let _ = ctx.ready_tx.send(peer);
}

async fn handle_fetch_success(
    ctx: &FetchTaskContext,
    peer: &NetworkPeer,
    blocks: &[u64],
    outcome: FetchIngestOutcome,
    fetch_elapsed: Duration,
    mode: FetchMode,
) {
    let FetchIngestOutcome {
        payloads,
        missing_blocks,
        fetch_stats: _,
    } = outcome;

    let completed: Vec<u64> = payloads.iter().map(|p| p.header().number).collect();
    if !completed.is_empty() {
        let _ = ctx.scheduler.mark_completed(&completed).await;
    }

    for payload in payloads {
        if ctx.payload_tx.send(payload).await.is_err() {
            break;
        }
    }

    tracing::debug!(
        peer_id = ?peer.peer_id,
        blocks_completed = completed.len(),
        range_start = blocks.first().copied().unwrap_or(0),
        range_end = blocks.last().copied().unwrap_or(0),
        elapsed_ms = fetch_elapsed.as_millis() as u64,
        mode = ?mode,
        "fetch: batch completed"
    );

    if missing_blocks.is_empty() {
        ctx.scheduler.record_peer_success(peer.peer_id).await;
    } else {
        handle_missing_blocks(ctx, peer, &completed, &missing_blocks, mode).await;
    }
}

async fn handle_missing_blocks(
    ctx: &FetchTaskContext,
    peer: &NetworkPeer,
    completed: &[u64],
    missing_blocks: &[u64],
    mode: FetchMode,
) {
    ctx.peer_health
        .note_error(
            peer.peer_id,
            format!("missing {} blocks in batch", missing_blocks.len()),
        )
        .await;

    if completed.is_empty() {
        ctx.scheduler.record_peer_failure(peer.peer_id).await;
    } else {
        ctx.scheduler.record_peer_partial(peer.peer_id).await;
    }

    for &block in missing_blocks {
        ctx.scheduler
            .record_block_peer_failure(block, peer.peer_id)
            .await;
    }

    requeue_blocks(ctx, missing_blocks, mode).await;

    let missing_sample: Vec<u64> = missing_blocks.iter().copied().take(10).collect();
    tracing::debug!(
        peer_id = ?peer.peer_id,
        missing = missing_blocks.len(),
        missing_blocks = ?missing_sample,
        completed = completed.len(),
        mode = ?mode,
        "fetch: batch partial - missing headers or payloads"
    );
}

async fn handle_fetch_error(
    ctx: &FetchTaskContext,
    peer: &NetworkPeer,
    blocks: &[u64],
    err: eyre::Error,
    fetch_elapsed: Duration,
    mode: FetchMode,
) {
    ctx.peer_health
        .note_error(peer.peer_id, format!("ingest error: {err}"))
        .await;
    ctx.scheduler.record_peer_failure(peer.peer_id).await;

    for &block in blocks {
        ctx.scheduler
            .record_block_peer_failure(block, peer.peer_id)
            .await;
    }

    requeue_blocks(ctx, blocks, mode).await;

    let failed_sample: Vec<u64> = blocks.iter().copied().take(10).collect();
    tracing::debug!(
        peer_id = ?peer.peer_id,
        error = %err,
        blocks = blocks.len(),
        failed_blocks = ?failed_sample,
        elapsed_ms = fetch_elapsed.as_millis() as u64,
        mode = ?mode,
        "fetch: batch error"
    );
}

async fn requeue_blocks(ctx: &FetchTaskContext, blocks: &[u64], mode: FetchMode) {
    match mode {
        FetchMode::Normal => {
            let _ = ctx.scheduler.requeue_failed(blocks).await;
        }
        FetchMode::Escalation => {
            for block in blocks {
                ctx.scheduler.requeue_escalation_block(*block).await;
            }
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::ensure_consecutive;

    #[test]
    fn ensure_consecutive_rejects_gaps() {
        assert!(ensure_consecutive(&[1, 2, 4]).is_err());
        assert!(ensure_consecutive(&[10, 11, 12]).is_ok());
    }
}
