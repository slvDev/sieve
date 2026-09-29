//! Fetch logic and task execution.

use crate::chain::ChainTypes;
use crate::filter::BloomFilter;
use crate::p2p::{fetch_payloads_for_headers, NetworkPeer, PeerPool};
use crate::sync::canonical::CanonicalChain;
use crate::sync::scheduler::{PeerHealthTracker, PeerWorkScheduler};
use crate::sync::validation::ValidatedPayload;
use crate::sync::{FetchItem, FetchMode, SkippedHeader};
use eyre::{eyre, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, OwnedSemaphorePermit};
use tracing::instrument;

// ── FetchIngestOutcome ───────────────────────────────────────────────

#[derive(Debug)]
pub struct FetchIngestOutcome<C: ChainTypes> {
    pub payloads: Vec<ValidatedPayload<C>>,
    pub missing_blocks: Vec<u64>,
    pub bloom_skipped: Vec<SkippedHeader>,
    #[expect(
        dead_code,
        reason = "populated during fetch for future metrics/logging"
    )]
    pub fetch_stats: crate::p2p::FetchStageStats,
}

/// Fetch full block payloads for a consecutive batch of blocks.
///
/// Headers are NEVER taken from the peer: every header comes from the
/// quorum-verified [`CanonicalChain`], the bloom decision is made on
/// canonical headers, and bodies+receipts are fetched BY canonical hash
/// (and verified downstream against canonical roots). A peer can
/// therefore fail to serve a block, but cannot substitute its own chain.
///
/// When a bloom filter is provided, only blocks whose canonical header
/// bloom matches a configured contract address have bodies+receipts
/// fetched; non-matching blocks are returned in `bloom_skipped`.
///
/// # Errors
///
/// Returns an error if the block batch is not consecutive, falls outside
/// the canonical segment, or the fetch fails.
pub async fn fetch_ingest_batch<C: ChainTypes>(
    peer: &NetworkPeer<C>,
    blocks: &[u64],
    bloom_filter: Option<&BloomFilter>,
    canonical: &CanonicalChain,
) -> Result<FetchIngestOutcome<C>> {
    if blocks.is_empty() {
        return Ok(FetchIngestOutcome {
            payloads: Vec::new(),
            missing_blocks: Vec::new(),
            bloom_skipped: Vec::new(),
            fetch_stats: crate::p2p::FetchStageStats::default(),
        });
    }
    ensure_consecutive(blocks)?;

    // Partition canonical headers: bloom matches get payload fetches,
    // the rest are recorded as skipped (hash + parent link preserved).
    let mut need_fetch = Vec::new();
    let mut bloom_skipped = Vec::new();
    for &number in blocks {
        let header = canonical.get(number).ok_or_else(|| {
            eyre!(
                "block {number} is outside the canonical segment {}..={}",
                canonical.start(),
                canonical.end()
            )
        })?;
        let matches = bloom_filter.is_none_or(|bloom| bloom.header_may_match(header.header()));
        if matches {
            need_fetch.push(header.clone());
        } else {
            bloom_skipped.push(SkippedHeader {
                number,
                hash: header.hash(),
                parent_hash: header.header().parent_hash,
            });
        }
    }

    if need_fetch.is_empty() {
        return Ok(FetchIngestOutcome {
            payloads: Vec::new(),
            missing_blocks: Vec::new(),
            bloom_skipped,
            fetch_stats: crate::p2p::FetchStageStats::default(),
        });
    }

    let result = fetch_payloads_for_headers(peer, need_fetch).await?;

    Ok(FetchIngestOutcome {
        payloads: result.payloads,
        missing_blocks: result.missing_blocks,
        bloom_skipped,
        fetch_stats: result.fetch_stats,
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
pub struct FetchTaskContext<C: ChainTypes> {
    pub scheduler: Arc<PeerWorkScheduler>,
    pub peer_health: Arc<PeerHealthTracker>,
    pub pool: Arc<PeerPool<C>>,
    pub payload_tx: mpsc::Sender<FetchItem<C>>,
    pub ready_tx: mpsc::UnboundedSender<NetworkPeer<C>>,
    pub bloom_filter: Option<Arc<BloomFilter>>,
    pub canonical: Arc<CanonicalChain>,
}

/// Parameters for a single fetch task invocation.
pub struct FetchTaskParams<C: ChainTypes> {
    pub peer: NetworkPeer<C>,
    pub blocks: Vec<u64>,
    pub mode: FetchMode,
    pub permit: OwnedSemaphorePermit,
}

// ── Fetch task execution ─────────────────────────────────────────────

/// Execute a single fetch task for a batch of blocks from a peer.
#[instrument(skip_all, fields(peer_id = ?params.peer.peer_id, blocks = params.blocks.len()))]
pub async fn run_fetch_task<C: ChainTypes>(ctx: FetchTaskContext<C>, params: FetchTaskParams<C>) {
    let FetchTaskParams {
        peer,
        blocks,
        mode,
        permit,
    } = params;
    let assigned_blocks = blocks.len();
    let _permit = permit;

    let fetch_started = tokio::time::Instant::now();
    let result =
        fetch_ingest_batch(&peer, &blocks, ctx.bloom_filter.as_deref(), &ctx.canonical).await;
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

async fn handle_fetch_success<C: ChainTypes>(
    ctx: &FetchTaskContext<C>,
    peer: &NetworkPeer<C>,
    blocks: &[u64],
    outcome: FetchIngestOutcome<C>,
    fetch_elapsed: Duration,
    mode: FetchMode,
) {
    let FetchIngestOutcome {
        payloads,
        missing_blocks,
        bloom_skipped,
        fetch_stats: _,
    } = outcome;

    // Mark both fetched AND bloom-skipped blocks as completed
    let mut completed: Vec<u64> = payloads.iter().map(|p| p.header().number).collect();
    completed.extend(bloom_skipped.iter().map(|s| s.number));
    if !completed.is_empty() {
        let _ = ctx.scheduler.mark_completed(&completed).await;
        if let Some(&max_block) = completed.iter().max() {
            ctx.pool.update_peer_head(peer.peer_id, max_block);
        }
        ctx.pool.mark_peer_success(peer.peer_id);
    }

    let fetched_count = payloads.len();
    for payload in payloads {
        if ctx
            .payload_tx
            .send(FetchItem::Payload(Box::new(payload)))
            .await
            .is_err()
        {
            break;
        }
    }
    // Skipped blocks still contribute their hash records downstream.
    for skipped in &bloom_skipped {
        if ctx
            .payload_tx
            .send(FetchItem::Skipped(*skipped))
            .await
            .is_err()
        {
            break;
        }
    }

    tracing::debug!(
        peer_id = ?peer.peer_id,
        blocks_fetched = fetched_count,
        bloom_skipped = bloom_skipped.len(),
        range_start = blocks.first().copied().unwrap_or(0),
        range_end = blocks.last().copied().unwrap_or(0),
        elapsed_ms = fetch_elapsed.as_millis() as u64,
        mode = ?mode,
        "fetch: batch completed"
    );

    if missing_blocks.is_empty() {
        ctx.scheduler.record_peer_success(peer.peer_id).await;
    } else {
        let fetched: Vec<u64> = completed
            .iter()
            .copied()
            .filter(|b| !bloom_skipped.iter().any(|s| s.number == *b))
            .collect();
        handle_missing_blocks(ctx, peer, &fetched, &missing_blocks, mode).await;
    }
}

async fn handle_missing_blocks<C: ChainTypes>(
    ctx: &FetchTaskContext<C>,
    peer: &NetworkPeer<C>,
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

async fn handle_fetch_error<C: ChainTypes>(
    ctx: &FetchTaskContext<C>,
    peer: &NetworkPeer<C>,
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

async fn requeue_blocks<C: ChainTypes>(ctx: &FetchTaskContext<C>, blocks: &[u64], mode: FetchMode) {
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
