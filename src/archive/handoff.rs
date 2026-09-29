//! Availability checks supplement, but never replace, canonical peer quorum.
use crate::{
    chain::BaseChain,
    p2p::{fetch_payloads_for_headers, PeerPool},
    sync::canonical,
};
use alloy_primitives::B256;
use eyre::{Result, WrapErr};
use std::time::Duration;
use tokio::sync::watch;

/// The pinned archive endpoint selects the bridge; its identity is persisted in
/// the archive job. Peer hints cannot move it or substitute a different anchor.
#[derive(Clone, Copy, Debug)]
pub struct Bridge {
    pub end: u64,
    pub hash: B256,
    pub retry_secs: u64,
}

#[derive(Debug)]
struct ArchiveConflict(u64);
impl std::fmt::Display for ArchiveConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "peer chain conflicts with trusted archive boundary at {}; explicit recovery required",
            self.0
        )
    }
}
impl std::error::Error for ArchiveConflict {}

impl Bridge {
    pub async fn verify_tail(
        &self,
        ctx: &crate::sync::SyncContext<BaseChain>,
        reader: &dyn crate::sync::validation::ArchiveRecoveryReader,
        policy: &canonical::QuorumPolicy,
    ) -> Result<bool> {
        if let canonical::FrontierStatus::Committed { checkpoint, .. } =
            canonical::committed_frontier_status(&ctx.db).await?
        {
            loop {
                let vote = tokio::select! {
                    () = cancelled(ctx.stop_rx.clone()) => return Ok(false),
                    result = canonical::quorum_at(&ctx.pool, checkpoint, policy) => result,
                };
                if vote.is_ok() {
                    break;
                }
                tracing::warn!(
                    checkpoint,
                    "waiting for Base peers to verify the saved P2P frontier"
                );
                if wait_retry(&ctx.stop_rx, Duration::from_secs(self.retry_secs)).await {
                    return Ok(false);
                }
            }
        }
        crate::sync::engine::verify_or_recover_frontier_with_archive(ctx, policy, Some(reader))
            .await?;
        // A tail reorg may return to the immutable archive boundary.
        if ctx
            .db
            .last_checkpoint()
            .await?
            .is_some_and(|n| n.as_u64() == self.end)
        {
            return self.wait(&ctx.pool, policy, &ctx.stop_rx).await;
        }
        Ok(!*ctx.stop_rx.borrow())
    }
    /// Archives have already committed before this wait begins. Temporary peer
    /// shortages cannot cancel that work or require a privately operated node.
    pub async fn wait(
        &self,
        pool: &PeerPool<BaseChain>,
        policy: &canonical::QuorumPolicy,
        stop: &watch::Receiver<bool>,
    ) -> Result<bool> {
        loop {
            if *stop.borrow() {
                return Ok(false);
            }
            match self.probe(pool, policy, stop).await {
                Ok(()) => return Ok(true),
                Err(error) if error.downcast_ref::<ArchiveConflict>().is_some() => {
                    return Err(error)
                }
                Err(error) => {
                    tracing::warn!(next = self.end + 1, error = %error, "archive complete; waiting for Base peer handoff");
                }
            }
            if wait_retry(stop, Duration::from_secs(self.retry_secs)).await {
                return Ok(false);
            }
        }
    }
    pub async fn probe(
        &self,
        pool: &PeerPool<BaseChain>,
        policy: &canonical::QuorumPolicy,
        stop: &watch::Receiver<bool>,
    ) -> Result<()> {
        let result = tokio::select! {
            () = cancelled(stop.clone()) => eyre::bail!("archive handoff cancelled"),
            result = tokio::time::timeout(Duration::from_secs(300), self.probe_inner(pool, policy)) => {
                result.wrap_err("peer bridge probe timed out")?
            }
        };
        result.wrap_err_with(|| {
            format!(
                "waiting for ordinary Base peers to serve block {}; archive progress is preserved",
                self.end + 1,
            )
        })
    }

    async fn probe_inner(
        &self,
        pool: &PeerPool<BaseChain>,
        policy: &canonical::QuorumPolicy,
    ) -> Result<()> {
        // Only the first unindexed block is requested from peers. Its quorum-
        // authenticated parent binds it directly to the pinned archive endpoint.
        let next = self.end + 1;
        let chain =
            canonical::establish_canonical_chain(pool, next, next, None, false, policy).await?;
        let header = chain
            .get(next)
            .ok_or_else(|| eyre::eyre!("missing bridge header"))?;
        if header.header().parent_hash != self.hash {
            return Err(eyre::Report::new(ArchiveConflict(self.end)));
        }
        let mut peers = pool.snapshot();
        // Hints only prioritize requests. Actual validated bodies AND receipts
        // must be returned, even if the indexer's bloom would skip this block.
        peers.sort_by_key(|p| p.earliest_block.is_some_and(|n| n > next));
        for peer in peers.into_iter().take(16) {
            let headers = vec![header.clone()];
            if let Ok(Ok(response)) = tokio::time::timeout(
                Duration::from_secs(20),
                fetch_payloads_for_headers(&peer, headers),
            )
            .await
            {
                if response.missing_blocks.is_empty() && response.payloads.len() == 1 {
                    tracing::info!(
                        block = next,
                        "archive peer bridge verified: headers, bodies, receipts"
                    );
                    return Ok(());
                }
            }
        }
        eyre::bail!("peers have not served the validated body and receipts for block {next}")
    }
}

async fn cancelled(mut stop: watch::Receiver<bool>) {
    while !*stop.borrow() {
        if stop.changed().await.is_err() {
            break;
        }
    }
}

async fn wait_retry(stop: &watch::Receiver<bool>, delay: Duration) -> bool {
    tokio::select! {
        () = cancelled(stop.clone()) => true,
        () = tokio::time::sleep(delay) => false,
    }
}

/// Catch up using served ascending headers, never an advertised retention range
/// or an unbounded allocation derived from a peer's claimed tip.
pub async fn catch_up(mut ctx: crate::sync::SyncContext<BaseChain>) -> Result<()> {
    ctx.is_backfill = true;
    loop {
        if *ctx.stop_rx.borrow() {
            return Ok(());
        }
        let baseline = ctx
            .db
            .last_checkpoint()
            .await?
            .ok_or_else(|| eyre::eyre!("archive handoff has no checkpoint"))?
            .as_u64();
        let observed = tokio::select! {
            () = cancelled(ctx.stop_rx.clone()) => return Ok(()),
            result = crate::p2p::discover_head_p2p(&ctx.pool, baseline, 8, 1024) => result?,
        };
        let Some(head) = observed else {
            tracing::warn!(
                next = baseline + 1,
                "waiting for Base peers to resume recent-history backfill"
            );
            if wait_retry(&ctx.stop_rx, Duration::from_secs(15)).await {
                return Ok(());
            }
            continue;
        };
        if head <= baseline {
            // One untrusted Status hint must not hold live notifications in
            // backfill mode forever after we reach the served canonical tip.
            let later = ctx
                .pool
                .snapshot()
                .iter()
                .filter(|p| p.head_number > baseline + 2)
                .count();
            if later >= canonical::QuorumPolicy::default().required_matches {
                tracing::warn!(
                    next = baseline + 1,
                    "waiting for Base peers to serve the next recent-history block"
                );
                if wait_retry(&ctx.stop_rx, Duration::from_secs(15)).await {
                    return Ok(());
                }
                continue;
            }
            return Ok(());
        }
        crate::sync::run_canonical_segments(
            crate::types::BlockNumber::new(baseline + 1),
            crate::types::BlockNumber::new(head),
            ctx.clone(),
        )
        .await
        .wrap_err_with(|| {
            format!(
                "recent-history backfill failed for {}..={head}; committed progress is preserved",
                baseline + 1
            )
        })?;
    }
}
