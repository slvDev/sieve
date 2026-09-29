//! Reorg detection and rollback.
//!
//! Before each follow-mode epoch, compare the stored block hash for the
//! last indexed block against what peers report. If hashes diverge, walk
//! backward to find the common ancestor, then roll back indexed data.

use crate::chain::ChainTypes;
use crate::db::Database;
use crate::p2p::{request_headers_batch, NetworkPeer, PeerPool};
use crate::sync::canonical::{quorum_at, QuorumPolicy};
use crate::types::BlockNumber;

use eyre::Result;
use reth_primitives_traits::SealedHeader;
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Maximum reorg depth we handle. If the fork is deeper than this, bail.
const MAX_REORG_DEPTH: u64 = 64;

/// Outcome of the reorg preflight check.
#[derive(Debug)]
#[non_exhaustive]
pub enum ReorgCheck<C: ChainTypes> {
    /// Stored hash matches the network — no reorg.
    NoReorg,
    /// A quorum of peers agreed on a different hash for the last indexed
    /// block. `anchors` are the peers that voted for `expected_tip`; the
    /// ancestor walk must validate against that exact hash.
    ReorgDetected {
        anchors: Vec<NetworkPeer<C>>,
        expected_tip: alloy_primitives::B256,
    },
    /// No peers could be reached — try again next epoch.
    Inconclusive,
}

/// Check whether the last indexed block is still on the canonical chain.
///
/// The stored hash for `last_indexed` is compared against the hash a
/// canonical peer QUORUM reports for that height — the SAME fixed-threshold
/// policy that authorizes header commits. A rollback is therefore only ever
/// authorized by the same multi-peer agreement as a write:
///
/// - quorum confirms the stored hash → no reorg;
/// - quorum agrees on a DIFFERENT hash → reorg to it (the voters become the
///   anchors whose divergent chain the ancestor walk validates);
/// - no quorum forms (sparse or split) → inconclusive, and NOTHING is
///   rolled back. One hostile peer can no longer trigger destructive
///   rollback of valid committed data.
///
/// # Errors
///
/// Returns an error if the DB read fails.
pub async fn preflight_reorg<C: ChainTypes>(
    db: &Database,
    pool: &PeerPool<C>,
    last_indexed: u64,
    policy: &QuorumPolicy,
) -> Result<ReorgCheck<C>> {
    let Some(stored_hash) = db.get_block_hash(BlockNumber::new(last_indexed)).await? else {
        debug!(
            block = last_indexed,
            "no stored hash — skipping reorg check"
        );
        return Ok(ReorgCheck::NoReorg);
    };

    // No degradation: a rollback needs the full canonical quorum. If it
    // cannot form, treat it as inconclusive and leave committed data
    // untouched rather than trusting too few peers.
    let winner = match quorum_at(pool, last_indexed, policy).await {
        Ok(winner) => winner,
        Err(err) => {
            debug!(
                block = last_indexed,
                error = %err,
                "reorg preflight: no canonical quorum; leaving data untouched"
            );
            return Ok(ReorgCheck::Inconclusive);
        }
    };

    if winner.hash == stored_hash {
        return Ok(ReorgCheck::NoReorg);
    }

    warn!(
        block = last_indexed,
        stored = %stored_hash,
        network = %winner.hash,
        voters = winner.voters.len(),
        "reorg preflight: canonical quorum diverges from stored hash"
    );
    Ok(ReorgCheck::ReorgDetected {
        anchors: winner.voters,
        expected_tip: winner.hash,
    })
}

/// Walk backward from `last_indexed` to find the highest block where stored
/// and network hashes agree. Returns the common ancestor block number, or
/// `None` if no anchor peer supplied a valid divergent chain (caller should
/// retry next epoch instead of rolling back).
///
/// Each anchor's header response is only trusted if it forms one continuous
/// parent-linked chain whose tip hash equals `expected_tip` — the hash the
/// preflight quorum agreed on. A peer replaying unrelated headers therefore
/// cannot influence the rollback depth.
///
/// # Errors
///
/// Returns an error if the reorg exceeds `MAX_REORG_DEPTH` or if the DB
/// read fails.
pub async fn find_common_ancestor<C: ChainTypes>(
    db: &Database,
    anchors: &[NetworkPeer<C>],
    last_indexed: u64,
    expected_tip: alloy_primitives::B256,
) -> Result<Option<u64>> {
    let archive_boundary = crate::db::verification::archive_frontier(db)
        .await?
        .map(|f| f.block);
    let low = last_indexed
        .saturating_sub(MAX_REORG_DEPTH)
        .max(archive_boundary.unwrap_or(0));
    eyre::ensure!(
        low <= last_indexed,
        "checkpoint precedes trusted archive boundary"
    );
    let count = (last_indexed - low + 1) as usize;

    for anchor in anchors {
        let headers = match request_headers_batch(anchor, low, count).await {
            Ok(h) => h,
            Err(e) => {
                debug!(peer_id = ?anchor.peer_id, error = %e, "ancestor fetch failed");
                continue;
            }
        };

        let Some(network_hashes) = validate_anchor_chain(headers, low, last_indexed, expected_tip)
        else {
            warn!(
                peer_id = ?anchor.peer_id,
                expected_tip = %expected_tip,
                "anchor peer returned an invalid or unrelated chain; trying next anchor"
            );
            continue;
        };

        let Some(ancestor) = walk_to_ancestor(db, &network_hashes, low, last_indexed).await? else {
            if archive_boundary == Some(low) {
                return Err(eyre::eyre!("canonical chain conflicts with trusted archive boundary at {low}; explicit recovery required"));
            }
            return Err(eyre::eyre!(
                "reorg exceeds max depth ({MAX_REORG_DEPTH} blocks) — cannot find common ancestor"
            ));
        };
        return Ok(Some(ancestor));
    }

    Ok(None)
}

/// Walk backward on a validated network chain, returning the highest block
/// where the stored hash agrees, or `None` if no agreement exists in range.
///
/// # Errors
///
/// Returns an error if the DB read fails.
async fn walk_to_ancestor(
    db: &Database,
    network_hashes: &HashMap<u64, alloy_primitives::B256>,
    low: u64,
    last_indexed: u64,
) -> Result<Option<u64>> {
    for block in (low..=last_indexed).rev() {
        let stored = db.get_block_hash(BlockNumber::new(block)).await?;
        let network = network_hashes.get(&block);

        if let (Some(s), Some(n)) = (stored, network) {
            if s == *n {
                info!(
                    common_ancestor = block,
                    depth = last_indexed - block,
                    "found common ancestor"
                );
                return Ok(Some(block));
            }
        }

        debug!(block, "hash mismatch or missing — continuing backward");
    }
    Ok(None)
}

/// Validate that `headers` form one continuous chain covering exactly
/// `low..=last_indexed` and ending at `expected_tip`.
///
/// Returns the block number → hash map of the validated chain, or `None`
/// if the response is incomplete, out of order, discontinuous, or ends at
/// a different tip hash.
fn validate_anchor_chain(
    headers: Vec<reth_primitives_traits::Header>,
    low: u64,
    last_indexed: u64,
    expected_tip: alloy_primitives::B256,
) -> Option<HashMap<u64, alloy_primitives::B256>> {
    let count = (last_indexed.checked_sub(low)? as usize).checked_add(1)?;
    if headers.len() != count {
        return None;
    }

    let mut hashes: HashMap<u64, alloy_primitives::B256> = HashMap::with_capacity(count);
    let mut prev_hash: Option<alloy_primitives::B256> = None;
    for (idx, header) in headers.into_iter().enumerate() {
        let expected_number = low.checked_add(idx as u64)?;
        if header.number != expected_number {
            return None;
        }
        let parent_hash = header.parent_hash;
        let sealed = SealedHeader::seal_slow(header);
        if let Some(prev) = prev_hash {
            if parent_hash != prev {
                return None;
            }
        }
        prev_hash = Some(sealed.hash());
        hashes.insert(expected_number, sealed.hash());
    }

    if prev_hash? != expected_tip {
        return None;
    }
    Some(hashes)
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::validate_anchor_chain;
    use alloy_primitives::B256;
    use reth_primitives_traits::{Header, SealedHeader};

    /// Build a parent-linked chain of headers starting at `low`.
    /// Returns the headers and the tip hash.
    fn build_chain(low: u64, len: usize) -> (Vec<Header>, B256) {
        let mut headers = Vec::with_capacity(len);
        let mut parent = B256::ZERO;
        for offset in 0..len {
            let header = Header {
                number: low + offset as u64,
                parent_hash: parent,
                ..Default::default()
            };
            parent = SealedHeader::seal_slow(header.clone()).hash();
            headers.push(header);
        }
        (headers, parent)
    }

    #[test]
    fn anchor_chain_valid_is_accepted() {
        let (headers, tip) = build_chain(10, 3);
        let hashes = validate_anchor_chain(headers, 10, 12, tip);
        assert!(hashes.is_some());
        let hashes = hashes.unwrap_or_default();
        assert_eq!(hashes.get(&12), Some(&tip));
        assert_eq!(hashes.len(), 3);
    }

    #[test]
    fn anchor_chain_wrong_tip_is_rejected() {
        let (headers, _tip) = build_chain(10, 3);
        let wrong_tip = B256::repeat_byte(0xEE);
        assert!(validate_anchor_chain(headers, 10, 12, wrong_tip).is_none());
    }

    #[test]
    fn anchor_chain_broken_link_is_rejected() {
        let (mut headers, tip) = build_chain(10, 3);
        headers[1].parent_hash = B256::repeat_byte(0xEE);
        assert!(validate_anchor_chain(headers, 10, 12, tip).is_none());
    }

    #[test]
    fn anchor_chain_incomplete_response_is_rejected() {
        let (mut headers, tip) = build_chain(10, 3);
        headers.pop();
        assert!(validate_anchor_chain(headers, 10, 12, tip).is_none());
    }

    #[test]
    fn anchor_chain_wrong_numbering_is_rejected() {
        let (mut headers, tip) = build_chain(10, 3);
        headers[2].number = 99;
        assert!(validate_anchor_chain(headers, 10, 12, tip).is_none());
    }
}
