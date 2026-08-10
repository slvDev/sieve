//! Reorg detection and rollback.
//!
//! Before each follow-mode epoch, compare the stored block hash for the
//! last indexed block against what peers report. If hashes diverge, walk
//! backward to find the common ancestor, then roll back indexed data.

use crate::db::Database;
use crate::p2p::{request_headers_batch, NetworkPeer, PeerPool};
use crate::types::BlockNumber;

use eyre::Result;
use reth_primitives_traits::SealedHeader;
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Maximum reorg depth we handle. If the fork is deeper than this, bail.
const MAX_REORG_DEPTH: u64 = 64;

/// Maximum number of peers to probe during reorg preflight.
const MAX_PROBE_PEERS: usize = 5;

/// Number of agreeing peers required for a reorg decision.
///
/// Degrades to the pool size when fewer peers are connected, so a
/// single-peer setup can still make progress.
const QUORUM: usize = 2;

/// Outcome of the reorg preflight check.
#[derive(Debug)]
#[non_exhaustive]
pub enum ReorgCheck {
    /// Stored hash matches the network — no reorg.
    NoReorg,
    /// A quorum of peers agreed on a different hash for the last indexed
    /// block. `anchors` are the peers that voted for `expected_tip`; the
    /// ancestor walk must validate against that exact hash.
    ReorgDetected {
        anchors: Vec<NetworkPeer>,
        expected_tip: alloy_primitives::B256,
    },
    /// No peers could be reached — try again next epoch.
    Inconclusive,
}

/// Check whether the last indexed block is still on the canonical chain.
///
/// Probes up to [`MAX_PROBE_PEERS`] peers, fetching the header at
/// `last_indexed` and comparing its hash to the stored value. A decision
/// (no-reorg or reorg) requires [`QUORUM`] agreeing peers, so a single
/// malicious peer cannot trigger a rollback or mask a real reorg.
///
/// # Errors
///
/// Returns an error if the DB read fails.
pub async fn preflight_reorg(
    db: &Database,
    pool: &PeerPool,
    last_indexed: u64,
) -> Result<ReorgCheck> {
    let Some(stored_hash) = db.get_block_hash(BlockNumber::new(last_indexed)).await? else {
        debug!(
            block = last_indexed,
            "no stored hash — skipping reorg check"
        );
        return Ok(ReorgCheck::NoReorg);
    };

    let peers = pool.snapshot();
    if peers.is_empty() {
        return Ok(ReorgCheck::Inconclusive);
    }

    let result = probe_peers_for_hash(&peers, last_indexed, stored_hash).await;
    Ok(result)
}

/// Result of probing a single peer for a block hash.
enum ProbeResult {
    /// Peer returned a header — its sealed hash.
    Hash(alloy_primitives::B256),
    /// Peer responded but header was empty — counts as probed.
    Empty,
    /// Request failed — doesn't count toward the probe limit.
    Failed,
}

/// Decision reached once enough peers agree.
#[derive(Debug, PartialEq, Eq)]
enum TallyDecision {
    /// Quorum confirmed the stored hash — no reorg.
    NoReorg,
    /// Quorum agreed on a different hash for the block.
    Reorg(alloy_primitives::B256),
}

/// Accumulates per-peer hash votes until a quorum is reached.
struct VoteTally {
    quorum: usize,
    matches: usize,
    divergent: HashMap<alloy_primitives::B256, usize>,
}

impl VoteTally {
    fn new(quorum: usize) -> Self {
        Self {
            quorum: quorum.max(1),
            matches: 0,
            divergent: HashMap::new(),
        }
    }

    /// Record one peer's hash vote; returns a decision once a quorum of
    /// peers agrees on the same answer.
    fn record(
        &mut self,
        network_hash: alloy_primitives::B256,
        stored_hash: alloy_primitives::B256,
    ) -> Option<TallyDecision> {
        if network_hash == stored_hash {
            self.matches += 1;
            if self.matches >= self.quorum {
                return Some(TallyDecision::NoReorg);
            }
        } else {
            let count = self.divergent.entry(network_hash).or_insert(0);
            *count += 1;
            if *count >= self.quorum {
                return Some(TallyDecision::Reorg(network_hash));
            }
        }
        None
    }
}

/// Probe up to [`MAX_PROBE_PEERS`] peers for the header hash at
/// `block_number` and tally votes until a quorum agrees.
async fn probe_peers_for_hash(
    peers: &[NetworkPeer],
    block_number: u64,
    stored_hash: alloy_primitives::B256,
) -> ReorgCheck {
    let mut tally = VoteTally::new(QUORUM.min(peers.len()));
    let mut divergent_anchors: HashMap<alloy_primitives::B256, Vec<NetworkPeer>> = HashMap::new();
    let mut probed = 0usize;

    for peer in peers {
        if probed >= MAX_PROBE_PEERS {
            break;
        }

        match probe_single_peer(peer, block_number).await {
            ProbeResult::Hash(network_hash) => {
                probed += 1;
                if network_hash == stored_hash {
                    debug!(
                        block = block_number,
                        peer_id = ?peer.peer_id,
                        "reorg preflight: hash matches"
                    );
                } else {
                    warn!(
                        block = block_number,
                        peer_id = ?peer.peer_id,
                        stored = %stored_hash,
                        network = %network_hash,
                        "reorg preflight: hash mismatch reported"
                    );
                    divergent_anchors
                        .entry(network_hash)
                        .or_default()
                        .push(peer.clone());
                }
                match tally.record(network_hash, stored_hash) {
                    Some(TallyDecision::NoReorg) => return ReorgCheck::NoReorg,
                    Some(TallyDecision::Reorg(hash)) => {
                        if let Some(anchors) = divergent_anchors.remove(&hash) {
                            return ReorgCheck::ReorgDetected {
                                anchors,
                                expected_tip: hash,
                            };
                        }
                        return ReorgCheck::Inconclusive;
                    }
                    None => {}
                }
            }
            ProbeResult::Empty => probed += 1,
            ProbeResult::Failed => {}
        }
    }

    ReorgCheck::Inconclusive
}

/// Probe a single peer for the header hash at `block_number`.
async fn probe_single_peer(peer: &NetworkPeer, block_number: u64) -> ProbeResult {
    let headers = match request_headers_batch(peer, block_number, 1).await {
        Ok(h) => h,
        Err(e) => {
            debug!(peer_id = ?peer.peer_id, error = %e, "reorg probe failed");
            return ProbeResult::Failed;
        }
    };

    let Some(header) = headers.into_iter().next() else {
        return ProbeResult::Empty;
    };

    ProbeResult::Hash(SealedHeader::seal_slow(header).hash())
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
pub async fn find_common_ancestor(
    db: &Database,
    anchors: &[NetworkPeer],
    last_indexed: u64,
    expected_tip: alloy_primitives::B256,
) -> Result<Option<u64>> {
    let low = last_indexed.saturating_sub(MAX_REORG_DEPTH);
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
    use super::{validate_anchor_chain, TallyDecision, VoteTally};
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

    #[test]
    fn tally_confirms_no_reorg_at_quorum() {
        let stored = B256::repeat_byte(0x01);
        let mut tally = VoteTally::new(2);
        assert_eq!(tally.record(stored, stored), None);
        assert_eq!(tally.record(stored, stored), Some(TallyDecision::NoReorg));
    }

    #[test]
    fn tally_detects_reorg_at_quorum() {
        let stored = B256::repeat_byte(0x01);
        let fork = B256::repeat_byte(0x02);
        let mut tally = VoteTally::new(2);
        assert_eq!(tally.record(fork, stored), None);
        assert_eq!(tally.record(fork, stored), Some(TallyDecision::Reorg(fork)));
    }

    #[test]
    fn tally_disagreeing_peers_reach_no_decision() {
        let stored = B256::repeat_byte(0x01);
        let fork_a = B256::repeat_byte(0x02);
        let fork_b = B256::repeat_byte(0x03);
        let mut tally = VoteTally::new(2);
        // A lone divergent peer, a second peer on a different fork, and a
        // single match: nothing reaches quorum.
        assert_eq!(tally.record(fork_a, stored), None);
        assert_eq!(tally.record(fork_b, stored), None);
        assert_eq!(tally.record(stored, stored), None);
    }

    #[test]
    fn tally_quorum_one_decides_immediately() {
        let stored = B256::repeat_byte(0x01);
        let fork = B256::repeat_byte(0x02);
        let mut tally = VoteTally::new(1);
        assert_eq!(tally.record(fork, stored), Some(TallyDecision::Reorg(fork)));
    }
}
