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

/// Outcome of the reorg preflight check.
#[derive(Debug)]
#[non_exhaustive]
pub enum ReorgCheck {
    /// Stored hash matches the network — no reorg.
    NoReorg,
    /// A peer returned a different hash for the last indexed block.
    ReorgDetected { anchor: NetworkPeer },
    /// No peers could be reached — try again next epoch.
    Inconclusive,
}

/// Check whether the last indexed block is still on the canonical chain.
///
/// Probes up to 3 peers, fetching the header at `last_indexed` and
/// comparing its hash to the stored value.
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
        debug!(block = last_indexed, "no stored hash — skipping reorg check");
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
    /// Got a definitive reorg check answer.
    Definitive(ReorgCheck),
    /// Peer responded but header was empty — counts as probed.
    Empty,
    /// Request failed — doesn't count toward the probe limit.
    Failed,
}

/// Probe up to 3 peers for the header hash at `block_number`.
async fn probe_peers_for_hash(
    peers: &[NetworkPeer],
    block_number: u64,
    stored_hash: alloy_primitives::B256,
) -> ReorgCheck {
    let mut probed = 0usize;

    for peer in peers {
        if probed >= 3 {
            break;
        }

        match probe_single_peer(peer, block_number, stored_hash).await {
            ProbeResult::Definitive(result) => return result,
            ProbeResult::Empty => probed += 1,
            ProbeResult::Failed => {}
        }
    }

    ReorgCheck::Inconclusive
}

/// Probe a single peer for the header hash at `block_number`.
async fn probe_single_peer(
    peer: &NetworkPeer,
    block_number: u64,
    stored_hash: alloy_primitives::B256,
) -> ProbeResult {
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

    let network_hash = SealedHeader::seal_slow(header).hash();

    if network_hash == stored_hash {
        debug!(block = block_number, "reorg preflight: hash matches");
        return ProbeResult::Definitive(ReorgCheck::NoReorg);
    }

    warn!(
        block = block_number,
        stored = %stored_hash,
        network = %network_hash,
        "reorg detected: hash mismatch"
    );
    ProbeResult::Definitive(ReorgCheck::ReorgDetected {
        anchor: peer.clone(),
    })
}

/// Walk backward from `last_indexed` to find the highest block where stored
/// and network hashes agree. Returns the common ancestor block number.
///
/// # Errors
///
/// Returns an error if the reorg exceeds `MAX_REORG_DEPTH` or if P2P/DB
/// queries fail.
pub async fn find_common_ancestor(
    db: &Database,
    anchor: &NetworkPeer,
    last_indexed: u64,
) -> Result<u64> {
    let low = last_indexed.saturating_sub(MAX_REORG_DEPTH);
    let count = (last_indexed - low + 1) as usize;

    let headers = request_headers_batch(anchor, low, count).await?;

    let mut network_hashes: HashMap<u64, alloy_primitives::B256> = HashMap::new();
    for header in headers {
        let num = header.number;
        let hash = SealedHeader::seal_slow(header).hash();
        network_hashes.insert(num, hash);
    }

    // Walk backward from last_indexed to low
    for block in (low..=last_indexed).rev() {
        let stored = db.get_block_hash(BlockNumber::new(block)).await?;
        let network = network_hashes.get(&block);

        if let (Some(s), Some(n)) = (stored, network) {
            if s == *n {
                info!(common_ancestor = block, depth = last_indexed - block, "found common ancestor");
                return Ok(block);
            }
        }

        debug!(block, "hash mismatch or missing — continuing backward");
    }

    Err(eyre::eyre!(
        "reorg exceeds max depth ({MAX_REORG_DEPTH} blocks) — cannot find common ancestor"
    ))
}
