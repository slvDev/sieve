//! Peer-quorum header verification and persisted-frontier classification.
//!
//! A Status handshake proves nothing about DATA: chain id, genesis, and
//! fork-id are copyable, and a hostile peer can serve a fully fabricated,
//! self-consistent, parent-linked header chain (observed live on OP
//! mainnet: forged headers with empty blooms that made every block look
//! skippable). Seal and continuity checks cannot catch this — a forgery
//! is internally consistent by construction.
//!
//! Every decision that trusts the network — establishing headers to
//! commit, verifying committed state at startup, and authorizing a
//! destructive reorg rollback — flows through ONE quorum policy here
//! ([`QuorumPolicy`]) so no path can be tricked while another is hardened.
//!
//! ## Quorum policy (fixed, capture-resistant)
//!
//! A hash is canonical for a height only when at least
//! [`QuorumPolicy::required_matches`] DISTINCT peers return it. The
//! threshold is an ABSOLUTE count, never a fraction of responders: empty
//! responses and timeouts are non-votes, they cannot shrink the
//! denominator and lower the bar. Two malicious peers answering while six
//! honest peers return nothing yields two votes — below the threshold —
//! so no quorum forms. Votes are collected as a COMPLETE round before
//! tallying (response order is irrelevant; a fast malicious responder
//! gains nothing), and the polled panel ROTATES across rounds so a
//! cluster of peers stuck at the front of the pool cannot capture every
//! round.
//!
//! There is no single-peer degradation anywhere. A sparse network that
//! cannot muster the quorum refuses to sync unverified data rather than
//! trusting one peer; operators bridge such networks with `trusted_peers`.
//!
//! ## Canonical segment flow
//!
//! For every bounded segment, BEFORE any payload fetch, bloom decision,
//! commit, or notification:
//! 1. the segment's FINAL header hash is decided by quorum;
//! 2. a complete consecutive header chain is fetched and validated
//!    backward from that tip (length, per-header parent-link, final hash
//!    == quorum tip);
//! 3. the chain's left SEAM is verified against committed state — the
//!    first header's parent hash must equal the stored hash below the
//!    segment. On an existing database the seam is MANDATORY; a missing
//!    or mismatched seam means poisoned/discontinuous state and is
//!    refused.
//!
//! Known limit: a sybil operator controlling `required_matches` of the
//! polled panel can still win the vote — quorum raises the bar from "one
//! bad peer" to "N colluding peers in-panel", it does not replace
//! consensus verification.

use crate::chain::ChainTypes;
use crate::db::Database;
use crate::p2p::{request_headers_batch, request_headers_chunked, NetworkPeer, PeerPool};
use crate::types::BlockNumber;
use alloy_primitives::B256;
use reth_network_peers::PeerId;
use reth_primitives_traits::{Header, SealedHeader};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, warn};

/// Maximum blocks per canonical segment (bounds header memory: ~8k
/// sealed headers ≈ a few MB).
pub const CANONICAL_SEGMENT_BLOCKS: u64 = 8_192;

/// Per-peer vote request timeout.
const QUORUM_VOTE_TIMEOUT: Duration = Duration::from_secs(10);

/// Distinct peers tried for the full header-chain fetch.
const CHAIN_FETCH_ATTEMPTS: usize = 4;

/// Fixed quorum policy plus timing knobs (overridable in tests so failure
/// paths don't wait for production timeouts).
#[derive(Debug, Clone, Copy)]
pub struct QuorumPolicy {
    /// Distinct matching votes required for ANY canonical decision.
    /// Absolute — empty/timeout responses never lower it.
    pub required_matches: usize,
    /// Maximum peers polled per round (the rotating panel size).
    pub panel_max: usize,
    /// How long to wait for at least `required_matches` connected peers.
    pub peer_wait: Duration,
    /// Rounds attempted before giving up (peers join, tips propagate).
    pub rounds: usize,
    /// Delay between rounds.
    pub round_delay: Duration,
}

impl Default for QuorumPolicy {
    fn default() -> Self {
        Self {
            required_matches: 3,
            panel_max: 8,
            peer_wait: Duration::from_secs(180),
            rounds: 8,
            round_delay: Duration::from_secs(3),
        }
    }
}

/// The verified canonical header chain for one sync segment.
#[derive(Debug)]
pub struct CanonicalChain {
    start: u64,
    headers: Vec<SealedHeader>,
}

impl CanonicalChain {
    /// First block number in the segment.
    #[must_use]
    pub const fn start(&self) -> u64 {
        self.start
    }

    /// Last block number in the segment.
    #[must_use]
    pub const fn end(&self) -> u64 {
        self.start
            .saturating_add(self.headers.len() as u64)
            .saturating_sub(1)
    }

    /// The canonical sealed header for `number`, if inside the segment.
    #[must_use]
    pub fn get(&self, number: u64) -> Option<&SealedHeader> {
        number
            .checked_sub(self.start)
            .and_then(|idx| self.headers.get(idx as usize))
    }
}

#[cfg(test)]
pub(super) fn fixture_chain(headers: Vec<Header>) -> eyre::Result<CanonicalChain> {
    let start = headers
        .first()
        .ok_or_else(|| eyre::eyre!("empty fixture"))?
        .number;
    let last = headers.last().ok_or_else(|| eyre::eyre!("empty fixture"))?;
    let end = last.number;
    let tip = SealedHeader::seal_slow(last.clone()).hash();
    Ok(CanonicalChain {
        start,
        headers: validate_canonical_chain(start, end, headers, tip, None, false)?,
    })
}

// ── Pure quorum + validation core (fully unit-tested) ────────────────

/// Outcome of tallying one round of votes against the fixed threshold.
#[derive(Debug, PartialEq, Eq)]
enum QuorumOutcome {
    /// Exactly one hash reached `required_matches` distinct votes.
    Agreed(B256),
    /// Two or more hashes each reached the threshold (conflicting).
    Split,
    /// No hash reached the threshold.
    Insufficient,
}

/// Tally one round of `(peer, hash)` votes against an ABSOLUTE threshold.
///
/// One vote per distinct peer id (later duplicates from the same peer are
/// ignored, so a single peer cannot manufacture a quorum). A hash is
/// canonical only if at least `required` DISTINCT peers voted for it.
/// Non-votes (empty responses, timeouts) never appear here, so they
/// cannot lower the bar — the denominator is the fixed threshold, not the
/// count of responders.
fn tally(votes: &[(PeerId, B256)], required: usize) -> QuorumOutcome {
    let mut per_peer: HashMap<PeerId, B256> = HashMap::new();
    for (peer, hash) in votes {
        per_peer.entry(*peer).or_insert(*hash);
    }

    let mut counts: HashMap<B256, usize> = HashMap::new();
    for hash in per_peer.values() {
        *counts.entry(*hash).or_insert(0) += 1;
    }

    let reached: Vec<B256> = counts
        .into_iter()
        .filter(|(_, count)| *count >= required.max(1))
        .map(|(hash, _)| hash)
        .collect();

    match reached.as_slice() {
        [] => QuorumOutcome::Insufficient,
        [hash] => QuorumOutcome::Agreed(*hash),
        _ => QuorumOutcome::Split,
    }
}

/// Rotate the polled panel across rounds so a static cluster of peers at
/// the front of the pool cannot dominate every round.
fn select_panel<C: ChainTypes>(
    peers: &[NetworkPeer<C>],
    panel_max: usize,
    round: usize,
) -> Vec<NetworkPeer<C>> {
    if peers.is_empty() || peers.len() <= panel_max {
        return peers.to_vec();
    }
    let offset = round
        .saturating_sub(1)
        .saturating_mul(panel_max)
        .checked_rem(peers.len())
        .unwrap_or(0);
    peers
        .iter()
        .cycle()
        .skip(offset)
        .take(panel_max)
        .cloned()
        .collect()
}

/// Validate a fetched header run as THE canonical chain for a segment.
///
/// Requirements, all mandatory:
/// - exactly `end - start + 1` headers, numbered consecutively from
///   `start`;
/// - every header parent-links to its predecessor's sealed hash;
/// - the FINAL sealed hash equals `expected_tip` (the quorum decision) —
///   this transitively pins every header below it, so a forged chain
///   (e.g. empty-bloom headers that make every block look skippable)
///   cannot pass regardless of internal consistency;
/// - when `frontier` is `Some`, the first header's parent hash must equal
///   it — the seam with committed state. When `require_seam` is set and
///   `frontier` is `None`, the segment is refused: an existing database
///   with no stored hash below the segment is poisoned or discontinuous.
fn validate_canonical_chain(
    start: u64,
    end: u64,
    headers: Vec<Header>,
    expected_tip: B256,
    frontier: Option<B256>,
    require_seam: bool,
) -> eyre::Result<Vec<SealedHeader>> {
    let expected_len = end
        .checked_sub(start)
        .and_then(|span| span.checked_add(1))
        .ok_or_else(|| eyre::eyre!("invalid canonical segment bounds {start}..={end}"))?;
    if headers.len() as u64 != expected_len {
        return Err(eyre::eyre!(
            "peer served {} headers for canonical segment {start}..={end} (need {expected_len})",
            headers.len(),
        ));
    }

    let mut sealed: Vec<SealedHeader> = Vec::with_capacity(headers.len());
    for (idx, header) in headers.into_iter().enumerate() {
        let expected_number = start + idx as u64;
        if header.number != expected_number {
            return Err(eyre::eyre!(
                "canonical segment header out of order: expected block {expected_number}, got {}",
                header.number
            ));
        }
        if let Some(prev) = sealed.last() {
            if header.parent_hash != prev.hash() {
                return Err(eyre::eyre!(
                    "canonical segment breaks at block {}: parent hash does not link",
                    header.number
                ));
            }
        }
        sealed.push(SealedHeader::seal_slow(header));
    }

    let tip = sealed
        .last()
        .map(SealedHeader::hash)
        .ok_or_else(|| eyre::eyre!("empty canonical segment"))?;
    if tip != expected_tip {
        return Err(eyre::eyre!(
            "header chain for segment {start}..={end} ends at {tip}, but the peer quorum \
             agreed on {expected_tip}; the serving peer is not on the canonical chain"
        ));
    }

    let first_parent = sealed.first().map(|h| h.parent_hash).unwrap_or_default();
    match frontier {
        Some(frontier_hash) => {
            if first_parent != frontier_hash {
                return Err(eyre::eyre!(
                    "committed chain state does not connect to the canonical chain: stored hash \
                     for block {} is {frontier_hash}, but the canonical block {start} builds on \
                     {first_parent}; the database may contain forged headers (or a reorg deeper \
                     than supported) — use a fresh database or run `sieve reset`",
                    start.saturating_sub(1),
                ));
            }
        }
        None if require_seam => {
            return Err(eyre::eyre!(
                "no committed hash below canonical block {start}, but this database already \
                 holds indexed state; its left seam cannot be verified (poisoned or \
                 discontinuous state) — use a fresh database or run `sieve reset`"
            ));
        }
        None => {}
    }

    Ok(sealed)
}

// ── IO shell ─────────────────────────────────────────────────────────

/// One peer's vote (peer retained so reorg can use the voters as anchors).
struct Vote<C: ChainTypes> {
    peer: NetworkPeer<C>,
    hash: B256,
}

/// The winning hash for a height plus the peers that voted for it.
#[derive(Debug)]
pub struct QuorumWinner<C: ChainTypes> {
    pub hash: B256,
    pub voters: Vec<NetworkPeer<C>>,
}

/// Decide the canonical header hash at `height` by fixed-threshold quorum.
///
/// The single network-trust primitive: used for segment tips, startup
/// frontier verification, and reorg authorization alike.
///
/// # Errors
///
/// Returns an error if too few peers connect, or no hash reaches the
/// required matches within the configured rounds.
pub async fn quorum_at<C: ChainTypes>(
    pool: &PeerPool<C>,
    height: u64,
    policy: &QuorumPolicy,
) -> eyre::Result<QuorumWinner<C>> {
    wait_for_peers(pool, policy).await?;

    for round in 1..=policy.rounds {
        let peers = pool.snapshot();
        let panel = select_panel(&peers, policy.panel_max, round);
        let votes = collect_votes(&panel, height).await;
        let ballots: Vec<(PeerId, B256)> = votes.iter().map(|v| (v.peer.peer_id, v.hash)).collect();
        match tally(&ballots, policy.required_matches) {
            QuorumOutcome::Agreed(hash) => {
                let voters = votes
                    .into_iter()
                    .filter(|v| v.hash == hash)
                    .map(|v| v.peer)
                    .collect();
                return Ok(QuorumWinner { hash, voters });
            }
            outcome => {
                warn!(
                    height,
                    round,
                    votes = votes.len(),
                    required = policy.required_matches,
                    ?outcome,
                    "no quorum on canonical header; retrying"
                );
            }
        }
        tokio::time::sleep(policy.round_delay).await;
    }

    Err(eyre::eyre!(
        "no canonical quorum on the header at block {height} after {} rounds (need {} agreeing \
         peers); refusing to trust unverified data — connect more peers or configure \
         trusted_peers",
        policy.rounds,
        policy.required_matches
    ))
}

/// Establish the canonical header chain for `start..=end`.
///
/// `frontier` is the stored hash of `start - 1` (the seam). `require_seam`
/// forces the seam to exist — set for every segment of an existing
/// database. See the module docs for the protocol.
///
/// # Errors
///
/// Returns an error when no quorum forms, no peer serves a chain reaching
/// the quorum tip, or the seam fails.
pub async fn establish_canonical_chain<C: ChainTypes>(
    pool: &PeerPool<C>,
    start: u64,
    end: u64,
    frontier: Option<B256>,
    require_seam: bool,
    policy: &QuorumPolicy,
) -> eyre::Result<CanonicalChain> {
    let tip = quorum_at(pool, end, policy).await?.hash;
    debug!(start, end, %tip, "canonical tip agreed by quorum");

    let sealed = fetch_validated_chain(pool, start, end, tip, frontier, require_seam).await?;
    Ok(CanonicalChain {
        start,
        headers: sealed,
    })
}

/// Fetch and validate the segment's full header chain from up to
/// [`CHAIN_FETCH_ATTEMPTS`] peers, returning the first that validates
/// against the quorum tip and the seam.
async fn fetch_validated_chain<C: ChainTypes>(
    pool: &PeerPool<C>,
    start: u64,
    end: u64,
    tip: B256,
    frontier: Option<B256>,
    require_seam: bool,
) -> eyre::Result<Vec<SealedHeader>> {
    let count = (end - start + 1) as usize;
    let mut last_err: Option<eyre::Report> = None;
    for peer in pool.snapshot().into_iter().take(CHAIN_FETCH_ATTEMPTS) {
        let headers = match request_headers_chunked(&peer, start, count).await {
            Ok(headers) => headers,
            Err(err) => {
                debug!(peer_id = ?peer.peer_id, error = %err, "canonical chain fetch failed");
                last_err = Some(err);
                continue;
            }
        };
        match validate_canonical_chain(start, end, headers, tip, frontier, require_seam) {
            Ok(sealed) => return Ok(sealed),
            // A seam failure is a statement about OUR database, not about
            // this peer — retrying other peers cannot fix it.
            Err(err) if is_seam_failure(&err) => return Err(err),
            Err(err) => {
                warn!(peer_id = ?peer.peer_id, error = %err, "peer served non-canonical chain");
                last_err = Some(err);
            }
        }
    }

    Err(last_err
        .unwrap_or_else(|| eyre::eyre!("no peers available to serve the canonical header chain"))
        .wrap_err(format!(
            "failed to obtain the canonical header chain for segment {start}..={end}"
        )))
}

/// Whether an error is a seam failure (about our DB, not the peer).
fn is_seam_failure(err: &eyre::Report) -> bool {
    let msg = err.to_string();
    msg.contains("does not connect") || msg.contains("left seam")
}

/// The committed-frontier state a database presents at startup.
#[derive(Debug, PartialEq, Eq)]
pub enum FrontierStatus {
    /// No indexed state — the first canonical segment establishes the
    /// frontier; nothing to verify, the API may start immediately.
    Fresh,
    /// Existing state built through canonical verification, self-consistent
    /// with its marker. The tip still needs a quorum re-check (a reorg may
    /// have happened while stopped).
    Committed { checkpoint: u64, hash: B256 },
    /// Archive-backed state requires replay of retained headers to its trusted anchor.
    Archive(super::validation::ArchiveFrontier),
}

/// Classify a database's committed frontier — the trust boundary.
///
/// The `_sieve_canonical` marker is the boundary: it is written atomically
/// with every commit and moved atomically on rollback, so its PRESENCE
/// (matching the checkpoint) proves the whole committed prefix was built
/// through canonical verification. Consequences, all decided here without
/// touching the network:
///
/// - no indexed state → [`FrontierStatus::Fresh`];
/// - indexed state with NO marker → refused: the database predates
///   canonical verification (or was written by another tool) and its
///   interior cannot be trusted (phase-one policy: reset/reindex, no
///   partial migration);
/// - indexed state whose marker disagrees with the checkpoint or the
///   stored checkpoint hash → refused as torn/poisoned;
/// - peer-quorum marker → [`FrontierStatus::Committed`], to be tip-verified by quorum;
/// - archive marker → [`FrontierStatus::Archive`], to be reverified using retained
///   header evidence, never by silently substituting a peer decision.
///
/// # Errors
///
/// Returns an error on refused state or a query failure.
pub async fn committed_frontier_status(db: &Database) -> eyre::Result<FrontierStatus> {
    let archive = crate::db::verification::archive_frontier(db).await?;
    if !db.has_indexed_state().await? {
        eyre::ensure!(
            archive.is_none(),
            "archive provenance exists without indexed state"
        );
        return Ok(FrontierStatus::Fresh);
    }
    let checkpoint = db.last_checkpoint().await?.map_or(0, BlockNumber::as_u64);

    let Some((marker_block, marker_hash)) = db.verified_frontier().await? else {
        return Err(eyre::eyre!(
            "database is indexed to block {checkpoint} but has no canonical verification marker; \
             it predates canonical verification (or was written by another tool) and its history \
             cannot be trusted — use a fresh database or run `sieve reset`"
        ));
    };

    let stored = db
        .get_block_hash(BlockNumber::new(checkpoint))
        .await?
        .ok_or_else(|| {
            eyre::eyre!(
                "database is indexed to block {checkpoint} but has no stored hash for it; the \
                 committed frontier is torn — use a fresh database or run `sieve reset`"
            )
        })?;

    if marker_block != checkpoint || marker_hash != stored {
        return Err(eyre::eyre!(
            "canonical marker (block {marker_block}) is inconsistent with the checkpoint \
             (block {checkpoint}); the committed state is torn or poisoned — use a fresh \
             database or run `sieve reset`"
        ));
    }

    if let Some(archive) = &archive {
        eyre::ensure!(
            archive.block <= checkpoint
                && db.get_block_hash(BlockNumber::new(archive.block)).await? == Some(archive.hash),
            "archive provenance does not match committed state"
        );
    }
    match crate::db::verification::frontier_source(db)
        .await?
        .as_deref()
    {
        Some("peer_quorum") => Ok(FrontierStatus::Committed {
            checkpoint,
            hash: stored,
        }),
        Some("archive_checkpoint") => {
            let archive = archive
                .ok_or_else(|| eyre::eyre!("archive frontier has no retained evidence metadata"))?;
            eyre::ensure!(
                archive.block == checkpoint && archive.hash == stored,
                "archive provenance does not match frontier"
            );
            Ok(FrontierStatus::Archive(archive))
        }
        _ => eyre::bail!("unsupported frontier verification source"),
    }
}

/// Collect one full round of votes: ask every panel peer for the header
/// at `height` and return each valid (exactly one header, exact height)
/// response. All requests run concurrently and ALL responses are awaited
/// before any tallying happens.
async fn collect_votes<C: ChainTypes>(peers: &[NetworkPeer<C>], height: u64) -> Vec<Vote<C>> {
    let requests = peers.iter().map(|peer| async move {
        let response =
            tokio::time::timeout(QUORUM_VOTE_TIMEOUT, request_headers_batch(peer, height, 1)).await;
        let headers = match response {
            Ok(Ok(headers)) => headers,
            Ok(Err(err)) => {
                debug!(peer_id = ?peer.peer_id, error = %err, "quorum vote request failed");
                return None;
            }
            Err(_) => {
                debug!(peer_id = ?peer.peer_id, "quorum vote request timed out");
                return None;
            }
        };
        if headers.len() != 1 {
            debug!(peer_id = ?peer.peer_id, got = headers.len(), "quorum vote wrong count");
            return None;
        }
        let header = headers.into_iter().next()?;
        if header.number != height {
            debug!(
                peer_id = ?peer.peer_id,
                requested = height,
                got = header.number,
                "quorum vote at wrong height; ignoring"
            );
            return None;
        }
        Some(Vote {
            peer: peer.clone(),
            hash: SealedHeader::seal_slow(header).hash(),
        })
    });

    futures::future::join_all(requests)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// Wait until at least `required_matches` peers are connected (the minimum
/// that could possibly reach the threshold).
async fn wait_for_peers<C: ChainTypes>(
    pool: &PeerPool<C>,
    policy: &QuorumPolicy,
) -> eyre::Result<()> {
    let deadline = tokio::time::Instant::now() + policy.peer_wait;
    loop {
        if pool.len() >= policy.required_matches {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(eyre::eyre!(
                "fewer than {} peers connected — cannot form a canonical header quorum; \
                 refusing to sync unverified data (connect more peers or configure \
                 trusted_peers)",
                policy.required_matches
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[cfg(test)]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions in tests are idiomatic"
)]
mod tests {
    use super::*;

    fn peer(byte: u8) -> PeerId {
        PeerId::repeat_byte(byte)
    }

    fn hash(byte: u8) -> B256 {
        B256::repeat_byte(byte)
    }

    #[test]
    fn tally_requires_absolute_threshold_not_majority_of_responders() {
        // THE finding-1 case: two malicious peers return the same forged
        // hash; six honest peers returned nothing (so they never appear
        // as votes). Two votes is below the fixed threshold of three —
        // empty responses do NOT shrink the denominator, so no quorum.
        let malicious = [(peer(1), hash(0xFF)), (peer(2), hash(0xFF))];
        assert_eq!(tally(&malicious, 3), QuorumOutcome::Insufficient);

        // The same two votes would have been a "majority of valid
        // responses" under the old rule — proving the fix.
        assert_eq!(tally(&malicious, 2), QuorumOutcome::Agreed(hash(0xFF)));
    }

    #[test]
    fn tally_dedups_and_needs_distinct_peers() {
        // A single peer voting three times is still one vote.
        let spam = [
            (peer(1), hash(0xAA)),
            (peer(1), hash(0xAA)),
            (peer(1), hash(0xAA)),
        ];
        assert_eq!(tally(&spam, 3), QuorumOutcome::Insufficient);

        let three = [
            (peer(1), hash(0xAA)),
            (peer(2), hash(0xAA)),
            (peer(3), hash(0xAA)),
        ];
        assert_eq!(tally(&three, 3), QuorumOutcome::Agreed(hash(0xAA)));
    }

    #[test]
    fn tally_split_when_two_hashes_reach_threshold() {
        let votes = [
            (peer(1), hash(0xAA)),
            (peer(2), hash(0xAA)),
            (peer(3), hash(0xBB)),
            (peer(4), hash(0xBB)),
        ];
        assert_eq!(tally(&votes, 2), QuorumOutcome::Split);
    }

    #[test]
    fn tally_is_order_independent() {
        // Malicious responders first, honest majority later: identical
        // outcome forwards and reversed, because the full set is tallied.
        let votes = [
            (peer(1), hash(0xFF)),
            (peer(2), hash(0xFF)),
            (peer(3), hash(0xAA)),
            (peer(4), hash(0xAA)),
            (peer(5), hash(0xAA)),
        ];
        assert_eq!(tally(&votes, 3), QuorumOutcome::Agreed(hash(0xAA)));
        let mut reversed = votes;
        reversed.reverse();
        assert_eq!(tally(&reversed, 3), QuorumOutcome::Agreed(hash(0xAA)));
    }

    /// Build a parent-linked header chain with the given seed byte.
    fn chain(start: u64, len: u64, seed: u8) -> Vec<Header> {
        let mut headers = Vec::new();
        let mut parent = B256::repeat_byte(seed);
        for number in start..start + len {
            let header = Header {
                number,
                parent_hash: parent,
                gas_limit: u64::from(seed),
                ..Default::default()
            };
            parent = SealedHeader::seal_slow(header.clone()).hash();
            headers.push(header);
        }
        headers
    }

    fn tip_of(headers: &[Header]) -> B256 {
        SealedHeader::seal_slow(headers.last().cloned().unwrap_or_default()).hash()
    }

    #[test]
    fn validate_accepts_honest_chain() -> eyre::Result<()> {
        let headers = chain(100, 5, 0x01);
        let tip = tip_of(&headers);
        let frontier = headers[0].parent_hash;

        let sealed = validate_canonical_chain(100, 104, headers, tip, Some(frontier), true)?;
        assert_eq!(sealed.len(), 5);
        assert_eq!(sealed[4].hash(), tip);
        Ok(())
    }

    #[test]
    fn validate_rejects_forged_empty_bloom_chain() {
        // A forger serves a fully self-consistent chain (empty blooms —
        // every block would bloom-skip) — but its tip cannot match the
        // hash the honest quorum agreed on.
        let honest = chain(100, 5, 0x01);
        let honest_tip = tip_of(&honest);
        let forged = chain(100, 5, 0x02);

        let result = validate_canonical_chain(100, 104, forged, honest_tip, None, false);
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("not on the canonical chain"));
    }

    #[test]
    fn validate_rejects_broken_links_and_gaps() {
        let mut headers = chain(100, 5, 0x01);
        let tip = tip_of(&headers);

        headers[2].gas_limit += 1;
        let result = validate_canonical_chain(100, 104, headers.clone(), tip, None, false);
        assert!(result.is_err());

        let short = chain(100, 4, 0x01);
        let result = validate_canonical_chain(100, 104, short, tip, None, false);
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("need 5"));
    }

    #[test]
    fn validate_rejects_poisoned_frontier() {
        // The canonical chain is honest, but the database's stored hash
        // below the segment is a forgery — the seam must refuse.
        let headers = chain(100, 5, 0x01);
        let tip = tip_of(&headers);
        let poisoned = B256::repeat_byte(0x66);

        let result = validate_canonical_chain(100, 104, headers, tip, Some(poisoned), true);
        assert!(result.is_err());
        let msg = format!("{result:?}");
        assert!(msg.contains("does not connect"));
        assert!(msg.contains("forged"));
    }

    #[test]
    fn validate_requires_seam_on_existing_state() {
        // Existing database (require_seam) with no stored hash below the
        // segment: refuse — the left seam cannot be verified.
        let headers = chain(100, 5, 0x01);
        let tip = tip_of(&headers);

        let result = validate_canonical_chain(100, 104, headers.clone(), tip, None, true);
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("left seam"));

        // Fresh database (no seam required) accepts a `None` frontier.
        assert!(validate_canonical_chain(100, 104, headers, tip, None, false).is_ok());
    }

    #[test]
    fn panel_rotates_across_rounds() {
        let peers: Vec<NetworkPeer<crate::chain::EthereumChain>> = Vec::new();
        // Empty pool returns empty panel regardless of round.
        assert!(select_panel(&peers, 3, 1).is_empty());
        assert!(select_panel(&peers, 3, 5).is_empty());
    }

    #[tokio::test]
    async fn quorum_refuses_without_enough_peers() {
        // No peers: quorum MUST fail fast. This is the gate that
        // suppresses ALL downstream work — payload fetch, commits, and
        // follow-mode notifications — because callers propagate the error
        // and never invoke the sync pipeline for an unverifiable segment.
        let pool = crate::p2p::PeerPool::<crate::chain::EthereumChain>::new_empty();
        let policy = fast_policy();
        let result = quorum_at(&pool, 100, &policy).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("cannot form a canonical header quorum"));
    }

    /// A policy that fails fast instead of waiting for production timeouts.
    fn fast_policy() -> QuorumPolicy {
        QuorumPolicy {
            required_matches: 3,
            panel_max: 8,
            peer_wait: Duration::from_millis(50),
            rounds: 1,
            round_delay: Duration::from_millis(1),
        }
    }

    async fn frontier_test_db() -> eyre::Result<Database> {
        let url = std::env::var("DATABASE_URL").map_err(|_| eyre::eyre!("DATABASE_URL not set"))?;
        let db = Database::connect(&url).await?;
        crate::db::create_internal_tables(&db).await?;
        Ok(db)
    }

    /// Reset the checkpoint / hashes / verified-frontier state these tests
    /// share (single-row tables — run with `--test-threads=1`).
    async fn reset_frontier_state(db: &Database) -> eyre::Result<()> {
        sqlx::query("UPDATE _sieve_checkpoints SET block_number = 0 WHERE id = 1")
            .execute(db.pool())
            .await?;
        sqlx::query("DELETE FROM _sieve_block_hashes")
            .execute(db.pool())
            .await?;
        sqlx::query("DELETE FROM _sieve_canonical")
            .execute(db.pool())
            .await?;
        Ok(())
    }

    async fn seed_hash(db: &Database, number: u64, hash: B256, parent: B256) -> eyre::Result<()> {
        let mut tx = db.begin().await?;
        crate::db::store_block_hashes_batch(
            &mut tx,
            &[number as i64],
            &[hash.as_slice().to_vec()],
            &[parent.as_slice().to_vec()],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_checkpoint(db: &Database, block: u64) -> eyre::Result<()> {
        let mut tx = db.begin().await?;
        crate::db::update_checkpoint(&mut tx, BlockNumber::new(block)).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_marker(db: &Database, block: u64, hash: B256) -> eyre::Result<()> {
        let mut tx = db.begin().await?;
        crate::db::advance_verified_frontier(&mut tx, BlockNumber::new(block), &hash).await?;
        tx.commit().await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn frontier_fresh_db_is_fresh() -> eyre::Result<()> {
        let db = frontier_test_db().await?;
        reset_frontier_state(&db).await?;
        // Fresh DB (checkpoint 0, no hashes) classifies as Fresh — the
        // first canonical segment will establish the marker.
        assert_eq!(committed_frontier_status(&db).await?, FrontierStatus::Fresh);
        reset_frontier_state(&db).await
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn frontier_refuses_legacy_state_without_marker() -> eyre::Result<()> {
        let db = frontier_test_db().await?;
        reset_frontier_state(&db).await?;
        // Existing indexed state (checkpoint 100, stored hash) but NO
        // canonical marker: a legacy/foreign database — refused, not
        // silently trusted. The interior is never blessed from one edge.
        seed_hash(&db, 100, B256::repeat_byte(0x22), B256::repeat_byte(0x21)).await?;
        set_checkpoint(&db, 100).await?;
        let result = committed_frontier_status(&db).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("no canonical verification marker"));
        reset_frontier_state(&db).await
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn frontier_refuses_marker_inconsistent_with_checkpoint() -> eyre::Result<()> {
        let db = frontier_test_db().await?;
        reset_frontier_state(&db).await?;
        // Marker says verified through 90, but the checkpoint is 100 — a
        // torn frontier. Refused.
        seed_hash(&db, 100, B256::repeat_byte(0x22), B256::repeat_byte(0x21)).await?;
        set_checkpoint(&db, 100).await?;
        set_marker(&db, 90, B256::repeat_byte(0x22)).await?;
        let result = committed_frontier_status(&db).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("inconsistent with the checkpoint"));
        reset_frontier_state(&db).await
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn frontier_accepts_consistent_marker() -> eyre::Result<()> {
        let db = frontier_test_db().await?;
        reset_frontier_state(&db).await?;
        // Marker matches the checkpoint and its stored hash: Committed,
        // ready for the network tip re-check.
        let hash = B256::repeat_byte(0x22);
        seed_hash(&db, 100, hash, B256::repeat_byte(0x21)).await?;
        set_checkpoint(&db, 100).await?;
        set_marker(&db, 100, hash).await?;
        assert_eq!(
            committed_frontier_status(&db).await?,
            FrontierStatus::Committed {
                checkpoint: 100,
                hash,
            }
        );
        reset_frontier_state(&db).await
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn rollback_moves_marker_and_never_creates_one() -> eyre::Result<()> {
        let db = frontier_test_db().await?;
        reset_frontier_state(&db).await?;

        // A Sieve-built DB: hashes + checkpoint + marker at 100.
        let h90 = B256::repeat_byte(0x90);
        let h100 = B256::repeat_byte(0xAA);
        seed_hash(&db, 90, h90, B256::repeat_byte(0x89)).await?;
        seed_hash(&db, 100, h100, B256::repeat_byte(0x99)).await?;
        set_checkpoint(&db, 100).await?;
        set_marker(&db, 100, h100).await?;

        // Rollback to 90 moves the marker in lockstep to (90, h90).
        let mut tx = db.begin().await?;
        crate::db::rollback_to(&mut tx, BlockNumber::new(90)).await?;
        tx.commit().await?;
        assert_eq!(
            committed_frontier_status(&db).await?,
            FrontierStatus::Committed {
                checkpoint: 90,
                hash: h90,
            }
        );

        // A LEGACY DB (no marker) must NOT gain one through rollback — it
        // still fails classification.
        sqlx::query("DELETE FROM _sieve_canonical")
            .execute(db.pool())
            .await?;
        let mut tx = db.begin().await?;
        crate::db::rollback_to(&mut tx, BlockNumber::new(90)).await?;
        tx.commit().await?;
        let result = committed_frontier_status(&db).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("no canonical verification marker"));

        reset_frontier_state(&db).await
    }
}
