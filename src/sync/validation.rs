//! Authenticated headers and payload commitment checks shared by block sources.

use super::BlockPayload;
use crate::chain::ChainTypes;
use alloy_consensus::{proofs, BlockBody};
use reth_primitives_traits::Header;

/// Validate a fetched body and receipts against the block header.
///
/// Recomputes the transaction, ommers, withdrawals, and receipt roots from
/// the fetched data and compares them to the header commitments. Receipt
/// blooms are recomputed from logs, never trusted from the peer.
pub fn validate_payload<C: ChainTypes>(
    header: &Header,
    body: &BlockBody<C::SignedTx>,
    receipts: &[C::Receipt],
) -> Result<(), &'static str> {
    if body.transactions.len() != receipts.len() {
        return Err("transaction/receipt count mismatch");
    }
    if proofs::calculate_transaction_root(&body.transactions) != header.transactions_root {
        return Err("transactions root mismatch");
    }
    if proofs::calculate_ommers_root(&body.ommers) != header.ommers_hash {
        return Err("ommers root mismatch");
    }
    if !C::withdrawals_valid(header, body) {
        return Err("withdrawals mismatch");
    }
    if C::receipts_root(receipts, header) != header.receipts_root {
        return Err("receipts root mismatch");
    }
    Ok(())
}

/// A payload whose chain-specific commitments have been checked. The inner
/// payload cannot be modified between validation and ordered processing.
#[derive(Debug)]
pub struct ValidatedPayload<C: ChainTypes>(BlockPayload<C>);

impl<C: ChainTypes> ValidatedPayload<C> {
    pub fn new(payload: BlockPayload<C>) -> Result<Self, &'static str> {
        validate_payload::<C>(payload.header(), payload.body(), payload.receipts())?;
        Ok(Self(payload))
    }

    pub const fn header(&self) -> &Header {
        self.0.header()
    }
    pub(super) fn into_inner(self) -> BlockPayload<C> {
        self.0
    }
}

use super::canonical::{CanonicalChain, CANONICAL_SEGMENT_BLOCKS};
use alloy_primitives::B256;
use eyre::{ensure, Result as EyreResult};
use reth_chainspec::EthChainSpec;
use reth_primitives_traits::SealedHeader;
use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, sync::Arc};

/// Durable trust inputs, not proof by themselves. The future jar reader must
/// reopen this pinned source and supply its complete consecutive header chain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveEvidence {
    pub manifest_sha256: B256,
    pub genesis_hash: B256,
    pub first_block: u64,
    pub anchor_block: u64,
    pub anchor_hash: B256,
}

impl ArchiveEvidence {
    pub fn validate(&self) -> EyreResult<()> {
        ensure!(
            self.genesis_hash == crate::chain::BaseChain::chain_spec().genesis_hash(),
            "archive evidence is not for Base mainnet"
        );
        ensure!(
            self.first_block <= self.anchor_block && self.anchor_block < i64::MAX as u64,
            "invalid archive evidence range"
        );
        Ok(())
    }
}

/// Archive boundary retained even after a later P2P frontier replaces it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveFrontier {
    pub block: u64,
    pub hash: B256,
    pub evidence: ArchiveEvidence,
}

/// Headers must originate in the quorum verifier or the archive anchor check.
/// Private fields prevent a source from blessing an arbitrary header vector.
pub struct AuthenticatedSegment<C: ChainTypes> {
    headers: SegmentHeaders,
    chain: PhantomData<C>,
}

enum SegmentHeaders {
    Peer(Arc<CanonicalChain>),
    Archive {
        start: u64,
        headers: Vec<SealedHeader>,
        evidence: ArchiveEvidence,
    },
}

impl<C: ChainTypes> AuthenticatedSegment<C> {
    pub const fn peer(chain: Arc<CanonicalChain>) -> Self {
        Self {
            headers: SegmentHeaders::Peer(chain),
            chain: PhantomData,
        }
    }
    pub fn start(&self) -> u64 {
        match &self.headers {
            SegmentHeaders::Peer(c) => c.start(),
            SegmentHeaders::Archive { start, .. } => *start,
        }
    }
    pub fn end(&self) -> u64 {
        match &self.headers {
            SegmentHeaders::Peer(c) => c.end(),
            SegmentHeaders::Archive { start, headers, .. } => start + headers.len() as u64 - 1,
        }
    }
    pub fn get(&self, number: u64) -> Option<&SealedHeader> {
        match &self.headers {
            SegmentHeaders::Peer(c) => c.get(number),
            SegmentHeaders::Archive { start, headers, .. } => number
                .checked_sub(*start)
                .and_then(|i| headers.get(i as usize)),
        }
    }
    pub const fn archive_evidence(&self) -> Option<&ArchiveEvidence> {
        match &self.headers {
            SegmentHeaders::Peer(_) => None,
            SegmentHeaders::Archive { evidence, .. } => Some(evidence),
        }
    }
}

impl AuthenticatedSegment<crate::chain::BaseChain> {
    /// Verify retained headers through the independently trusted anchor, keeping
    /// only the bounded segment being submitted.
    pub fn archive(
        evidence: ArchiveEvidence,
        headers: impl IntoIterator<Item = EyreResult<Header>>,
        start: u64,
        end: u64,
    ) -> EyreResult<Self> {
        let headers = verify_archive_headers(&evidence, headers, start, end)?;
        Ok(Self {
            headers: SegmentHeaders::Archive {
                start,
                headers,
                evidence,
            },
            chain: PhantomData,
        })
    }
}

/// Replay the complete retained header evidence with bounded memory. This does
/// not trust a remembered per-group hash in place of a chain to the anchor.
pub fn verify_archive_headers(
    evidence: &ArchiveEvidence,
    headers: impl IntoIterator<Item = EyreResult<Header>>,
    start: u64,
    end: u64,
) -> EyreResult<Vec<SealedHeader>> {
    evidence.validate()?;
    ensure!(
        start >= evidence.first_block && start <= end && end <= evidence.anchor_block,
        "segment outside archive evidence"
    );
    ensure!(
        end - start < CANONICAL_SEGMENT_BLOCKS,
        "archive segment exceeds bounded ingestion window"
    );
    let mut next = evidence.first_block;
    let mut previous = None;
    let mut selected = Vec::new();
    for header in headers {
        let header = header?;
        ensure!(
            next <= evidence.anchor_block && header.number == next,
            "archive evidence has missing, extra, or unordered headers at {next}"
        );
        if let Some(hash) = previous {
            ensure!(
                header.parent_hash == hash,
                "archive header chain breaks at {next}"
            );
        }
        let sealed = SealedHeader::seal_slow(header);
        if next == 0 {
            ensure!(
                sealed.hash() == evidence.genesis_hash,
                "archive genesis mismatch"
            );
        }
        previous = Some(sealed.hash());
        if (start..=end).contains(&next) {
            selected.push(sealed);
        }
        next += 1;
    }
    ensure!(
        next == evidence.anchor_block + 1,
        "archive header evidence is truncated"
    );
    ensure!(
        previous == Some(evidence.anchor_hash),
        "archive header chain does not reach trusted anchor"
    );
    Ok(selected)
}

/// Reader contract for startup. Implementations reopen retained jars for exactly
/// the recorded manifest and return all headers from first_block through anchor.
/// No peer/RPC fallback may silently replace this trust input.
pub trait ArchiveRecoveryReader: Send + Sync {
    fn headers<'a>(
        &'a self,
        evidence: &ArchiveEvidence,
    ) -> EyreResult<Box<dyn Iterator<Item = EyreResult<Header>> + 'a>>;
}

pub fn verify_archive_recovery(
    frontier: &ArchiveFrontier,
    reader: &dyn ArchiveRecoveryReader,
) -> EyreResult<()> {
    let headers = verify_archive_headers(
        &frontier.evidence,
        reader.headers(&frontier.evidence)?,
        frontier.block,
        frontier.block,
    )?;
    ensure!(
        headers.first().map(SealedHeader::hash) == Some(frontier.hash),
        "retained archive evidence does not authenticate committed frontier"
    );
    Ok(())
}
