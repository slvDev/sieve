//! Base static-file reconstruction adapted from Shinode snapshot/reader.rs (MIT).
use super::{manifest::Archive, plan::Group, staging::Staging};
use crate::{
    chain::{BaseChain, ChainKind},
    sync::{
        validation::{ArchiveEvidence, ArchiveRecoveryReader, ValidatedPayload},
        BlockPayload,
    },
};
use alloy_consensus::{proofs, BlockBody, Header, TxReceipt};
use alloy_primitives::B256;
use eyre::{ensure, eyre, Result, WrapErr};
use op_alloy_consensus::{OpReceipt, OpTxEnvelope};
use reth_codecs::Compact;
use reth_nippy_jar::{NippyJar, NippyJarCursor};
use reth_static_file_types::{SegmentHeader, StaticFileSegment};
use std::path::Path;

fn load(
    stage: &Staging,
    chunk: &Archive,
    group: &Group,
    kind: StaticFileSegment,
) -> Result<NippyJar<SegmentHeader>> {
    let file = chunk
        .files
        .iter()
        .find(|file| Path::new(&file.path).extension().is_none())
        .ok_or_else(|| eyre!("missing static file"))?;
    let jar = NippyJar::<SegmentHeader>::load(&stage.component_dir(chunk).join(&file.path))?;
    ensure!(
        jar.user_header().segment() == kind && jar.columns() == kind.columns(),
        "static-file segment/column mismatch"
    );
    ensure!(
        jar.user_header().block_start() == Some(group.archive_range[0]),
        "static-file start mismatch"
    );
    ensure!(
        jar.user_header().expected_block_start() == group.archive_range[0]
            && jar.user_header().expected_block_end() == group.archive_range[1],
        "static-file expected range differs from manifest"
    );
    ensure!(
        jar.user_header()
            .block_end()
            .is_some_and(|end| end >= group.archive_range[0] && end <= group.archive_range[1]),
        "static-file end mismatch"
    );
    Ok(jar)
}

fn compact<T: Compact>(bytes: &[u8]) -> Result<T> {
    // Upstream Compact exposes infallible decoders that can panic on an unknown
    // format. Convert that into an import error before any output is committed.
    // Compressed branches return the original input in their remainder slot.
    std::panic::catch_unwind(|| T::from_compact(bytes, bytes.len()).0)
        .map_err(|_| eyre!("unsupported or corrupt snapshot Compact encoding"))
}

pub(super) fn reconstruct(
    header: Header,
    max_transactions: u32,
    mut next: impl FnMut() -> Result<(OpTxEnvelope, OpReceipt)>,
) -> Result<ValidatedPayload<BaseChain>> {
    // OP bodies contain an empty withdrawals list from Canyon onward. Isthmus
    // repurposes the header root; the shared chain validator handles that rule.
    let mut body = BlockBody::<OpTxEnvelope> {
        withdrawals: header
            .withdrawals_root
            .map(|_| alloy_eips::eip4895::Withdrawals::default()),
        ..Default::default()
    };
    ensure!(
        proofs::calculate_ommers_root(&body.ommers) == header.ommers_hash,
        "missing ommers"
    );
    let mut receipts = Vec::new();
    let mut gas = 0;
    while gas != header.gas_used
        || proofs::calculate_transaction_root(&body.transactions) != header.transactions_root
    {
        ensure!(
            receipts.len() < max_transactions as usize,
            "per-block transaction limit reached"
        );
        let (tx, receipt) = next()?;
        ensure!(
            u8::from(tx.tx_type()) == u8::from(receipt.tx_type()),
            "transaction/receipt type mismatch"
        );
        let cumulative = receipt.cumulative_gas_used();
        ensure!(
            cumulative >= gas && cumulative <= header.gas_used,
            "receipt gas outside block"
        );
        gas = cumulative;
        body.transactions.push(tx);
        receipts.push(receipt);
    }
    ValidatedPayload::new(BlockPayload::new(header, body, receipts)).map_err(|reason| eyre!(reason))
}

pub(super) struct HeaderReader {
    jars: Vec<NippyJar<SegmentHeader>>,
    evidence: ArchiveEvidence,
}

impl HeaderReader {
    pub fn open(stage: &Staging, groups: &[Group], evidence: ArchiveEvidence) -> Result<Self> {
        let jars = groups
            .iter()
            .map(|group| {
                let jar = load(stage, &group.archives[0], group, StaticFileSegment::Headers)?;
                ensure!(
                    jar.rows() as u64 == group.available_range[1] - group.available_range[0] + 1
                        && jar.user_header().block_end() == Some(group.available_range[1]),
                    "truncated header jar"
                );
                Ok(jar)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { jars, evidence })
    }
}
impl ArchiveRecoveryReader for HeaderReader {
    fn headers<'a>(
        &'a self,
        evidence: &ArchiveEvidence,
    ) -> Result<Box<dyn Iterator<Item = Result<Header>> + 'a>> {
        ensure!(
            *evidence == self.evidence,
            "retained header evidence identity mismatch"
        );
        self.range(evidence.first_block, evidence.anchor_block)
    }
}
impl HeaderReader {
    pub fn range(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Box<dyn Iterator<Item = Result<Header>> + '_>> {
        ensure!(
            start >= self.evidence.first_block && start <= end && end <= self.evidence.anchor_block,
            "header range outside retained evidence"
        );
        let mut jars = self
            .jars
            .iter()
            .filter(move |jar| jar.user_header().block_end().is_some_and(|n| n >= start));
        let mut cursor: Option<NippyJarCursor<'_, SegmentHeader>> = None;
        let mut next = start;
        Ok(Box::new(std::iter::from_fn(move || {
            if next > end {
                return None;
            }
            let number = next;
            next += 1;
            Some((|| {
                if cursor
                    .as_ref()
                    .is_none_or(|c| c.row_index() as usize == c.jar().rows())
                {
                    let jar = jars
                        .next()
                        .ok_or_else(|| eyre!("missing retained header jar"))?;
                    cursor = Some(NippyJarCursor::new(jar)?);
                }
                let cursor = cursor
                    .as_mut()
                    .ok_or_else(|| eyre!("missing header cursor"))?;
                let offset = number
                    .checked_sub(
                        cursor
                            .jar()
                            .user_header()
                            .block_start()
                            .ok_or_else(|| eyre!("missing header start"))?,
                    )
                    .ok_or_else(|| eyre!("header range gap"))?;
                let row = cursor
                    .row_by_number(usize::try_from(offset)?)?
                    .ok_or_else(|| eyre!("missing retained header"))?;
                ensure!(row.len() == 3, "wrong header columns");
                let header: Header = compact(row[0])?;
                ensure!(
                    header.number == number && row[2] == header.hash_slow().as_slice(),
                    "retained header number/hash mismatch"
                );
                Ok(header)
            })())
        })))
    }
}

/// Matching metadata is checked before ingestion; complete consumption in scan
/// checks that these exact rows were assigned before the last block is emitted.
pub(super) fn transaction_range(stage: &Staging, group: &Group) -> Result<Option<[u64; 2]>> {
    let transactions = load(
        stage,
        &group.archives[1],
        group,
        StaticFileSegment::Transactions,
    )?;
    let receipts = load(
        stage,
        &group.archives[2],
        group,
        StaticFileSegment::Receipts,
    )?;
    validate_payload_metadata(&transactions, &receipts, group.available_range[1])?;
    transactions
        .user_header()
        .tx_range()
        .map(|range| {
            Ok([
                range.start(),
                range
                    .end()
                    .checked_add(1)
                    .ok_or_else(|| eyre!("transaction range overflow"))?,
            ])
        })
        .transpose()
}

fn validate_payload_metadata(
    transactions: &NippyJar<SegmentHeader>,
    receipts: &NippyJar<SegmentHeader>,
    end: u64,
) -> Result<()> {
    ensure!(
        transactions.rows() == receipts.rows()
            && transactions.user_header().tx_range() == receipts.user_header().tx_range(),
        "transaction/receipt rows differ"
    );
    ensure!(
        transactions.user_header().tx_len().unwrap_or(0) == transactions.rows() as u64,
        "transaction range does not match row count"
    );
    ensure!(
        transactions.user_header().block_end() == Some(end)
            && receipts.user_header().block_end() == Some(end),
        "incomplete transaction/receipt block range"
    );
    Ok(())
}

#[derive(Default)]
struct Position {
    previous: Option<B256>,
    block: u64,
    transaction: u64,
    transaction_known: bool,
}

pub(super) fn scan(
    stage: &Staging,
    group: &Group,
    max_transactions: u32,
    mut consume: impl FnMut(ValidatedPayload<BaseChain>) -> Result<()>,
) -> Result<()> {
    let mut position = Position {
        block: group.decode_from,
        transaction_known: group.decode_from == 0,
        ..Position::default()
    };
    let headers = load(stage, &group.archives[0], group, StaticFileSegment::Headers)?;
    let transactions = load(
        stage,
        &group.archives[1],
        group,
        StaticFileSegment::Transactions,
    )?;
    let receipts = load(
        stage,
        &group.archives[2],
        group,
        StaticFileSegment::Receipts,
    )?;
    let end = group.available_range[1];
    ensure!(
        headers.rows() as u64 == end - group.archive_range[0] + 1
            && headers.user_header().block_end() == Some(end),
        "header chunk is truncated"
    );
    ensure!(
        position.block == group.archive_range[0],
        "gap between header chunks"
    );
    validate_payload_metadata(&transactions, &receipts, end)?;
    if !position.transaction_known {
        if let Some(first) = transactions.user_header().tx_start() {
            position.transaction = first;
            position.transaction_known = true;
        }
    }
    ensure!(
        transactions.rows() == 0
            || transactions.user_header().tx_start() == Some(position.transaction),
        "transaction-number discontinuity"
    );
    let first_transaction = position.transaction;
    let mut h = NippyJarCursor::new(&headers)?;
    let mut t = NippyJarCursor::new(&transactions)?;
    let mut r = NippyJarCursor::new(&receipts)?;
    while position.block <= end.min(group.index_range[1]) {
        let row = h.next_row()?.ok_or_else(|| eyre!("missing header row"))?;
        ensure!(row.len() == 3, "wrong header columns");
        let header: Header = compact(row[0])?;
        let hash = header.hash_slow();
        ensure!(
            header.number == position.block && row[2] == hash.as_slice(),
            "header number/hash mismatch"
        );
        if let Some(parent) = position.previous {
            ensure!(header.parent_hash == parent, "broken parent continuity");
        } else if position.block == 0 {
            ensure!(
                hash == ChainKind::Base.genesis_hash(),
                "snapshot genesis mismatch"
            );
        }
        let payload = reconstruct(header, max_transactions, || {
            let row = t
                .next_row()?
                .ok_or_else(|| eyre!("missing transaction row"))?;
            let tx = compact(
                row.first()
                    .ok_or_else(|| eyre!("missing transaction column"))?,
            )?;
            let row = r.next_row()?.ok_or_else(|| eyre!("missing receipt row"))?;
            let receipt = compact(row.first().ok_or_else(|| eyre!("missing receipt column"))?)?;
            position.transaction += 1;
            Ok((tx, receipt))
        })
        .wrap_err_with(|| format!("reconstructing block {}", position.block))?;
        if position.block == end {
            ensure!(
                position.transaction - first_transaction == transactions.rows() as u64,
                "unassigned transactions at archive boundary"
            );
        }
        consume(payload)?;
        position.previous = Some(hash);
        position.block += 1;
    }
    if position.block == end + 1 {
        ensure!(
            position.transaction - first_transaction == transactions.rows() as u64,
            "unassigned transactions at archive boundary"
        );
    }
    Ok(())
}
