//! Event filter — matches raw logs against user-defined contract/event criteria.
//!
//! For each receipt in a fetched block:
//! 1. Check if any log's address matches a configured contract
//! 2. Check if topic0 matches a configured event signature
//! 3. If both match, collect the log with its block/tx context
//!
//! This runs at sync time, discarding non-matching data immediately.

use crate::config::IndexConfig;
use crate::sync::BlockPayload;
use alloy_primitives::{Log, LogData, B256};
use tracing::warn;

/// A log that matched the user's filter criteria, with block context.
pub struct FilteredLog {
    /// The raw log (address + topics + data).
    pub log: Log<LogData>,
    /// Block number where this log was emitted.
    pub block_number: u64,
    /// Transaction hash that produced this log.
    pub tx_hash: B256,
    /// Transaction index within the block.
    pub tx_index: usize,
    /// Log index within the transaction's receipt.
    pub log_index: usize,
}

/// Filter a block's receipts against the index config.
///
/// Returns only logs whose address matches a configured contract AND whose
/// topic0 matches a configured event selector.
///
/// Pre-computes tx hashes, validates receipt count matches transaction count,
/// then zips for iteration.
#[must_use]
pub fn filter_block(payload: &BlockPayload, config: &IndexConfig) -> Vec<FilteredLog> {
    let block_number = payload.header.number;

    // Validate receipt/transaction count match
    if payload.receipts.len() != payload.body.transactions.len() {
        warn!(
            block_number,
            receipts = payload.receipts.len(),
            transactions = payload.body.transactions.len(),
            "receipt/transaction count mismatch, skipping block"
        );
        return Vec::new();
    }

    // Pre-compute tx hashes upfront
    let tx_hashes: Vec<B256> = payload
        .body
        .transactions
        .iter()
        .map(|tx| *tx.tx_hash())
        .collect();

    let mut matched = Vec::new();

    for (tx_index, (receipt, tx_hash)) in payload
        .receipts
        .iter()
        .zip(tx_hashes.iter())
        .enumerate()
    {
        for (log_index, log) in receipt.logs.iter().enumerate() {
            let topics = log.data.topics();

            // Need at least topic0 (the event selector)
            let Some(&topic0) = topics.first() else {
                continue;
            };

            // Check address matches a configured contract
            let Some(contract) = config.contract_for_address(&log.address) else {
                continue;
            };

            // Check topic0 matches a configured event
            if contract.events.contains_key(&topic0) {
                matched.push(FilteredLog {
                    log: log.clone(),
                    block_number,
                    tx_hash: *tx_hash,
                    tx_index,
                    log_index,
                });
            }
        }
    }

    matched
}

#[cfg(test)]
#[expect(clippy::panic_in_result_fn, reason = "assertions in tests are idiomatic")]
mod tests {
    use super::*;
    use crate::config::usdc_transfer_config;
    use alloy_primitives::{address, bytes, Address, Bytes, B256};
    use reth_ethereum_primitives::{BlockBody, Receipt};
    use reth_primitives_traits::Header;

    /// Build a log with given address and topics.
    fn make_log(addr: Address, topics: Vec<B256>, data: Bytes) -> Log<LogData> {
        Log {
            address: addr,
            data: LogData::new_unchecked(topics, data),
        }
    }

    /// Build a minimal receipt with given logs.
    fn make_receipt(logs: Vec<Log<LogData>>) -> Receipt {
        Receipt {
            tx_type: alloy_consensus::TxType::Legacy,
            success: true,
            cumulative_gas_used: 0,
            logs,
        }
    }

    #[test]
    fn filter_matches_correct_logs() -> eyre::Result<()> {
        let config = usdc_transfer_config()?;
        let usdc_addr = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let transfer_selector: B256 =
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                .parse()
                .map_err(|e| eyre::eyre!("parse: {e}"))?;
        let wrong_addr = address!("0000000000000000000000000000000000000001");

        // Log 0: matching (USDC address + Transfer topic)
        let matching_log = make_log(
            usdc_addr,
            vec![transfer_selector, B256::ZERO, B256::ZERO],
            bytes!("0000000000000000000000000000000000000000000000000000000000000064"),
        );

        // Log 1: wrong address
        let wrong_addr_log = make_log(
            wrong_addr,
            vec![transfer_selector],
            Bytes::new(),
        );

        // Log 2: right address, wrong topic
        let wrong_topic_log = make_log(
            usdc_addr,
            vec![B256::ZERO],
            Bytes::new(),
        );

        let receipt = make_receipt(vec![matching_log, wrong_addr_log, wrong_topic_log]);

        // We need a transaction in the body for the tx_hash lookup.
        // Build a minimal legacy transaction.
        let tx = build_test_transaction();
        let body = BlockBody {
            transactions: vec![tx],
            ommers: vec![],
            withdrawals: None,
        };

        let header = Header {
            number: 21_000_042,
            ..Default::default()
        };

        let payload = BlockPayload {
            header,
            body,
            receipts: vec![receipt],
        };

        let matched = filter_block(&payload, &config);

        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].block_number, 21_000_042);
        assert_eq!(matched[0].log.address, usdc_addr);
        assert_eq!(matched[0].tx_index, 0);
        assert_eq!(matched[0].log_index, 0);
        Ok(())
    }

    #[test]
    fn filter_skips_block_on_receipt_tx_mismatch() -> eyre::Result<()> {
        let config = usdc_transfer_config()?;
        let usdc_addr = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let transfer_selector: B256 =
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                .parse()
                .map_err(|e| eyre::eyre!("parse: {e}"))?;

        // One receipt with a matching log, but zero transactions in body
        let log = make_log(usdc_addr, vec![transfer_selector], Bytes::new());
        let receipt = make_receipt(vec![log]);
        let body = BlockBody {
            transactions: vec![],
            ommers: vec![],
            withdrawals: None,
        };
        let payload = BlockPayload {
            header: Header::default(),
            body,
            receipts: vec![receipt],
        };

        let matched = filter_block(&payload, &config);
        assert!(matched.is_empty(), "should skip block on count mismatch");
        Ok(())
    }

    #[test]
    fn filter_empty_block() -> eyre::Result<()> {
        let config = usdc_transfer_config()?;
        let header = Header::default();
        let body = BlockBody {
            transactions: vec![],
            ommers: vec![],
            withdrawals: None,
        };
        let payload = BlockPayload {
            header,
            body,
            receipts: vec![],
        };

        let matched = filter_block(&payload, &config);
        assert!(matched.is_empty());
        Ok(())
    }

    /// Build a minimal legacy transaction for testing.
    fn build_test_transaction() -> reth_ethereum_primitives::TransactionSigned {
        use alloy_consensus::{Signed, TxLegacy};
        use alloy_primitives::{Signature, TxKind, U256};

        let tx = TxLegacy {
            chain_id: Some(1),
            nonce: 0,
            gas_price: 0,
            gas_limit: 21000,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Bytes::new(),
        };

        let sig = Signature::new(U256::from(1u64), U256::from(1u64), false);
        let signed = Signed::new_unchecked(tx, sig, B256::repeat_byte(0xAA));
        reth_ethereum_primitives::TransactionSigned::Legacy(signed)
    }
}
