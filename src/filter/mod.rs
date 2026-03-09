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
use crate::toml_config::ResolvedFactory;
use crate::types::{BlockNumber, LogIndex, TxIndex};
use alloy_dyn_abi::{DynSolValue, EventExt};
use alloy_primitives::{Address, BloomInput, Log, LogData, B256};
use reth_primitives_traits::Header;
use std::collections::HashMap;
use tracing::{debug, warn};

// ── Bloom filter pre-screening ──────────────────────────────────────

/// Pre-computed set of contract addresses for bloom filter pre-screening.
///
/// Ethereum block headers contain a `logs_bloom` (2048-bit bloom filter) that
/// encodes all log-emitting addresses and topics. Checking our configured
/// contract addresses against the bloom lets us skip fetching bodies+receipts
/// for blocks that definitely contain no matching events (~98% of blocks).
///
/// False positives are possible (bloom says "maybe" but no real match).
/// False negatives are impossible — if an event exists, the bloom WILL match.
#[derive(Debug)]
pub struct BloomFilter {
    addresses: Vec<Address>,
}

impl BloomFilter {
    /// Create a new bloom filter with the given contract addresses.
    #[must_use]
    pub fn new(addresses: Vec<Address>) -> Self {
        Self { addresses }
    }

    /// Check if any configured address might have emitted a log in this block.
    #[must_use]
    pub fn header_may_match(&self, header: &Header) -> bool {
        self.addresses.iter().any(|addr| {
            header
                .logs_bloom
                .contains_input(BloomInput::Raw(addr.as_slice()))
        })
    }
}

/// A log that matched the user's filter criteria, with block context.
pub struct FilteredLog {
    /// The raw log (address + topics + data).
    pub log: Log<LogData>,
    /// Block number where this log was emitted.
    pub block_number: BlockNumber,
    /// Block timestamp (seconds since epoch).
    pub block_timestamp: u64,
    /// Transaction hash that produced this log.
    pub tx_hash: B256,
    /// Transaction index within the block.
    pub tx_index: TxIndex,
    /// Log index within the transaction's receipt.
    pub log_index: LogIndex,
}

// Compile-time size assertion for hot type (reth pattern).
#[cfg(target_pointer_width = "64")]
const _: [(); 136] = [(); core::mem::size_of::<FilteredLog>()];

/// Filter a block's receipts against the index config.
///
/// Returns only logs whose address matches a configured contract AND whose
/// topic0 matches a configured event selector.
///
/// Pre-computes tx hashes, validates receipt count matches transaction count,
/// then zips for iteration.
#[must_use]
pub fn filter_block(payload: &BlockPayload, config: &IndexConfig) -> Vec<FilteredLog> {
    let block_number = BlockNumber::new(payload.header().number);
    let block_timestamp = payload.header().timestamp;

    // Validate receipt/transaction count match
    if payload.receipts().len() != payload.body().transactions.len() {
        warn!(
            block_number = block_number.as_u64(),
            receipts = payload.receipts().len(),
            transactions = payload.body().transactions.len(),
            "receipt/transaction count mismatch, skipping block"
        );
        return Vec::new();
    }

    // Pre-compute tx hashes upfront
    let tx_hashes: Vec<B256> = payload
        .body()
        .transactions
        .iter()
        .map(|tx| *tx.tx_hash())
        .collect();

    let mut matched = Vec::new();

    for (tx_idx, (receipt, tx_hash)) in payload.receipts().iter().zip(tx_hashes.iter()).enumerate()
    {
        let tx_index = TxIndex::from_usize(tx_idx);
        for (log_idx, log) in receipt.logs.iter().enumerate() {
            let log_index = LogIndex::from_usize(log_idx);
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
                // Check indexed parameter filters (if any configured for this event)
                if let Some(filters) = contract.topic_filters.get(&topic0) {
                    let pass = filters.iter().all(|tf| {
                        topics
                            .get(tf.topic_index)
                            .is_some_and(|t| tf.values.contains(t))
                    });
                    if !pass {
                        continue;
                    }
                }

                matched.push(FilteredLog {
                    log: log.clone(),
                    block_number,
                    block_timestamp,
                    tx_hash: *tx_hash,
                    tx_index,
                    log_index,
                });
            }
        }
    }

    matched
}

// ── Factory discovery ────────────────────────────────────────────────

/// A discovered factory-created child contract.
#[derive(Debug)]
pub struct FactoryDiscovery {
    /// Name of the child contract (for handler/table lookup).
    pub child_contract_name: String,
    /// Address of the newly created child contract.
    pub child_address: Address,
    /// Block number where the creation event was emitted.
    pub block_number: u64,
}

/// Scan a block's logs for factory creation events.
///
/// Returns discovered child addresses. Zero overhead if `factories` is empty.
#[must_use]
pub fn scan_factory_events(
    payload: &BlockPayload,
    factories: &[ResolvedFactory],
) -> Vec<FactoryDiscovery> {
    if factories.is_empty() {
        return Vec::new();
    }

    // Build lookup: (factory_address, creation_selector) → &ResolvedFactory
    let mut lookup: HashMap<(Address, B256), &ResolvedFactory> =
        HashMap::with_capacity(factories.len());
    for factory in factories {
        lookup.insert(
            (factory.factory_address, factory.creation_selector),
            factory,
        );
    }

    let mut discoveries = Vec::new();

    for receipt in payload.receipts() {
        for log in &receipt.logs {
            let topics = log.data.topics();
            let Some(&topic0) = topics.first() else {
                continue;
            };

            let Some(factory) = lookup.get(&(log.address, topic0)) else {
                continue;
            };

            if let Some(child_addr) = decode_child_address(log, factory) {
                debug!(
                    factory = %factory.child_contract_name,
                    child = ?child_addr,
                    block = payload.header().number,
                    "discovered factory child"
                );
                discoveries.push(FactoryDiscovery {
                    child_contract_name: factory.child_contract_name.clone(),
                    child_address: child_addr,
                    block_number: payload.header().number,
                });
            }
        }
    }

    discoveries
}

/// Decode a creation log to extract the child contract address.
fn decode_child_address(log: &Log<LogData>, factory: &ResolvedFactory) -> Option<Address> {
    let decoded = factory.creation_event.decode_log(&log.data).ok()?;

    // Search indexed params first, then body params
    for (i, input) in factory.creation_event.inputs.iter().enumerate() {
        if input.name == factory.child_address_param {
            let value = if input.indexed {
                // Find position among indexed params
                let indexed_pos = factory
                    .creation_event
                    .inputs
                    .iter()
                    .take(i + 1)
                    .filter(|p| p.indexed)
                    .count()
                    - 1;
                decoded.indexed.get(indexed_pos)?
            } else {
                // Find position among body params
                let body_pos = factory
                    .creation_event
                    .inputs
                    .iter()
                    .take(i + 1)
                    .filter(|p| !p.indexed)
                    .count()
                    - 1;
                decoded.body.get(body_pos)?
            };

            if let DynSolValue::Address(addr) = value {
                return Some(*addr);
            }
        }
    }

    None
}

#[cfg(test)]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions in tests are idiomatic"
)]
mod tests {
    use super::*;
    use crate::config::usdc_transfer_config;
    use crate::test_utils::{build_test_transaction, make_log, make_receipt};
    use crate::types::{BlockNumber, LogIndex, TxIndex};
    use alloy_primitives::{address, bytes, Bytes, B256};
    use reth_ethereum_primitives::BlockBody;
    use reth_primitives_traits::Header;

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
        let wrong_addr_log = make_log(wrong_addr, vec![transfer_selector], Bytes::new());

        // Log 2: right address, wrong topic
        let wrong_topic_log = make_log(usdc_addr, vec![B256::ZERO], Bytes::new());

        let receipt = make_receipt(vec![matching_log, wrong_addr_log, wrong_topic_log]);

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

        let payload = BlockPayload::new(header, body, vec![receipt]);

        let matched = filter_block(&payload, &config);

        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].block_number, BlockNumber::new(21_000_042));
        assert_eq!(matched[0].log.address, usdc_addr);
        assert_eq!(matched[0].tx_index, TxIndex::new(0));
        assert_eq!(matched[0].log_index, LogIndex::new(0));
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

        let log = make_log(usdc_addr, vec![transfer_selector], Bytes::new());
        let receipt = make_receipt(vec![log]);
        let body = BlockBody {
            transactions: vec![],
            ommers: vec![],
            withdrawals: None,
        };
        let payload = BlockPayload::new(Header::default(), body, vec![receipt]);

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
        let payload = BlockPayload::new(header, body, vec![]);

        let matched = filter_block(&payload, &config);
        assert!(matched.is_empty());
        Ok(())
    }

    // ── Factory tests ────────────────────────────────────────────────

    #[test]
    fn filter_with_topic_filter_matches() -> eyre::Result<()> {
        use crate::toml_config::TopicFilter;
        use std::collections::HashSet;

        let mut config = usdc_transfer_config()?;
        let usdc_addr = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let transfer_selector: B256 =
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                .parse()
                .map_err(|e| eyre::eyre!("parse: {e}"))?;

        // Filter: only topic2 (to) matching a specific address
        let target_addr = address!("0000000000000000000000000000000000000042");
        let target_topic = B256::left_padding_from(target_addr.as_slice());
        config.contracts[0].topic_filters.insert(
            transfer_selector,
            vec![TopicFilter {
                topic_index: 2,
                values: HashSet::from([target_topic]),
            }],
        );

        // Log matching the filter (to = target)
        let matching_log = make_log(
            usdc_addr,
            vec![transfer_selector, B256::ZERO, target_topic],
            bytes!("0000000000000000000000000000000000000000000000000000000000000064"),
        );

        // Log NOT matching the filter (to = something else)
        let non_matching_log = make_log(
            usdc_addr,
            vec![transfer_selector, B256::ZERO, B256::ZERO],
            bytes!("0000000000000000000000000000000000000000000000000000000000000064"),
        );

        let receipt = make_receipt(vec![matching_log, non_matching_log]);
        let tx1 = build_test_transaction();
        let tx2 = build_test_transaction();
        let body = BlockBody {
            transactions: vec![tx1, tx2],
            ommers: vec![],
            withdrawals: None,
        };
        let header = Header {
            number: 21_000_042,
            ..Default::default()
        };
        // Two txs but one receipt — need to match
        let receipt2 = make_receipt(vec![]);
        let payload = BlockPayload::new(header, body, vec![receipt, receipt2]);

        let matched = filter_block(&payload, &config);
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].log_index, LogIndex::new(0)); // Only the first log matched
        Ok(())
    }

    #[test]
    fn filter_without_topic_filter_accepts_all() -> eyre::Result<()> {
        // This is the existing behavior — no topic filters means all events match
        let config = usdc_transfer_config()?;
        assert!(config.contracts[0].topic_filters.is_empty());

        let usdc_addr = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let transfer_selector: B256 =
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                .parse()
                .map_err(|e| eyre::eyre!("parse: {e}"))?;

        let log = make_log(
            usdc_addr,
            vec![transfer_selector, B256::ZERO, B256::ZERO],
            bytes!("0000000000000000000000000000000000000000000000000000000000000064"),
        );
        let receipt = make_receipt(vec![log]);
        let tx = build_test_transaction();
        let body = BlockBody {
            transactions: vec![tx],
            ommers: vec![],
            withdrawals: None,
        };
        let payload = BlockPayload::new(Header::default(), body, vec![receipt]);

        let matched = filter_block(&payload, &config);
        assert_eq!(matched.len(), 1);
        Ok(())
    }

    #[test]
    fn filter_with_multi_param_and_logic() -> eyre::Result<()> {
        use crate::toml_config::TopicFilter;
        use std::collections::HashSet;

        let mut config = usdc_transfer_config()?;
        let usdc_addr = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let transfer_selector: B256 =
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                .parse()
                .map_err(|e| eyre::eyre!("parse: {e}"))?;

        let from_addr = address!("0000000000000000000000000000000000000001");
        let to_addr = address!("0000000000000000000000000000000000000002");
        let from_topic = B256::left_padding_from(from_addr.as_slice());
        let to_topic = B256::left_padding_from(to_addr.as_slice());

        // Filter: both from AND to must match
        config.contracts[0].topic_filters.insert(
            transfer_selector,
            vec![
                TopicFilter {
                    topic_index: 1,
                    values: HashSet::from([from_topic]),
                },
                TopicFilter {
                    topic_index: 2,
                    values: HashSet::from([to_topic]),
                },
            ],
        );

        // Log with correct from but wrong to → should NOT match
        let log_wrong_to = make_log(
            usdc_addr,
            vec![transfer_selector, from_topic, B256::ZERO],
            bytes!("0000000000000000000000000000000000000000000000000000000000000064"),
        );

        // Log with correct from AND to → should match
        let log_match = make_log(
            usdc_addr,
            vec![transfer_selector, from_topic, to_topic],
            bytes!("0000000000000000000000000000000000000000000000000000000000000064"),
        );

        let receipt1 = make_receipt(vec![log_wrong_to]);
        let receipt2 = make_receipt(vec![log_match]);
        let tx1 = build_test_transaction();
        let tx2 = build_test_transaction();
        let body = BlockBody {
            transactions: vec![tx1, tx2],
            ommers: vec![],
            withdrawals: None,
        };
        let header = Header {
            number: 21_000_042,
            ..Default::default()
        };
        let payload = BlockPayload::new(header, body, vec![receipt1, receipt2]);

        let matched = filter_block(&payload, &config);
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].tx_index, TxIndex::new(1)); // Only the second tx's log matched
        Ok(())
    }

    // ── Factory tests ────────────────────────────────────────────────

    #[test]
    fn scan_factory_events_empty_when_no_factories() {
        let header = Header::default();
        let body = BlockBody {
            transactions: vec![],
            ommers: vec![],
            withdrawals: None,
        };
        let payload = BlockPayload::new(header, body, vec![]);
        let discoveries = scan_factory_events(&payload, &[]);
        assert!(discoveries.is_empty());
    }

    #[test]
    fn scan_factory_events_finds_creation() -> eyre::Result<()> {
        // Build a minimal factory ABI for "PoolCreated(address indexed pool)"
        let abi_json = r#"[
            {"anonymous":false,"inputs":[
                {"indexed":true,"internalType":"address","name":"pool","type":"address"}
            ],"name":"PoolCreated","type":"event"}
        ]"#;
        let abi: alloy_json_abi::JsonAbi =
            serde_json::from_str(abi_json).map_err(|e| eyre::eyre!("parse: {e}"))?;
        let creation_event = abi
            .events
            .get("PoolCreated")
            .and_then(|v| v.first())
            .ok_or_else(|| eyre::eyre!("no PoolCreated event"))?;

        let factory_addr = address!("1F98431c8aD98523631AE4a59f267346ea31F984");
        let child_addr = address!("8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8");

        let factory = ResolvedFactory {
            factory_address: factory_addr,
            creation_event: creation_event.clone(),
            creation_selector: creation_event.selector(),
            child_address_param: "pool".to_string(),
            child_contract_name: "UniswapV3Pool".to_string(),
            start_block: 12_369_621,
        };

        // Build a log that matches the factory event
        let child_topic = B256::left_padding_from(child_addr.as_slice());
        let log = make_log(
            factory_addr,
            vec![creation_event.selector(), child_topic],
            Bytes::new(),
        );
        let receipt = make_receipt(vec![log]);

        let header = Header {
            number: 12_369_700,
            ..Default::default()
        };
        let body = BlockBody {
            transactions: vec![],
            ommers: vec![],
            withdrawals: None,
        };
        let payload = BlockPayload::new(header, body, vec![receipt]);

        let discoveries = scan_factory_events(&payload, &[factory]);
        assert_eq!(discoveries.len(), 1);
        assert_eq!(discoveries[0].child_address, child_addr);
        assert_eq!(discoveries[0].child_contract_name, "UniswapV3Pool");
        assert_eq!(discoveries[0].block_number, 12_369_700);
        Ok(())
    }

    // ── Bloom filter tests ──────────────────────────────────────────

    #[test]
    fn bloom_matches_address_in_header() {
        use alloy_primitives::Bloom;

        let addr = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");

        // Build a bloom that contains this address
        let mut bloom = Bloom::default();
        bloom.accrue(BloomInput::Raw(addr.as_slice()));

        let header = Header {
            logs_bloom: bloom,
            ..Default::default()
        };

        let filter = BloomFilter::new(vec![addr]);
        assert!(filter.header_may_match(&header));
    }

    #[test]
    fn bloom_rejects_missing_address() {
        let addr = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let other = address!("dAC17F958D2ee523a2206206994597C13D831ec7");

        // Build a bloom with a different address
        let mut bloom = alloy_primitives::Bloom::default();
        bloom.accrue(BloomInput::Raw(other.as_slice()));

        let header = Header {
            logs_bloom: bloom,
            ..Default::default()
        };

        let filter = BloomFilter::new(vec![addr]);
        assert!(!filter.header_may_match(&header));
    }

    #[test]
    fn bloom_rejects_empty_bloom() {
        let addr = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let header = Header::default(); // empty bloom

        let filter = BloomFilter::new(vec![addr]);
        assert!(!filter.header_may_match(&header));
    }

    #[test]
    fn bloom_matches_any_of_multiple_addresses() {
        let addr1 = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let addr2 = address!("dAC17F958D2ee523a2206206994597C13D831ec7");

        // Bloom only contains addr2
        let mut bloom = alloy_primitives::Bloom::default();
        bloom.accrue(BloomInput::Raw(addr2.as_slice()));

        let header = Header {
            logs_bloom: bloom,
            ..Default::default()
        };

        let filter = BloomFilter::new(vec![addr1, addr2]);
        assert!(filter.header_may_match(&header));
    }
}
