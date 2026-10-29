//! ABI decoder — decodes matched logs into typed event data.
//!
//! Uses `alloy-dyn-abi` `EventExt` trait to decode log topics and data
//! at runtime, based on the JSON ABI loaded in [`crate::config`].
//!
//! Output: [`DecodedEvent`] with named parameters, block context, and tx hash.

use crate::config::{param_name, ContractConfig};
use crate::filter::FilteredLog;
use crate::types::{BlockNumber, LogIndex, TxIndex};
use alloy_dyn_abi::{DynSolValue, EventExt};
use alloy_json_abi::Param;
use alloy_primitives::{hex, Address, B256};
use serde_json::Value;
use std::fmt;

/// A single decoded parameter from an event log or call.
#[derive(Debug)]
pub struct DecodedParam {
    /// ABI input name, or `param_<position>` when unnamed.
    pub name: String,
    /// Solidity type string (e.g. "address", "uint256").
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "diagnostic metadata; only read in tests")
    )]
    pub solidity_type: String,
    /// Decoded value.
    pub value: DynSolValue,
}

/// A fully decoded event with named parameters and block context.
#[derive(Debug)]
pub struct DecodedEvent {
    /// Event name from the ABI (e.g. "Transfer"); a label, not an identity.
    pub event_name: String,
    /// Selector (topic0) of the ABI event that decoded this log.
    pub topic0: B256,
    /// Contract name (e.g. "USDC").
    pub contract_name: String,
    /// Decoded indexed parameters (in ABI order).
    pub indexed: Vec<DecodedParam>,
    /// Decoded non-indexed (body) parameters (in ABI order).
    pub body: Vec<DecodedParam>,
    /// Block number where the event was emitted.
    pub block_number: BlockNumber,
    /// Block timestamp (seconds since epoch).
    pub block_timestamp: u64,
    /// Transaction hash that produced the event.
    pub tx_hash: B256,
    /// Transaction index within the block.
    pub tx_index: TxIndex,
    /// Log index within the transaction's receipt.
    pub log_index: LogIndex,
    /// Contract address that emitted this event.
    pub contract_address: Address,
}

// Compile-time size assertion for hot type (reth pattern).
#[cfg(target_pointer_width = "64")]
const _: [(); 208] = [(); core::mem::size_of::<DecodedEvent>()];

impl fmt::Display for DecodedEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}(", self.contract_name, self.event_name)?;
        let mut first = true;
        for param in self.indexed.iter().chain(self.body.iter()) {
            if !first {
                write!(f, ", ")?;
            }
            first = false;
            write!(f, "{}={:?}", param.name, param.value)?;
        }
        write!(
            f,
            ") block={} ts={} tx={} log={}",
            self.block_number.as_u64(),
            self.block_timestamp,
            self.tx_index,
            self.log_index
        )
    }
}

/// Decode a filtered log into a [`DecodedEvent`] using the contract's ABI.
///
/// # Errors
///
/// Returns an error if the log's topic0 doesn't match any configured event,
/// or if the log data cannot be decoded against the ABI.
pub fn decode_log(log: &FilteredLog, contract: &ContractConfig) -> eyre::Result<DecodedEvent> {
    let topics = log.log.data.topics();
    let topic0 = topics
        .first()
        .ok_or_else(|| eyre::eyre!("log has no topics"))?;

    let event_abi = contract
        .events
        .get(topic0)
        .ok_or_else(|| eyre::eyre!("no event ABI for selector {topic0}"))?;

    let decoded = event_abi.decode_log(&log.log.data)?;

    let mut indexed_values = decoded.indexed.into_iter();
    let mut body_values = decoded.body.into_iter();
    let mut indexed = Vec::with_capacity(indexed_values.len());
    let mut body = Vec::with_capacity(body_values.len());
    for (position, input) in event_abi.inputs.iter().enumerate() {
        let (values, params) = if input.indexed {
            (&mut indexed_values, &mut indexed)
        } else {
            (&mut body_values, &mut body)
        };
        let value = values
            .next()
            .ok_or_else(|| eyre::eyre!("decoded log has no value for input {position}"))?;
        params.push(DecodedParam {
            name: param_name(&input.name, position).into_owned(),
            solidity_type: input.ty.clone(),
            value,
        });
    }

    Ok(DecodedEvent {
        event_name: event_abi.name.clone(),
        topic0: *topic0,
        contract_name: contract.name.clone(),
        indexed,
        body,
        block_number: log.block_number,
        block_timestamp: log.block_timestamp,
        tx_hash: log.tx_hash,
        tx_index: log.tx_index,
        log_index: log.log_index,
        contract_address: log.log.address,
    })
}

/// JSON form of a decoded value, as stored in JSON columns and sent to streams.
///
/// Arrays become JSON arrays; tuples become objects keyed by component name
/// (`param_<i>` when unnamed), which `components` supplies at every level.
/// Integers are decimal strings, addresses checksummed, bytes 0x-hex.
///
/// # Errors
///
/// Returns an error if a tuple's arity differs from its ABI components.
pub fn value_to_json(value: &DynSolValue, components: &[Param]) -> eyre::Result<Value> {
    Ok(match value {
        DynSolValue::Bool(b) => Value::Bool(*b),
        DynSolValue::Int(n, _) => Value::String(n.to_string()),
        DynSolValue::Uint(n, _) => Value::String(n.to_string()),
        DynSolValue::FixedBytes(word, size) => Value::String(hex::encode_prefixed(&word[..*size])),
        DynSolValue::Address(addr) => Value::String(addr.to_checksum(None)),
        DynSolValue::Function(function) => Value::String(hex::encode_prefixed(function)),
        DynSolValue::Bytes(bytes) => Value::String(hex::encode_prefixed(bytes)),
        DynSolValue::String(s) => Value::String(s.clone()),
        DynSolValue::Array(items) | DynSolValue::FixedArray(items) => Value::Array(
            items
                .iter()
                .map(|item| value_to_json(item, components))
                .collect::<eyre::Result<_>>()?,
        ),
        DynSolValue::Tuple(fields) => {
            eyre::ensure!(
                fields.len() == components.len(),
                "tuple has {} fields but its ABI type has {} components",
                fields.len(),
                components.len()
            );
            let mut object = serde_json::Map::with_capacity(fields.len());
            for (position, (field, component)) in fields.iter().zip(components).enumerate() {
                object.insert(
                    param_name(&component.name, position).into_owned(),
                    value_to_json(field, &component.components)?,
                );
            }
            Value::Object(object)
        }
    })
}

/// Stream `data` object for decoded params paired with their ABI components.
///
/// # Errors
///
/// Returns an error if a value does not match its ABI components.
pub fn params_to_json<'a>(
    params: impl Iterator<Item = (&'a DecodedParam, &'a [Param])>,
) -> eyre::Result<serde_json::Map<String, Value>> {
    params
        .map(|(param, components)| {
            Ok((param.name.clone(), value_to_json(&param.value, components)?))
        })
        .collect()
}

#[cfg(test)]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions in tests are idiomatic"
)]
pub mod tests {
    use super::*;
    use crate::config::usdc_transfer_config;
    use crate::types::{BlockNumber, LogIndex, TxIndex};
    use alloy_dyn_abi::DynSolValue as V;
    use alloy_primitives::{address, keccak256, Bytes, Log, LogData, I256, U256};
    use eyre::WrapErr;
    use serde_json::json;

    fn components(json: &str) -> eyre::Result<Vec<Param>> {
        Ok(serde_json::from_str(json)?)
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "one literal case per value shape")]
    fn value_to_json_cases() -> eyre::Result<()> {
        let addr = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let uint = |n: u64, bits| V::Uint(U256::from(n), bits);
        let allocation = components(
            r#"[{"name":"gauge","type":"address"},{"name":"allocated","type":"uint128"},
                {"name":"data","type":"bytes"}]"#,
        )?;
        let unnamed = components(
            r#"[{"name":"","type":"uint8"},{"name":"amountIn","type":"bool"},
                {"name":"","type":"tuple","components":[
                    {"name":"","type":"int256"},{"name":"inner","type":"string"}]}]"#,
        )?;
        let cases: Vec<(V, &[Param], Value)> = vec![
            (V::Bool(true), &[], json!(true)),
            (uint(0, 256), &[], json!("0")),
            (
                V::Uint(U256::MAX, 256),
                &[],
                json!("115792089237316195423570985008687907853269984665640564039457584007913129639935"),
            ),
            (
                V::Uint(U256::from(u128::MAX), 128),
                &[],
                json!("340282366920938463463374607431768211455"),
            ),
            (V::Int(I256::try_from(-42i64)?, 128), &[], json!("-42")),
            (
                V::Int(I256::MIN, 256),
                &[],
                json!("-57896044618658097711785492504343953926634992332820282019728792003956564819968"),
            ),
            (
                V::Address(addr),
                &[],
                json!("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
            ),
            (V::Bytes(vec![]), &[], json!("0x")),
            (V::Bytes(vec![0xde, 0xad]), &[], json!("0xdead")),
            (
                V::FixedBytes(B256::repeat_byte(0xab), 4),
                &[],
                json!("0xabababab"),
            ),
            (V::String("hello".into()), &[], json!("hello")),
            (
                V::Function(alloy_primitives::Function::repeat_byte(0x01)),
                &[],
                json!(format!("0x{}", "01".repeat(24))),
            ),
            (V::Array(vec![]), &[], json!([])),
            (
                V::Array(vec![V::Array(vec![uint(1, 8), uint(2, 8)]), V::Array(vec![])]),
                &[],
                json!([["1", "2"], []]),
            ),
            (
                V::FixedArray(vec![
                    V::FixedBytes(B256::repeat_byte(1), 32),
                    V::FixedBytes(B256::ZERO, 32),
                ]),
                &[],
                json!([format!("0x{}", "01".repeat(32)), format!("0x{}", "00".repeat(32))]),
            ),
            (
                V::Array(vec![V::Bytes(vec![]), V::Bytes(vec![1])]),
                &[],
                json!(["0x", "0x01"]),
            ),
            (
                V::FixedArray(vec![
                    V::Tuple(vec![V::Address(addr), uint(1000, 128), V::Bytes(vec![])]),
                    V::Tuple(vec![
                        V::Address(Address::ZERO),
                        V::Uint(U256::from(u128::MAX), 128),
                        V::Bytes(vec![0xff]),
                    ]),
                ]),
                &allocation,
                json!([
                    {"gauge": addr.to_checksum(None), "allocated": "1000", "data": "0x"},
                    {
                        "gauge": Address::ZERO.to_checksum(None),
                        "allocated": "340282366920938463463374607431768211455",
                        "data": "0xff"
                    }
                ]),
            ),
            (
                V::Tuple(vec![
                    uint(7, 8),
                    V::Bool(false),
                    V::Tuple(vec![V::Int(I256::MINUS_ONE, 256), V::String("x".into())]),
                ]),
                &unnamed,
                json!({"param_0": "7", "amountIn": false, "param_2": {"param_0": "-1", "inner": "x"}}),
            ),
        ];
        for (value, components, expected) in cases {
            assert_eq!(value_to_json(&value, components)?, expected, "{value:?}");
        }

        let mismatch = V::Tuple(vec![uint(1, 8)]);
        assert!(value_to_json(&mismatch, &allocation).is_err());
        assert!(value_to_json(&V::Array(vec![mismatch]), &[]).is_err());
        Ok(())
    }

    #[test]
    fn unnamed_inputs_decode_with_positional_names() -> eyre::Result<()> {
        let abi = r#"[{"type":"event","name":"E","anonymous":false,"inputs":[
            {"name":"","type":"uint256","indexed":true},
            {"name":"","type":"address","indexed":false},
            {"name":"a","type":"uint256","indexed":true},
            {"name":"","type":"uint256[]","indexed":false}]}]"#;
        let contract = ContractConfig::new("C", Address::repeat_byte(1), abi, &["E"])?;
        let data = V::Tuple(vec![
            V::Address(Address::repeat_byte(2)),
            V::Array(vec![V::Uint(U256::from(3), 256)]),
        ])
        .abi_encode_params();
        let log = FilteredLog {
            log: crate::test_utils::make_log(
                contract.address,
                vec![
                    keccak256("E(uint256,address,uint256,uint256[])"),
                    U256::from(1).into(),
                    U256::from(2).into(),
                ],
                data.into(),
            ),
            block_number: BlockNumber::new(1),
            block_timestamp: 0,
            tx_hash: B256::ZERO,
            tx_index: TxIndex::new(0),
            log_index: LogIndex::new(0),
        };
        let decoded = decode_log(&log, &contract)?;
        let names = |params: &[DecodedParam]| -> Vec<String> {
            params.iter().map(|p| p.name.clone()).collect()
        };
        assert_eq!(names(&decoded.indexed), ["param_0", "a"]);
        assert_eq!(names(&decoded.body), ["param_1", "param_3"]);
        assert_eq!(decoded.indexed[1].value, V::Uint(U256::from(2), 256));
        Ok(())
    }

    const POOL_FACTORY: Address = address!("420DD381b31aEf6683db6B902084cB0FFECe40Da");
    const LEAF_VOTER: Address = address!("382409fad8503FbCdbB27A4211551fa03e884078");
    const ROOT_VOTER: Address = address!("5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e");
    const INDEXED_COMPLEX: Address = address!("7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e7e");
    /// Topic hashes standing in for the indexed array and tuple values.
    const VALUES_HASH: B256 = B256::repeat_byte(0xc1);
    const PAIR_HASH: B256 = B256::repeat_byte(0xc2);

    /// `tests/fixtures/structured-values`.
    #[must_use]
    pub fn structured_dir() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/structured-values")
    }

    fn allocation(gauge: u8, allocated: U256, data: &[u8]) -> V {
        V::Tuple(vec![
            V::Address(Address::repeat_byte(gauge)),
            V::Uint(allocated, 128),
            V::Bytes(data.to_vec()),
        ])
    }

    fn allocation_json(gauge: u8, allocated: &str, data: &str) -> Value {
        json!({
            "gauge": Address::repeat_byte(gauge).to_checksum(None),
            "allocated": allocated,
            "data": data,
        })
    }

    fn event_log(address: Address, signature: &str, topics: &[B256], body: Vec<V>) -> Log {
        crate::test_utils::make_log(
            address,
            std::iter::once(keccak256(signature))
                .chain(topics.iter().copied())
                .collect(),
            V::Tuple(body).abi_encode_params().into(),
        )
    }

    fn word(n: u64) -> B256 {
        U256::from(n).into()
    }

    /// One receipt's logs for the structured-values fixture, in log-index order.
    #[must_use]
    pub fn structured_logs() -> Vec<Log> {
        const GAUGES_ALLOCATED: &str = "GaugesAllocated(uint256,(address,uint128,bytes)[],uint256)";
        vec![
            event_log(
                POOL_FACTORY,
                "PoolCreated(address,address,bool,address,uint256)",
                &[
                    Address::repeat_byte(0xa1).into_word(),
                    Address::repeat_byte(0xa2).into_word(),
                    word(1),
                ],
                vec![
                    V::Address(Address::repeat_byte(0xb1)),
                    V::Uint(U256::from(17), 256),
                ],
            ),
            event_log(
                LEAF_VOTER,
                GAUGES_ALLOCATED,
                &[word(7)],
                vec![
                    V::Array(vec![allocation(0x11, U256::from(1000), &[])]),
                    V::Uint(U256::from(5), 256),
                ],
            ),
            event_log(
                LEAF_VOTER,
                GAUGES_ALLOCATED,
                &[word(8)],
                vec![
                    V::Array(vec![
                        allocation(0x22, U256::from(u128::MAX), &[0xde, 0xad, 0xbe, 0xef]),
                        allocation(0x33, U256::ZERO, &[0]),
                    ]),
                    V::Uint(U256::ZERO, 256),
                ],
            ),
            event_log(
                LEAF_VOTER,
                "EmissionsMinted(address,address[],uint128[])",
                &[Address::repeat_byte(0x44).into_word()],
                vec![
                    V::Array(vec![
                        V::Address(Address::repeat_byte(0x45)),
                        V::Address(Address::repeat_byte(0x46)),
                    ]),
                    V::Array(vec![V::Uint(U256::ZERO, 128), V::Uint(U256::from(42), 128)]),
                ],
            ),
            event_log(
                ROOT_VOTER,
                "GaugesAllocated(uint256,uint256,(address,uint128,bytes)[])",
                &[word(7), word(8453)],
                vec![V::Array(vec![])],
            ),
            event_log(
                INDEXED_COMPLEX,
                "IndexedComplex(uint256[],(address,uint128),uint256)",
                &[VALUES_HASH, PAIR_HASH],
                vec![V::Uint(U256::from(3), 256)],
            ),
        ]
    }

    /// `allocateGauges(9, [(0x5555…, 1, 0x01)])` calldata, sent to the `LeafVoter`.
    #[must_use]
    pub fn allocate_gauges_call() -> (Address, Bytes) {
        let selector = &keccak256("allocateGauges(uint256,(address,uint128,bytes)[])")[..4];
        let args = V::Tuple(vec![
            V::Uint(U256::from(9), 256),
            V::Array(vec![allocation(0x55, U256::from(1), &[1])]),
        ])
        .abi_encode_params();
        (LEAF_VOTER, [selector, &args].concat().into())
    }

    /// A stored row: table, log index (-1 for calls), and each column's JSON value.
    pub type Row = (String, i32, Vec<(String, Value)>);

    /// Rows the structured-values fixture stores, sorted by table and log index.
    #[must_use]
    #[expect(
        clippy::too_many_lines,
        reason = "literal expected rows for every table"
    )]
    pub fn structured_rows() -> Vec<Row> {
        let addr = |b: u8| json!(Address::repeat_byte(b).to_checksum(None));
        let first = json!([allocation_json(0x11, "1000", "0x")]);
        let second = json!([
            allocation_json(
                0x22,
                "340282366920938463463374607431768211455",
                "0xdeadbeef"
            ),
            allocation_json(0x33, "0", "0x00"),
        ]);
        let mut rows: Vec<Row> = [
            (
                "pool_created",
                0,
                vec![
                    ("token0", addr(0xa1)),
                    ("token1", addr(0xa2)),
                    ("stable", json!(true)),
                    ("pool", addr(0xb1)),
                    ("param_4", json!("17")),
                ],
            ),
            (
                "pool_created_explicit",
                0,
                vec![("param_4", json!("17")), ("pool", addr(0xb1))],
            ),
            (
                "pool_created_legacy",
                0,
                vec![("pool", addr(0xb1)), ("pool_count", json!("17"))],
            ),
            (
                "gauges_allocated",
                1,
                vec![
                    ("token_id", json!("7")),
                    ("allocations", first.clone()),
                    ("emissions_per_vp", json!("5")),
                ],
            ),
            (
                "gauges_allocated",
                2,
                vec![
                    ("token_id", json!("8")),
                    ("allocations", second.clone()),
                    ("emissions_per_vp", json!("0")),
                ],
            ),
            (
                "gauges_allocated_text",
                1,
                vec![("token_id", json!("7")), ("allocations", first)],
            ),
            (
                "gauges_allocated_text",
                2,
                vec![("token_id", json!("8")), ("allocations", second)],
            ),
            (
                "emissions_minted",
                3,
                vec![
                    ("gauge", addr(0x44)),
                    ("recipients", json!([addr(0x45), addr(0x46)])),
                    ("amounts", json!(["0", "42"])),
                ],
            ),
            (
                "root_gauges_allocated",
                4,
                vec![
                    ("token_id", json!("7")),
                    ("chain_id", json!("8453")),
                    ("gauges", json!([])),
                ],
            ),
            (
                "indexed_complex",
                5,
                vec![
                    ("values", json!(VALUES_HASH.to_string())),
                    ("pair", json!(PAIR_HASH.to_string())),
                    ("amount", json!("3")),
                ],
            ),
            (
                "indexed_complex_text",
                5,
                vec![
                    ("values", json!(VALUES_HASH.to_string())),
                    ("pair", json!(PAIR_HASH.to_string())),
                ],
            ),
            (
                "allocate_gauges_calls",
                -1,
                vec![
                    ("token_id", json!("9")),
                    ("gauges", json!([allocation_json(0x55, "1", "0x01")])),
                ],
            ),
        ]
        .into_iter()
        .map(|(table, log, columns)| {
            (
                table.to_owned(),
                log,
                columns
                    .into_iter()
                    .map(|(column, value)| (column.to_owned(), value))
                    .collect(),
            )
        })
        .collect();
        rows.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
        rows
    }

    /// Real Aerodrome logs through the production filter, decoder and column positions.
    #[test]
    fn structured_fixture_decodes_to_json_columns() -> eyre::Result<()> {
        let dir = structured_dir();
        let resolved = crate::toml_config::resolve_config(
            &crate::toml_config::load_config(&dir.join("sieve.toml"))?,
            &dir,
        )?;
        let payload = crate::sync::BlockPayload::<crate::chain::EthereumChain>::new(
            reth_primitives_traits::Header {
                number: 100,
                ..Default::default()
            },
            reth_ethereum_primitives::BlockBody {
                transactions: vec![crate::test_utils::build_test_transaction()],
                ommers: vec![],
                withdrawals: None,
            },
            vec![crate::test_utils::make_receipt(structured_logs())],
        );
        let mut rows: Vec<Row> = Vec::new();
        for log in crate::filter::filter_block(&payload, &resolved.index_config) {
            let contract = resolved
                .index_config
                .contract_for_address(&log.log.address)
                .ok_or_else(|| eyre::eyre!("no contract for {}", log.log.address))?;
            let event = decode_log(&log, contract)?;
            for target in resolved
                .resolved_events
                .iter()
                .filter(|e| e.contract_name == event.contract_name && e.topic0 == event.topic0)
            {
                let mut columns = Vec::new();
                for col in &target.columns {
                    let param = col
                        .source
                        .pick(&event.indexed, &event.body)
                        .ok_or_else(|| eyre::eyre!("no value for {}", col.column_name))?;
                    columns.push((
                        col.column_name.clone(),
                        value_to_json(&param.value, &col.components)?,
                    ));
                }
                rows.push((target.table_name.clone(), event.log_index.as_i32(), columns));
            }
        }
        rows.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
        let mut expected = structured_rows();
        expected.retain(|(table, _, _)| table != "allocate_gauges_calls");
        assert_eq!(rows, expected);
        Ok(())
    }

    #[test]
    fn decode_transfer_log() -> eyre::Result<()> {
        let config = usdc_transfer_config()?;
        let contract = &config.contracts[0];

        let transfer_selector: B256 =
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                .parse()
                .wrap_err("parse transfer selector")?;

        let from = address!("1111111111111111111111111111111111111111");
        let to = address!("2222222222222222222222222222222222222222");

        // Encode topics: [selector, from (left-padded), to (left-padded)]
        let from_topic = B256::left_padding_from(from.as_slice());
        let to_topic = B256::left_padding_from(to.as_slice());

        // Encode data: uint256 value = 1_000_000 (USDC has 6 decimals)
        let value = U256::from(1_000_000u64);
        let data_bytes = value.to_be_bytes::<32>();

        let log_data = LogData::new_unchecked(
            vec![transfer_selector, from_topic, to_topic],
            Bytes::copy_from_slice(&data_bytes),
        );
        let log = Log {
            address: contract.address,
            data: log_data,
        };

        let filtered = FilteredLog {
            log,
            block_number: BlockNumber::new(21_000_042),
            block_timestamp: 1_700_000_000,
            tx_hash: B256::repeat_byte(0xBB),
            tx_index: TxIndex::new(0),
            log_index: LogIndex::new(0),
        };

        let decoded = decode_log(&filtered, contract)?;

        assert_eq!(decoded.event_name, "Transfer");
        assert_eq!(decoded.contract_name, "USDC");
        assert_eq!(decoded.block_number, BlockNumber::new(21_000_042));
        assert_eq!(decoded.tx_index, TxIndex::new(0));
        assert_eq!(decoded.log_index, LogIndex::new(0));

        // Check indexed params: from, to
        assert_eq!(decoded.indexed.len(), 2);
        assert_eq!(decoded.indexed[0].name, "from");
        assert_eq!(decoded.indexed[0].solidity_type, "address");
        assert_eq!(decoded.indexed[1].name, "to");
        assert_eq!(decoded.indexed[1].solidity_type, "address");

        // Check body params: value
        assert_eq!(decoded.body.len(), 1);
        assert_eq!(decoded.body[0].name, "value");
        assert_eq!(decoded.body[0].solidity_type, "uint256");

        // Verify Display output
        let display = format!("{decoded}");
        assert!(display.contains("USDC.Transfer("));
        assert!(display.contains("block=21000042"));
        assert!(display.contains("ts=1700000000"));
        assert!(display.contains("tx=0"));
        assert!(display.contains("log=0"));

        Ok(())
    }
}
