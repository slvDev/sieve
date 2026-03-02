//! ABI decoder — decodes matched logs into typed event data.
//!
//! Uses `alloy-dyn-abi` `EventExt` trait to decode log topics and data
//! at runtime, based on the JSON ABI loaded in [`crate::config`].
//!
//! Output: [`DecodedEvent`] with named parameters, block context, and tx hash.

use crate::config::ContractConfig;
use crate::filter::FilteredLog;
use crate::types::BlockNumber;
use alloy_dyn_abi::{DynSolValue, EventExt};
use alloy_primitives::{Address, B256};
use std::fmt;

/// A single decoded parameter from an event log.
#[derive(Debug)]
pub struct DecodedParam {
    /// Parameter name from the ABI (e.g. "from", "to", "value").
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
    /// Event name from the ABI (e.g. "Transfer").
    pub event_name: String,
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
    pub tx_index: usize,
    /// Log index within the transaction's receipt.
    pub log_index: usize,
    /// Contract address that emitted this event.
    pub contract_address: Address,
}

// Compile-time size assertion for hot type (reth pattern).
#[cfg(target_pointer_width = "64")]
const _: [(); 184] = [(); core::mem::size_of::<DecodedEvent>()];

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
            self.block_number.as_u64(), self.block_timestamp, self.tx_index, self.log_index
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

    // Zip decoded values with ABI input names/types
    let indexed_inputs: Vec<_> = event_abi.inputs.iter().filter(|p| p.indexed).collect();
    let body_inputs: Vec<_> = event_abi.inputs.iter().filter(|p| !p.indexed).collect();

    let indexed = decoded
        .indexed
        .into_iter()
        .zip(indexed_inputs.iter())
        .map(|(value, param)| DecodedParam {
            name: param.name.clone(),
            solidity_type: param.ty.clone(),
            value,
        })
        .collect();

    let body = decoded
        .body
        .into_iter()
        .zip(body_inputs.iter())
        .map(|(value, param)| DecodedParam {
            name: param.name.clone(),
            solidity_type: param.ty.clone(),
            value,
        })
        .collect();

    Ok(DecodedEvent {
        event_name: event_abi.name.clone(),
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

#[cfg(test)]
#[expect(clippy::panic_in_result_fn, reason = "assertions in tests are idiomatic")]
mod tests {
    use super::*;
    use crate::config::usdc_transfer_config;
    use crate::types::BlockNumber;
    use alloy_primitives::{address, Bytes, Log, LogData, U256};
    use eyre::WrapErr;

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
            tx_index: 0,
            log_index: 0,
        };

        let decoded = decode_log(&filtered, contract)?;

        assert_eq!(decoded.event_name, "Transfer");
        assert_eq!(decoded.contract_name, "USDC");
        assert_eq!(decoded.block_number, BlockNumber::new(21_000_042));
        assert_eq!(decoded.tx_index, 0);
        assert_eq!(decoded.log_index, 0);

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
