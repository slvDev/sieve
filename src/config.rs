//! Index configuration: which contracts and events to index.
//!
//! Users specify contract addresses, ABIs, and event names. At startup,
//! we parse the ABI, compute event selectors (topic0), and build a fast
//! address→contract lookup for use during filtering.

use alloy_json_abi::{Event, JsonAbi};
use alloy_primitives::{address, Address, B256};
use std::collections::HashMap;

/// Configuration for a single contract to index.
pub struct ContractConfig {
    /// Human-readable name (e.g. "USDC").
    pub name: String,
    /// On-chain contract address.
    pub address: Address,
    /// Full parsed ABI.
    pub abi: JsonAbi,
    /// Events to index, keyed by selector (topic0).
    pub events: HashMap<B256, Event>,
    /// Block number to start indexing from.
    pub start_block: u64,
}

impl ContractConfig {
    /// Create a new contract config by parsing an ABI JSON string and
    /// looking up the specified event names.
    ///
    /// # Errors
    ///
    /// Returns an error if the ABI JSON is invalid or an event name is
    /// not found in the ABI.
    pub fn new(
        name: impl Into<String>,
        address: Address,
        abi_json: &str,
        event_names: &[&str],
        start_block: u64,
    ) -> eyre::Result<Self> {
        let abi: JsonAbi = serde_json::from_str(abi_json)?;
        let mut events = HashMap::with_capacity(event_names.len());

        for &event_name in event_names {
            let event_list = abi
                .events
                .get(event_name)
                .ok_or_else(|| eyre::eyre!("event '{event_name}' not found in ABI"))?;
            let event = event_list
                .first()
                .ok_or_else(|| eyre::eyre!("no variants for event '{event_name}'"))?;
            let selector = event.selector();
            events.insert(selector, event.clone());
        }

        Ok(Self {
            name: name.into(),
            address,
            abi,
            events,
            start_block,
        })
    }
}

/// Top-level index configuration holding all contracts to watch.
pub struct IndexConfig {
    /// All contract configurations.
    pub contracts: Vec<ContractConfig>,
    /// Fast lookup: address → index into `contracts`.
    address_to_contract: HashMap<Address, usize>,
}

impl IndexConfig {
    /// Build an index config from a list of contract configs.
    #[must_use]
    pub fn new(contracts: Vec<ContractConfig>) -> Self {
        let mut address_to_contract = HashMap::with_capacity(contracts.len());
        for (idx, contract) in contracts.iter().enumerate() {
            address_to_contract.insert(contract.address, idx);
        }
        Self {
            contracts,
            address_to_contract,
        }
    }

    /// Look up the contract config for a given address. O(1).
    #[must_use]
    pub fn contract_for_address(&self, address: &Address) -> Option<&ContractConfig> {
        self.address_to_contract
            .get(address)
            .map(|&idx| &self.contracts[idx])
    }
}

/// Hardcoded USDC Transfer config for testing/demo.
///
/// # Errors
///
/// Returns an error if the ABI JSON fails to parse (should not happen).
pub fn usdc_transfer_config() -> eyre::Result<IndexConfig> {
    let abi_json = r#"[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]"#;

    let contract = ContractConfig::new(
        "USDC",
        address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
        abi_json,
        &["Transfer"],
        0,
    )?;

    Ok(IndexConfig::new(vec![contract]))
}

#[cfg(test)]
#[expect(clippy::panic_in_result_fn, reason = "assertions in tests are idiomatic")]
mod tests {
    use super::*;

    #[test]
    fn transfer_selector_matches_expected() -> eyre::Result<()> {
        let config = usdc_transfer_config()?;
        let contract = &config.contracts[0];

        // keccak256("Transfer(address,address,uint256)")
        let expected: B256 =
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                .parse()
                .map_err(|e| eyre::eyre!("parse error: {e}"))?;

        assert!(contract.events.contains_key(&expected));
        assert_eq!(contract.events.len(), 1);
        Ok(())
    }

    #[test]
    fn lookup_correct_address() -> eyre::Result<()> {
        let config = usdc_transfer_config()?;
        let usdc = address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");

        let found = config.contract_for_address(&usdc);
        assert!(found.is_some());
        assert_eq!(found.map(|c| c.name.as_str()), Some("USDC"));
        Ok(())
    }

    #[test]
    fn lookup_wrong_address_returns_none() -> eyre::Result<()> {
        let config = usdc_transfer_config()?;
        let wrong = address!("0000000000000000000000000000000000000001");

        assert!(config.contract_for_address(&wrong).is_none());
        Ok(())
    }
}
