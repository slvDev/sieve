//! Index configuration: which contracts and events to index.
//!
//! Users specify contract addresses, ABIs, and event names. At startup,
//! we parse the ABI, compute event selectors (topic0), and build a fast
//! address→contract lookup for use during filtering.

use crate::toml_config::TopicFilter;
use alloy_json_abi::{Event, Function, JsonAbi};
use alloy_primitives::{Address, FixedBytes, B256};
use std::collections::HashMap;
use std::sync::RwLock;

/// 4-byte function selector.
pub type Selector = FixedBytes<4>;

/// Configuration for a single contract to index.
#[derive(Debug)]
pub struct ContractConfig {
    /// Human-readable name (e.g. "USDC").
    pub name: String,
    /// On-chain contract address.
    pub address: Address,
    /// Events to index, keyed by selector (topic0).
    pub events: HashMap<B256, Event>,
    /// Indexed parameter filters per event selector (topic0).
    /// If an event has no filters, it won't have an entry here.
    pub topic_filters: HashMap<B256, Vec<TopicFilter>>,
    /// Functions to index, keyed by 4-byte selector.
    pub functions: HashMap<Selector, Function>,
}

impl ContractConfig {
    /// Test convenience: parse an ABI and select each unique event by name.
    ///
    /// # Errors
    ///
    /// Returns an error if the ABI JSON is invalid or an event cannot be selected.
    #[cfg(test)]
    pub fn new(
        name: impl Into<String>,
        address: Address,
        abi_json: &str,
        event_names: &[&str],
    ) -> eyre::Result<Self> {
        let name = name.into();
        let abi: JsonAbi = serde_json::from_str(abi_json)?;
        let mut events = HashMap::with_capacity(event_names.len());
        for &event_name in event_names {
            let event = select_event(&abi, &name, event_name, None)?;
            events.insert(event.selector(), event.clone());
        }
        Ok(Self {
            name,
            address,
            events,
            topic_filters: HashMap::new(),
            functions: HashMap::new(),
        })
    }
}

/// Select the ABI event a config entry refers to.
///
/// With `signature`, it must equal a candidate's canonical `Event::signature()`
/// exactly. Without it, the name must have exactly one layout. Identical
/// duplicate ABI entries count as one; distinct layouts sharing a canonical
/// signature (e.g. different `indexed` flags) are rejected because topic0
/// cannot tell them apart.
///
/// # Errors
///
/// Returns an error if the name is missing, the signature matches no
/// candidate, the name is overloaded and no signature was given, or the
/// selected signature has conflicting layouts.
pub fn select_event<'a>(
    abi: &'a JsonAbi,
    contract_name: &str,
    event_name: &str,
    signature: Option<&str>,
) -> eyre::Result<&'a Event> {
    let all = abi
        .events
        .get(event_name)
        .filter(|events| !events.is_empty())
        .ok_or_else(|| {
            eyre::eyre!("event '{event_name}' not found in ABI for contract '{contract_name}'")
        })?;
    let mut distinct: Vec<&Event> = Vec::with_capacity(all.len());
    for event in all {
        if !distinct.contains(&event) {
            distinct.push(event);
        }
    }
    let mut candidates: Vec<String> = distinct.iter().map(|e| e.signature()).collect();
    candidates.sort_unstable();
    candidates.dedup();

    let wanted = match signature {
        Some(wanted) => wanted.to_owned(),
        None if candidates.len() == 1 => candidates.swap_remove(0),
        None => {
            return Err(eyre::eyre!(
                "event '{event_name}' in contract '{contract_name}' is overloaded; add \
                 `signature` with one of: {} (e.g. signature = \"{}\")",
                candidates.join(", "),
                candidates[0],
            ));
        }
    };
    let mut matching = distinct.into_iter().filter(|e| e.signature() == wanted);
    match (matching.next(), matching.next()) {
        (Some(event), None) => Ok(event),
        (Some(_), Some(_)) => Err(eyre::eyre!(
            "event '{wanted}' in contract '{contract_name}' has conflicting ABI layouts \
             (parameter names or indexed flags differ) with the same topic0; keep one in the ABI"
        )),
        (None, _) => Err(eyre::eyre!(
            "event '{event_name}' in contract '{contract_name}' has no ABI variant with \
             signature '{wanted}'; signatures are canonical (\"Name(type,...)\", no spaces or \
             parameter names). Candidates: {}",
            candidates.join(", "),
        )),
    }
}

/// Top-level index configuration holding all contracts to watch.
pub struct IndexConfig {
    /// All contract configurations.
    pub contracts: Vec<ContractConfig>,
    /// Fast lookup: address → index into `contracts`.
    address_to_contract: HashMap<Address, usize>,
    /// Dynamically registered factory children: address → index into `contracts`.
    factory_children: RwLock<HashMap<Address, usize>>,
}

impl std::fmt::Debug for IndexConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let children_count = self
            .factory_children
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        f.debug_struct("IndexConfig")
            .field("contracts", &self.contracts)
            .field("static_addresses", &self.address_to_contract.len())
            .field("factory_children", &children_count)
            .finish()
    }
}

impl IndexConfig {
    /// Build an index config from a list of contract configs.
    ///
    /// Skips `Address::ZERO` entries (factory-child placeholders) in the
    /// static address map.
    #[must_use]
    pub fn new(contracts: Vec<ContractConfig>) -> Self {
        let mut address_to_contract = HashMap::with_capacity(contracts.len());
        for (idx, contract) in contracts.iter().enumerate() {
            // Skip placeholder addresses used by factory-child contracts
            if contract.address != Address::ZERO {
                address_to_contract.insert(contract.address, idx);
            }
        }
        Self {
            contracts,
            address_to_contract,
            factory_children: RwLock::new(HashMap::new()),
        }
    }

    /// Look up the contract config for a given address. O(1).
    ///
    /// Checks static addresses first, then dynamically registered factory children.
    #[must_use]
    pub fn contract_for_address(&self, address: &Address) -> Option<&ContractConfig> {
        // Check static map first
        if let Some(&idx) = self.address_to_contract.get(address) {
            return Some(&self.contracts[idx]);
        }

        // Check dynamic factory children
        let children = self
            .factory_children
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        children.get(address).map(|&idx| &self.contracts[idx])
    }

    /// Register a factory-created child contract address.
    ///
    /// Returns `false` if the address was already registered.
    pub fn register_factory_child(&self, address: Address, contract_idx: usize) -> bool {
        let mut children = self
            .factory_children
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if children.contains_key(&address) {
            return false;
        }
        children.insert(address, contract_idx);
        true
    }

    /// Unregister a factory child address (used during reorg rollback).
    pub fn unregister_factory_child(&self, address: &Address) {
        let mut children = self
            .factory_children
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        children.remove(address);
    }

    /// Atomically replace the entire dynamic factory-children map.
    ///
    /// Used to discard speculative registrations after a rejected batch:
    /// the committed set is rebuilt off to the side and swapped in whole,
    /// so a failed rebuild never leaves partial or empty state behind.
    pub fn replace_factory_children(&self, children: HashMap<Address, usize>) {
        let mut guard = self
            .factory_children
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = children;
    }
}

/// Hardcoded USDC Transfer config for tests.
///
/// # Errors
///
/// Returns an error if the ABI JSON fails to parse.
#[cfg(test)]
pub fn usdc_transfer_config() -> eyre::Result<IndexConfig> {
    use alloy_primitives::address;

    let abi_json = r#"[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]"#;

    let contract = ContractConfig::new(
        "USDC",
        address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
        abi_json,
        &["Transfer"],
    )?;

    Ok(IndexConfig::new(vec![contract]))
}

#[cfg(test)]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions in tests are idiomatic"
)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    #[test]
    fn transfer_selector_matches_expected() -> eyre::Result<()> {
        let config = usdc_transfer_config()?;
        let contract = &config.contracts[0];

        // keccak256("Transfer(address,address,uint256)")
        let expected: B256 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
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

    #[test]
    fn register_and_lookup_factory_child() -> eyre::Result<()> {
        let config = usdc_transfer_config()?;
        let child = address!("0000000000000000000000000000000000000042");

        // Not found before registration
        assert!(config.contract_for_address(&child).is_none());

        // Register child pointing to contract index 0 (USDC)
        assert!(config.register_factory_child(child, 0));

        // Now found
        let found = config.contract_for_address(&child);
        assert!(found.is_some());
        assert_eq!(found.map(|c| c.name.as_str()), Some("USDC"));

        // Duplicate registration returns false
        assert!(!config.register_factory_child(child, 0));

        Ok(())
    }

    #[test]
    fn unregister_factory_child() -> eyre::Result<()> {
        let config = usdc_transfer_config()?;
        let child = address!("0000000000000000000000000000000000000042");

        config.register_factory_child(child, 0);
        assert!(config.contract_for_address(&child).is_some());

        config.unregister_factory_child(&child);
        assert!(config.contract_for_address(&child).is_none());

        Ok(())
    }

    #[test]
    fn replace_factory_children_swaps_whole_map() -> eyre::Result<()> {
        let config = usdc_transfer_config()?;
        let speculative = address!("0000000000000000000000000000000000000042");
        let committed = address!("0000000000000000000000000000000000000043");

        config.register_factory_child(speculative, 0);

        let mut committed_map = HashMap::new();
        committed_map.insert(committed, 0);
        config.replace_factory_children(committed_map);

        assert!(config.contract_for_address(&speculative).is_none());
        assert!(config.contract_for_address(&committed).is_some());
        Ok(())
    }

    #[test]
    fn zero_address_skipped_in_static_map() -> eyre::Result<()> {
        let abi_json = r#"[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]"#;

        let contract = ContractConfig::new("FactoryChild", Address::ZERO, abi_json, &["Transfer"])?;

        let config = IndexConfig::new(vec![contract]);

        // Address::ZERO should not be in the static map
        assert!(config.contract_for_address(&Address::ZERO).is_none());

        Ok(())
    }
}
