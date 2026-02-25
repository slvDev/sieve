//! ABI decoder — decodes matched logs into typed event data.
//!
//! Uses alloy-json-abi and alloy-sol-types to:
//! 1. Parse the ABI JSON for the contract
//! 2. Match topic0 to the event signature
//! 3. Decode indexed topics and data field into typed values
//!
//! Output: DecodedEvent { name, params: Vec<(name, value)>, block_number, tx_hash, log_index }
