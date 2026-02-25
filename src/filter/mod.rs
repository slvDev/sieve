//! Event filter — matches raw logs against user-defined contract/event criteria.
//!
//! For each receipt in a fetched block:
//! 1. Check if any log's address matches a configured contract
//! 2. Check if topic0 matches a configured event signature
//! 3. If both match, pass the log to the decoder
//!
//! This runs at sync time, discarding non-matching data immediately.
