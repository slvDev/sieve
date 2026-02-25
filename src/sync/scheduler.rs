//! Work queue management — adapted from SHiNode.
//!
//! Reference: shinode/node/src/sync/historical/scheduler.rs
//!
//! - Manages block ranges to fetch
//! - Assigns work to peers
//! - AIMD batch sizing (additive increase, multiplicative decrease)
//! - Quality scoring for peers
