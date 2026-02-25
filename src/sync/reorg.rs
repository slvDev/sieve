//! Reorg handling — adapted from SHiNode.
//!
//! Reference: shinode/node/src/sync/historical/reorg.rs
//!
//! On chain reorg:
//! 1. Detect reorg (new block at existing height with different hash)
//! 2. Roll back indexed data: DELETE FROM events WHERE block_number > reorg_point
//! 3. Re-index from the reorg point
