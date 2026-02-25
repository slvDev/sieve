//! Sync engine — adapted from SHiNode.
//!
//! Reference: shinode/node/src/sync/
//!
//! Modules:
//! - scheduler: work queue management, peer assignment, AIMD batch sizing
//! - fetch: batch fetching from peers
//! - follow: head-following mode after catching up
//! - reorg: rollback handling within configured window
//!
//! Key difference from SHiNode: instead of storing raw receipts,
//! the processing pipeline passes receipts through filter → decode → handler.

pub mod fetch;
pub mod follow;
pub mod reorg;
pub mod scheduler;
