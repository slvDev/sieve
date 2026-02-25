//! Sync engine — adapted from SHiNode.
//!
//! Core types used across the sync pipeline. Sub-modules implement
//! scheduling, fetching, head-following, and reorg handling.

use reth_ethereum_primitives::{BlockBody, Receipt};
use reth_primitives_traits::Header;

pub mod fetch;
pub mod follow;
pub mod reorg;
pub mod scheduler;

/// Full payload for a block: header, body, receipts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockPayload {
    pub header: Header,
    pub body: BlockBody,
    pub receipts: Vec<Receipt>,
}

/// High-level sync state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncStatus {
    LookingForPeers,
    Fetching,
    Finalizing,
    UpToDate,
    Following,
}

impl SyncStatus {
    /// Machine-readable status string for logging and metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LookingForPeers => "looking_for_peers",
            Self::Fetching => "fetching",
            Self::Finalizing => "finalizing",
            Self::UpToDate => "up_to_date",
            Self::Following => "following",
        }
    }
}
