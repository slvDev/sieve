//! Sync engine.
//!
//! Core types used across the sync pipeline. Sub-modules implement
//! scheduling, fetching, head-following, and reorg handling.

use crate::config::IndexConfig;
use crate::db::Database;
use crate::handler::{CallRegistry, HandlerRegistry, TransferRegistry};
use crate::metrics::SieveMetrics;
use crate::p2p::PeerPool;
use crate::toml_config::ResolvedFactory;
use reth_ethereum_primitives::{BlockBody, Receipt};
use reth_primitives_traits::Header;
use std::sync::Arc;
use tokio::sync::watch;

pub mod engine;
pub mod fetch;
pub mod follow;
pub mod reorg;
pub mod scheduler;

pub use engine::run_sync;
pub use follow::run_follow_loop;
pub use reorg::ReorgCheck;

/// Shared context for sync operations, bundling parameters that would
/// otherwise require 7+ function arguments.
pub struct SyncContext {
    /// Peer pool for P2P block fetching.
    pub pool: Arc<PeerPool>,
    /// Event filter and ABI decode configuration.
    pub config: Arc<IndexConfig>,
    /// PostgreSQL database handle.
    pub db: Arc<Database>,
    /// Event handler registry for storing decoded events.
    pub handlers: Arc<HandlerRegistry>,
    /// Prometheus metrics for observability.
    pub metrics: Arc<SieveMetrics>,
    /// Shutdown signal receiver.
    pub stop_rx: watch::Receiver<bool>,
    /// Factory definitions for dynamic contract discovery.
    pub factories: Arc<Vec<ResolvedFactory>>,
    /// Native ETH transfer handler registry.
    pub transfer_handlers: Arc<TransferRegistry>,
    /// Function call handler registry.
    pub call_handlers: Arc<CallRegistry>,
}

/// Full payload for a block: header, body, receipts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockPayload {
    header: Header,
    body: BlockBody,
    receipts: Vec<Receipt>,
}

impl BlockPayload {
    /// Create a new block payload.
    #[must_use]
    pub const fn new(header: Header, body: BlockBody, receipts: Vec<Receipt>) -> Self {
        Self { header, body, receipts }
    }

    /// Block header.
    #[must_use]
    pub const fn header(&self) -> &Header {
        &self.header
    }

    /// Block body (transactions, ommers, withdrawals).
    #[must_use]
    pub const fn body(&self) -> &BlockBody {
        &self.body
    }

    /// Transaction receipts (one per transaction, in order).
    #[must_use]
    pub fn receipts(&self) -> &[Receipt] {
        &self.receipts
    }
}

/// Fetch scheduling mode for a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchMode {
    /// Normal batch selection from pending queue.
    Normal,
    /// Escalation batch selection (priority retry across peers).
    Escalation,
}

/// A batch of blocks assigned to a peer.
#[derive(Debug, Clone)]
pub struct FetchBatch {
    pub blocks: Vec<u64>,
    pub mode: FetchMode,
}

// Compile-time size assertions for hot types (reth pattern).
#[cfg(target_pointer_width = "64")]
const _: [(); 816] = [(); core::mem::size_of::<BlockPayload>()];
#[cfg(target_pointer_width = "64")]
const _: [(); 32] = [(); core::mem::size_of::<FetchBatch>()];
#[cfg(target_pointer_width = "64")]
const _: [(); 80] = [(); core::mem::size_of::<SyncContext>()];
