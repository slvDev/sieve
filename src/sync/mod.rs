//! Sync engine.
//!
//! Core types used across the sync pipeline. Sub-modules implement
//! scheduling, fetching, head-following, and reorg handling.

use crate::chain::ChainTypes;
use crate::config::IndexConfig;
use crate::db::Database;
use crate::filter::BloomFilter;
use crate::handler::{CallRegistry, HandlerRegistry, TransferRegistry};
use crate::metrics::SieveMetrics;
use crate::p2p::PeerPool;
use crate::stream::StreamDispatcher;
use crate::toml_config::ResolvedFactory;
use alloy_consensus::BlockBody;
use reth_primitives_traits::Header;
use std::collections::{HashMap, HashSet};
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

/// Fatal error: the in-memory factory-child state could not be restored
/// after a rejected batch.
///
/// While this error is live, the shared [`IndexConfig`] may still contain
/// speculative (unvalidated) factory children. Retrying sync in this state
/// could index events against attacker-supplied addresses or miss events
/// from committed children — callers must propagate instead of retrying.
#[derive(Debug)]
pub struct FactoryStateError {
    message: String,
}

impl FactoryStateError {
    /// Build an [`eyre::Report`] with this error as the outermost type so
    /// callers can `downcast_ref` it to detect the non-retriable case.
    pub fn report(sync_err: &eyre::Report, rebuild_err: &eyre::Report) -> eyre::Report {
        eyre::Report::new(Self {
            message: format!(
                "factory-children rebuild failed after rejected batch \
                 (in-memory state may be stale): {rebuild_err:?}; \
                 original sync error: {sync_err:?}"
            ),
        })
    }
}

impl std::fmt::Display for FactoryStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for FactoryStateError {}

/// Shared context for sync operations, bundling parameters that would
/// otherwise require 7+ function arguments.
pub struct SyncContext<C: ChainTypes> {
    /// Peer pool for P2P block fetching.
    pub pool: Arc<PeerPool<C>>,
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
    /// Optional stream notification dispatcher.
    pub stream_dispatcher: Option<Arc<StreamDispatcher>>,
    /// Maps (contract_name, event_name) → table_name for notification payloads.
    pub event_table_map: Arc<HashMap<String, (String, String)>>,
    /// Whether this sync run is a historical backfill.
    pub is_backfill: bool,
    /// Table names that have `include_receipts = true` (for streaming enrichment).
    pub receipt_tables: Arc<HashSet<String>>,
    /// Bloom filter for skipping blocks with no matching contract addresses.
    /// `None` when transfers, calls, or factories are configured.
    pub bloom_filter: Option<Arc<BloomFilter>>,
    /// Global observed head from follow-mode head tracker (overrides per-peer head_cap).
    pub head_seen_rx: Option<watch::Receiver<u64>>,
    /// Whether to use verbose tracing output (vs pretty UI).
    pub verbose: bool,
}

/// Full payload for a block: header, body, receipts.
#[derive(Debug, Clone)]
pub struct BlockPayload<C: ChainTypes> {
    header: Header,
    body: BlockBody<C::SignedTx>,
    receipts: Vec<C::Receipt>,
}

impl<C: ChainTypes> BlockPayload<C> {
    /// Create a new block payload.
    #[must_use]
    pub const fn new(
        header: Header,
        body: BlockBody<C::SignedTx>,
        receipts: Vec<C::Receipt>,
    ) -> Self {
        Self {
            header,
            body,
            receipts,
        }
    }

    /// Block header.
    #[must_use]
    pub const fn header(&self) -> &Header {
        &self.header
    }

    /// Block body (transactions, ommers, withdrawals).
    #[must_use]
    pub const fn body(&self) -> &BlockBody<C::SignedTx> {
        &self.body
    }

    /// Transaction receipts (one per transaction, in order).
    #[must_use]
    pub fn receipts(&self) -> &[C::Receipt] {
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
const _: [(); 816] = [(); core::mem::size_of::<BlockPayload<crate::chain::EthereumChain>>()];
#[cfg(target_pointer_width = "64")]
const _: [(); 32] = [(); core::mem::size_of::<FetchBatch>()];
// SyncContext size varies with stream fields — skip assertion.
