//! Prometheus metrics for the Sieve indexer.
//!
//! Central metrics struct shared via `Arc<SieveMetrics>` across sync engine,
//! follow loop, and API server. Exposes counters and gauges with `sieve_`
//! prefix, plus a readiness flag for `/ready` endpoint.

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::sync::atomic::AtomicBool;

/// Central metrics for the Sieve indexer.
pub struct SieveMetrics {
    registry: Registry,

    // Counters (monotonic)
    /// Total blocks processed through the pipeline.
    pub blocks_indexed: Counter,
    /// Total events matched by address + topic0 filter.
    pub events_matched: Counter,
    /// Total events stored to database.
    pub events_stored: Counter,

    // Gauges (point-in-time)
    /// Latest observed chain head block number.
    pub chain_head: Gauge,
    /// Latest indexed block number (checkpoint).
    pub indexed_block: Gauge,
    /// Current number of connected peers.
    pub connected_peers: Gauge,
    /// Current number of active fetch tasks.
    pub active_fetches: Gauge,
    /// Number of blocks remaining in the scheduler queue.
    pub pending_blocks: Gauge,

    /// Readiness flag — `true` when caught up to chain head.
    pub is_ready: AtomicBool,
}

impl SieveMetrics {
    /// Create a new metrics instance with all metrics registered.
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let blocks_indexed = Counter::default();
        let events_matched = Counter::default();
        let events_stored = Counter::default();
        let chain_head = Gauge::default();
        let indexed_block = Gauge::default();
        let connected_peers = Gauge::default();
        let active_fetches = Gauge::default();
        let pending_blocks = Gauge::default();

        registry.register(
            "sieve_blocks_indexed",
            "Total blocks processed",
            blocks_indexed.clone(),
        );
        registry.register(
            "sieve_events_matched",
            "Total events matched by filter",
            events_matched.clone(),
        );
        registry.register(
            "sieve_events_stored",
            "Total events stored to database",
            events_stored.clone(),
        );
        registry.register(
            "sieve_chain_head",
            "Latest observed chain head block number",
            chain_head.clone(),
        );
        registry.register(
            "sieve_indexed_block",
            "Latest indexed block number",
            indexed_block.clone(),
        );
        registry.register(
            "sieve_connected_peers",
            "Current number of connected peers",
            connected_peers.clone(),
        );
        registry.register(
            "sieve_active_fetches",
            "Current number of active fetch tasks",
            active_fetches.clone(),
        );
        registry.register(
            "sieve_pending_blocks",
            "Blocks remaining in scheduler queue",
            pending_blocks.clone(),
        );

        Self {
            registry,
            blocks_indexed,
            events_matched,
            events_stored,
            chain_head,
            indexed_block,
            connected_peers,
            active_fetches,
            pending_blocks,
            is_ready: AtomicBool::new(false),
        }
    }

    /// Encode all metrics as OpenMetrics text format.
    ///
    /// # Errors
    ///
    /// Returns an error if text encoding fails.
    pub fn encode(&self) -> Result<String, std::fmt::Error> {
        let mut buf = String::new();
        encode(&mut buf, &self.registry)?;
        Ok(buf)
    }
}

impl Default for SieveMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn encode_contains_metric_names() {
        let m = SieveMetrics::new();
        m.blocks_indexed.inc();
        m.events_matched.inc_by(5);
        m.chain_head.set(21_000_000);

        let text = m.encode().ok();
        assert!(text.is_some());
        let text = text.unwrap_or_default();
        assert!(text.contains("sieve_blocks_indexed"));
        assert!(text.contains("sieve_events_matched"));
        assert!(text.contains("sieve_chain_head"));
        assert!(text.contains("sieve_indexed_block"));
        assert!(text.contains("sieve_connected_peers"));
        assert!(text.contains("sieve_active_fetches"));
        assert!(text.contains("sieve_pending_blocks"));
    }

    #[test]
    fn is_ready_defaults_false() {
        let m = SieveMetrics::new();
        assert!(!m.is_ready.load(Ordering::Relaxed));
    }
}
