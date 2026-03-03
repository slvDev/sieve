//! Streaming notifications — post-commit block and event notifications to external sinks.
//!
//! The [`StreamDispatcher`] runs a background task that receives
//! [`BlockNotification`]s and [`EventPayload`]s via a bounded channel and
//! delivers them to all configured [`StreamSink`] implementations. Delivery
//! is best-effort: failures are logged but never propagate.

pub mod rabbitmq;
pub mod webhook;

use alloy_dyn_abi::DynSolValue;
use alloy_primitives::Address;
use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// Notification payload sent after a block is committed to the database.
#[derive(Debug, Clone, Serialize)]
pub struct BlockNotification {
    /// Block number that was just committed.
    pub block_number: u64,
    /// Block timestamp (seconds since epoch).
    pub block_timestamp: u64,
    /// Tables that received new rows in this block.
    pub tables: Vec<TableNotification>,
}

/// Per-table insert summary within a block notification.
#[derive(Debug, Clone, Serialize)]
pub struct TableNotification {
    /// Postgres table name.
    pub name: String,
    /// Event/call/transfer name.
    pub event: String,
    /// Number of rows inserted.
    pub count: u64,
}

/// Per-event payload for streaming decoded event data to sinks.
#[derive(Debug, Clone, Serialize)]
pub struct EventPayload {
    /// Postgres table name this event is stored in.
    pub table: String,
    /// Event/function/transfer name.
    pub event: String,
    /// Contract address (checksummed hex).
    pub contract: String,
    /// Block number where the event occurred.
    pub block_number: u64,
    /// Block timestamp (seconds since epoch).
    pub block_timestamp: u64,
    /// Transaction hash (0x-prefixed hex).
    pub tx_hash: String,
    /// Log index within the receipt (None for calls/transfers).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_index: Option<u32>,
    /// Transaction index within the block.
    pub tx_index: u32,
    /// Transaction sender (checksummed hex). None for legacy event payloads.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_from: Option<String>,
    /// Decoded event parameters as key-value pairs.
    pub data: serde_json::Map<String, serde_json::Value>,
}

/// Convert a [`DynSolValue`] to a [`serde_json::Value`].
///
/// Mirrors the conversion logic in `handler::bind_dyn_value` but targets
/// JSON instead of PostgreSQL parameters.
#[must_use]
pub fn dyn_sol_to_json(value: &DynSolValue) -> serde_json::Value {
    match value {
        DynSolValue::Address(addr) => {
            serde_json::Value::String(Address::to_checksum(addr, None))
        }
        DynSolValue::Bool(b) => serde_json::Value::Bool(*b),
        DynSolValue::Uint(val, _) => serde_json::Value::String(val.to_string()),
        DynSolValue::Int(val, _) => serde_json::Value::String(val.to_string()),
        DynSolValue::String(s) => serde_json::Value::String(s.clone()),
        DynSolValue::Bytes(b) => {
            serde_json::Value::String(format!("0x{}", alloy_primitives::hex::encode(b)))
        }
        DynSolValue::FixedBytes(word, size) => {
            let bytes = &word.as_slice()[..(*size)];
            serde_json::Value::String(format!("0x{}", alloy_primitives::hex::encode(bytes)))
        }
        other => serde_json::Value::String(format!("{other:?}")),
    }
}

/// Trait for notification sinks (webhook, RabbitMQ, etc.).
#[async_trait]
pub trait StreamSink: Send + Sync {
    /// Human-readable name for logging.
    fn name(&self) -> &str;

    /// Deliver a block-level notification. Implementations must not propagate errors.
    async fn notify(&self, notification: &BlockNotification);

    /// Deliver per-event payloads. Default is a no-op (e.g. webhooks ignore this).
    async fn notify_events(&self, _events: &[EventPayload]) {
        // Default no-op — sinks that don't care about individual events do nothing.
    }
}

/// Message sent through the dispatcher channel.
enum DispatchMessage {
    /// Block-level summary notification (table names + row counts).
    Block {
        notification: BlockNotification,
        is_backfill: bool,
    },
    /// Per-event decoded payloads.
    Events {
        payloads: Vec<EventPayload>,
        is_backfill: bool,
    },
}

/// A sink entry: the sink itself plus its backfill preference.
struct SinkEntry {
    sink: Box<dyn StreamSink>,
    backfill: bool,
}

/// Dispatches block notifications to all configured stream sinks.
///
/// Uses a bounded mpsc channel to decouple the sync engine from network I/O.
/// A background task drains the channel and delivers to each sink.
///
/// When all clones of the dispatcher are dropped, the sender closes and the
/// background task drains remaining notifications.
pub struct StreamDispatcher {
    tx: mpsc::Sender<DispatchMessage>,
}

impl StreamDispatcher {
    /// Create a new dispatcher with the given sinks and channel capacity.
    ///
    /// Each sink is paired with a `backfill` flag: if `false`, the sink is
    /// skipped when `is_backfill` is `true` on the notification.
    #[must_use]
    pub fn new(
        sinks: Vec<(Box<dyn StreamSink>, bool)>,
        capacity: usize,
    ) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        tokio::spawn(run_delivery(rx, sinks));
        Self { tx }
    }

    /// Send a block-level notification to the dispatcher.
    /// Drops silently if the channel is full.
    pub fn send(&self, notification: BlockNotification, is_backfill: bool) {
        if let Err(e) = self.tx.try_send(DispatchMessage::Block {
            notification,
            is_backfill,
        }) {
            warn!(error = %e, "stream dispatcher channel full, dropping block notification");
        }
    }

    /// Send per-event payloads to the dispatcher.
    /// Drops silently if the channel is full.
    pub fn send_events(&self, payloads: Vec<EventPayload>, is_backfill: bool) {
        if let Err(e) = self.tx.try_send(DispatchMessage::Events {
            payloads,
            is_backfill,
        }) {
            warn!(error = %e, "stream dispatcher channel full, dropping event payloads");
        }
    }
}

/// Background delivery loop: reads from channel, fans out to sinks.
async fn run_delivery(
    mut rx: mpsc::Receiver<DispatchMessage>,
    sinks: Vec<(Box<dyn StreamSink>, bool)>,
) {
    let entries: Vec<SinkEntry> = sinks
        .into_iter()
        .map(|(sink, backfill)| SinkEntry { sink, backfill })
        .collect();

    while let Some(msg) = rx.recv().await {
        match msg {
            DispatchMessage::Block {
                notification,
                is_backfill,
            } => {
                deliver_block(&entries, &notification, is_backfill).await;
            }
            DispatchMessage::Events {
                payloads,
                is_backfill,
            } => {
                deliver_events(&entries, &payloads, is_backfill).await;
            }
        }
    }

    debug!("stream delivery loop exiting");
}

/// Deliver a block notification to all eligible sinks.
async fn deliver_block(entries: &[SinkEntry], notification: &BlockNotification, is_backfill: bool) {
    for entry in entries {
        if is_backfill && !entry.backfill {
            debug!(
                sink = entry.sink.name(),
                block = notification.block_number,
                "skipping backfill block notification"
            );
            continue;
        }
        entry.sink.notify(notification).await;
    }
}

/// Deliver event payloads to all eligible sinks.
async fn deliver_events(entries: &[SinkEntry], payloads: &[EventPayload], is_backfill: bool) {
    for entry in entries {
        if is_backfill && !entry.backfill {
            debug!(
                sink = entry.sink.name(),
                events = payloads.len(),
                "skipping backfill event payloads"
            );
            continue;
        }
        entry.sink.notify_events(payloads).await;
    }
}

#[cfg(test)]
#[expect(clippy::panic_in_result_fn, reason = "assertions in tests are idiomatic")]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    struct MockSink {
        name: String,
        block_count: Arc<AtomicU64>,
        event_payloads: Arc<Mutex<Vec<EventPayload>>>,
    }

    #[async_trait]
    impl StreamSink for MockSink {
        fn name(&self) -> &str {
            &self.name
        }

        async fn notify(&self, _notification: &BlockNotification) {
            self.block_count.fetch_add(1, Ordering::Relaxed);
        }

        async fn notify_events(&self, events: &[EventPayload]) {
            let mut payloads = self.event_payloads.lock().await;
            payloads.extend(events.iter().cloned());
        }
    }

    struct SlowSink {
        call_count: Arc<AtomicU64>,
    }

    #[async_trait]
    impl StreamSink for SlowSink {
        #[expect(clippy::unnecessary_literal_bound, reason = "trait requires &str")]
        fn name(&self) -> &str {
            "slow"
        }

        async fn notify(&self, _notification: &BlockNotification) {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            self.call_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn block_notification_serializes_to_json() -> eyre::Result<()> {
        let notif = BlockNotification {
            block_number: 22_516_100,
            block_timestamp: 1_700_000_000,
            tables: vec![
                TableNotification {
                    name: "trove_updated_weth".to_string(),
                    event: "TroveUpdated".to_string(),
                    count: 3,
                },
                TableNotification {
                    name: "trove_operations_weth".to_string(),
                    event: "TroveOperation".to_string(),
                    count: 1,
                },
            ],
        };

        let json = serde_json::to_string(&notif)?;
        assert!(json.contains("\"block_number\":22516100"));
        assert!(json.contains("\"block_timestamp\":1700000000"));
        assert!(json.contains("\"trove_updated_weth\""));
        assert!(json.contains("\"count\":3"));
        Ok(())
    }

    #[test]
    fn event_payload_serializes_to_json() -> eyre::Result<()> {
        let mut data = serde_json::Map::new();
        data.insert("from".to_string(), serde_json::Value::String("0xABC".to_string()));
        data.insert("value".to_string(), serde_json::Value::String("1000000".to_string()));

        let payload = EventPayload {
            table: "usdc_transfers".to_string(),
            event: "Transfer".to_string(),
            contract: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_string(),
            block_number: 22_000_000,
            block_timestamp: 1_700_000_000,
            tx_hash: "0xabc123".to_string(),
            log_index: Some(5),
            tx_index: 3,
            tx_from: Some("0xDEAD".to_string()),
            data,
        };

        let json = serde_json::to_string(&payload)?;
        assert!(json.contains("\"table\":\"usdc_transfers\""));
        assert!(json.contains("\"tx_index\":3"));
        assert!(json.contains("\"event\":\"Transfer\""));
        assert!(json.contains("\"block_number\":22000000"));
        assert!(json.contains("\"log_index\":5"));
        assert!(json.contains("\"from\":\"0xABC\""));
        assert!(json.contains("\"tx_from\":\"0xDEAD\""));
        Ok(())
    }

    #[test]
    fn event_payload_omits_null_log_index() -> eyre::Result<()> {
        let payload = EventPayload {
            table: "eth_transfers".to_string(),
            event: "transfer".to_string(),
            contract: "0x0000000000000000000000000000000000000000".to_string(),
            block_number: 1,
            block_timestamp: 100,
            tx_hash: "0x00".to_string(),
            log_index: None,
            tx_index: 0,
            tx_from: None,
            data: serde_json::Map::new(),
        };

        let json = serde_json::to_string(&payload)?;
        assert!(!json.contains("log_index"));
        assert!(!json.contains("tx_from"));
        Ok(())
    }

    #[test]
    fn dyn_sol_to_json_address() {
        let addr = alloy_primitives::address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
        let val = dyn_sol_to_json(&DynSolValue::Address(addr));
        assert_eq!(val, serde_json::Value::String("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".to_string()));
    }

    #[test]
    fn dyn_sol_to_json_uint() {
        let val = dyn_sol_to_json(&DynSolValue::Uint(
            alloy_primitives::U256::from(1_000_000u64),
            256,
        ));
        assert_eq!(val, serde_json::Value::String("1000000".to_string()));
    }

    #[test]
    fn dyn_sol_to_json_bool() {
        let val = dyn_sol_to_json(&DynSolValue::Bool(true));
        assert_eq!(val, serde_json::Value::Bool(true));
    }

    #[test]
    fn dyn_sol_to_json_bytes() {
        let val = dyn_sol_to_json(&DynSolValue::Bytes(vec![0xde, 0xad, 0xbe, 0xef]));
        assert_eq!(val, serde_json::Value::String("0xdeadbeef".to_string()));
    }

    #[test]
    fn dyn_sol_to_json_string() {
        let val = dyn_sol_to_json(&DynSolValue::String("hello".to_string()));
        assert_eq!(val, serde_json::Value::String("hello".to_string()));
    }

    #[test]
    fn dyn_sol_to_json_fixed_bytes() {
        let word = alloy_primitives::B256::from_slice(&[0xab; 32]);
        let val = dyn_sol_to_json(&DynSolValue::FixedBytes(word, 4));
        assert_eq!(val, serde_json::Value::String("0xabababab".to_string()));
    }

    #[tokio::test]
    async fn dispatcher_delivers_to_sink() {
        let count = Arc::new(AtomicU64::new(0));
        let sink: Box<dyn StreamSink> = Box::new(MockSink {
            name: "test".to_string(),
            block_count: Arc::clone(&count),
            event_payloads: Arc::new(Mutex::new(vec![])),
        });

        let dispatcher = StreamDispatcher::new(vec![(sink, true)], 16);

        dispatcher.send(
            BlockNotification {
                block_number: 1,
                block_timestamp: 100,
                tables: vec![],
            },
            false,
        );

        // Give background task time to process
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(count.load(Ordering::Relaxed), 1);

        drop(dispatcher);
    }

    #[tokio::test]
    async fn dispatcher_skips_backfill_when_opted_out() {
        let count = Arc::new(AtomicU64::new(0));
        let sink: Box<dyn StreamSink> = Box::new(MockSink {
            name: "no_backfill".to_string(),
            block_count: Arc::clone(&count),
            event_payloads: Arc::new(Mutex::new(vec![])),
        });

        // backfill=false means skip during backfill
        let dispatcher = StreamDispatcher::new(vec![(sink, false)], 16);

        // Send as backfill — should be skipped
        dispatcher.send(
            BlockNotification {
                block_number: 1,
                block_timestamp: 100,
                tables: vec![],
            },
            true,
        );

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(count.load(Ordering::Relaxed), 0);

        // Send as non-backfill — should be delivered
        dispatcher.send(
            BlockNotification {
                block_number: 2,
                block_timestamp: 200,
                tables: vec![],
            },
            false,
        );

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(count.load(Ordering::Relaxed), 1);

        drop(dispatcher);
    }

    #[tokio::test]
    async fn dispatcher_channel_full_does_not_panic() {
        let count = Arc::new(AtomicU64::new(0));

        let sink: Box<dyn StreamSink> = Box::new(SlowSink {
            call_count: Arc::clone(&count),
        });

        // Capacity of 1 — second send should drop
        let dispatcher = StreamDispatcher::new(vec![(sink, true)], 1);

        let notif = BlockNotification {
            block_number: 1,
            block_timestamp: 100,
            tables: vec![],
        };

        // Fill the channel
        dispatcher.send(notif.clone(), false);
        dispatcher.send(notif.clone(), false);
        dispatcher.send(notif, false);

        // Should not panic — dropped notifications are logged
        drop(dispatcher);
    }

    #[tokio::test]
    async fn dispatcher_delivers_all_queued() {
        let count = Arc::new(AtomicU64::new(0));
        let sink: Box<dyn StreamSink> = Box::new(MockSink {
            name: "drain_test".to_string(),
            block_count: Arc::clone(&count),
            event_payloads: Arc::new(Mutex::new(vec![])),
        });

        let dispatcher = StreamDispatcher::new(vec![(sink, true)], 64);

        for i in 0..5 {
            dispatcher.send(
                BlockNotification {
                    block_number: i,
                    block_timestamp: 100 + i,
                    tables: vec![],
                },
                false,
            );
        }

        // Give background task time to process
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert_eq!(count.load(Ordering::Relaxed), 5);
        drop(dispatcher);
    }

    #[tokio::test]
    async fn dispatcher_delivers_events_to_sink() {
        let event_payloads = Arc::new(Mutex::new(vec![]));
        let sink: Box<dyn StreamSink> = Box::new(MockSink {
            name: "event_test".to_string(),
            block_count: Arc::new(AtomicU64::new(0)),
            event_payloads: Arc::clone(&event_payloads),
        });

        let dispatcher = StreamDispatcher::new(vec![(sink, true)], 16);

        let mut data = serde_json::Map::new();
        data.insert("from".to_string(), serde_json::Value::String("0xABC".to_string()));

        dispatcher.send_events(
            vec![EventPayload {
                table: "usdc_transfers".to_string(),
                event: "Transfer".to_string(),
                contract: "0xA0b8".to_string(),
                block_number: 100,
                block_timestamp: 1000,
                tx_hash: "0xabc".to_string(),
                log_index: Some(0),
                tx_index: 1,
                tx_from: None,
                data,
            }],
            false,
        );

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let received = event_payloads.lock().await;
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].table, "usdc_transfers");
        assert_eq!(received[0].event, "Transfer");
        drop(dispatcher);
    }

    #[tokio::test]
    async fn dispatcher_skips_backfill_events() {
        let event_payloads = Arc::new(Mutex::new(vec![]));
        let sink: Box<dyn StreamSink> = Box::new(MockSink {
            name: "no_backfill_events".to_string(),
            block_count: Arc::new(AtomicU64::new(0)),
            event_payloads: Arc::clone(&event_payloads),
        });

        // backfill=false
        let dispatcher = StreamDispatcher::new(vec![(sink, false)], 16);

        dispatcher.send_events(
            vec![EventPayload {
                table: "test".to_string(),
                event: "Test".to_string(),
                contract: "0x00".to_string(),
                block_number: 1,
                block_timestamp: 100,
                tx_hash: "0x00".to_string(),
                log_index: None,
                tx_index: 0,
                tx_from: None,
                data: serde_json::Map::new(),
            }],
            true, // backfill
        );

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let received = event_payloads.lock().await;
        assert!(received.is_empty());
        drop(dispatcher);
    }
}
