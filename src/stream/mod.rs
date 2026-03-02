//! Streaming notifications — post-commit block notifications to external sinks.
//!
//! The [`StreamDispatcher`] runs a background task that receives
//! [`BlockNotification`]s via a bounded channel and delivers them to all
//! configured [`StreamSink`] implementations. Delivery is best-effort:
//! failures are logged but never propagate.

pub mod webhook;

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

/// Trait for notification sinks (webhook, future: Kafka, AMQP, etc.).
#[async_trait]
pub trait StreamSink: Send + Sync {
    /// Human-readable name for logging.
    fn name(&self) -> &str;

    /// Deliver a block notification. Implementations must not propagate errors.
    async fn notify(&self, notification: &BlockNotification);
}

/// Message sent through the dispatcher channel.
struct DispatchMessage {
    notification: BlockNotification,
    is_backfill: bool,
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

    /// Send a notification to the dispatcher. Drops silently if the channel is full.
    pub fn send(&self, notification: BlockNotification, is_backfill: bool) {
        if let Err(e) = self.tx.try_send(DispatchMessage {
            notification,
            is_backfill,
        }) {
            warn!(error = %e, "stream dispatcher channel full, dropping notification");
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
        for entry in &entries {
            // Skip sinks that opted out of backfill notifications
            if msg.is_backfill && !entry.backfill {
                debug!(
                    sink = entry.sink.name(),
                    block = msg.notification.block_number,
                    "skipping backfill notification"
                );
                continue;
            }
            entry.sink.notify(&msg.notification).await;
        }
    }

    debug!("stream delivery loop exiting");
}

#[cfg(test)]
#[expect(clippy::panic_in_result_fn, reason = "assertions in tests are idiomatic")]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    struct MockSink {
        name: String,
        call_count: Arc<AtomicU64>,
    }

    #[async_trait]
    impl StreamSink for MockSink {
        fn name(&self) -> &str {
            &self.name
        }

        async fn notify(&self, _notification: &BlockNotification) {
            self.call_count.fetch_add(1, Ordering::Relaxed);
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

    #[tokio::test]
    async fn dispatcher_delivers_to_sink() {
        let count = Arc::new(AtomicU64::new(0));
        let sink: Box<dyn StreamSink> = Box::new(MockSink {
            name: "test".to_string(),
            call_count: Arc::clone(&count),
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
            call_count: Arc::clone(&count),
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
            call_count: Arc::clone(&count),
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
}
