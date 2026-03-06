//! RabbitMQ sink — publishes per-event JSON messages to an AMQP exchange.
//!
//! Lazy-connects on first `notify_events()` call. On failure, drops the
//! channel and reconnects on the next call. Best-effort, like webhooks.

use super::{BlockNotification, EventPayload, StreamSink};
use async_trait::async_trait;
use lapin::options::{BasicPublishOptions, ExchangeDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// RabbitMQ sink that publishes decoded event payloads as JSON messages.
///
/// Each event is published to the configured exchange with a routing key
/// derived from the template (e.g. `"{table}.{event}"`).
pub struct RabbitMqSink {
    name: String,
    url: String,
    exchange: String,
    routing_key_template: String,
    channel: Mutex<Option<Channel>>,
}

impl RabbitMqSink {
    /// Create a new RabbitMQ sink. Connection is lazy (deferred to first publish).
    #[must_use]
    pub fn new(name: String, url: String, exchange: String, routing_key_template: String) -> Self {
        Self {
            name,
            url,
            exchange,
            routing_key_template,
            channel: Mutex::new(None),
        }
    }

    /// Connect to RabbitMQ, create a channel, and declare the exchange.
    async fn connect(&self) -> Result<Channel, lapin::Error> {
        let conn = Connection::connect(&self.url, ConnectionProperties::default()).await?;
        let ch = conn.create_channel().await?;

        ch.exchange_declare(
            &self.exchange,
            ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..ExchangeDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await?;

        debug!(
            sink = %self.name,
            exchange = %self.exchange,
            "connected to rabbitmq"
        );

        Ok(ch)
    }

    /// Resolve the routing key by substituting `{table}` and `{event}` placeholders.
    #[expect(
        clippy::literal_string_with_formatting_args,
        reason = "template placeholders, not format args"
    )]
    #[must_use]
    fn resolve_routing_key(&self, table: &str, event: &str) -> String {
        self.routing_key_template
            .replace("{table}", table)
            .replace("{event}", event)
    }
}

#[async_trait]
impl StreamSink for RabbitMqSink {
    fn name(&self) -> &str {
        &self.name
    }

    /// Block-level notifications are a no-op for RabbitMQ.
    /// Event data is delivered via `notify_events` instead.
    async fn notify(&self, _notification: &BlockNotification) {
        // No-op: RabbitMQ uses per-event payloads, not block summaries.
    }

    async fn notify_events(&self, events: &[EventPayload]) {
        if events.is_empty() {
            return;
        }

        let mut guard = self.channel.lock().await;

        // Ensure we have a channel
        if guard.is_none() {
            match self.connect().await {
                Ok(ch) => *guard = Some(ch),
                Err(e) => {
                    warn!(
                        sink = %self.name,
                        error = %e,
                        "failed to connect to rabbitmq, dropping {} events",
                        events.len()
                    );
                    return;
                }
            }
        }

        // Channel is guaranteed to be Some after the connection block above.
        let Some(ch) = guard.as_ref() else {
            return;
        };

        for event in events {
            let routing_key = self.resolve_routing_key(&event.table, &event.event);

            let body = match serde_json::to_vec(event) {
                Ok(b) => b,
                Err(e) => {
                    warn!(
                        sink = %self.name,
                        event = %event.event,
                        error = %e,
                        "failed to serialize event payload"
                    );
                    continue;
                }
            };

            let result = ch
                .basic_publish(
                    &self.exchange,
                    &routing_key,
                    BasicPublishOptions::default(),
                    &body,
                    BasicProperties::default()
                        .with_content_type("application/json".into())
                        .with_delivery_mode(2), // persistent
                )
                .await;

            match result {
                Ok(confirm) => {
                    // Wait for publisher confirm if enabled, but don't block on error
                    if let Err(e) = confirm.await {
                        warn!(
                            sink = %self.name,
                            routing_key = %routing_key,
                            error = %e,
                            "rabbitmq publisher confirm failed"
                        );
                        // Drop channel so we reconnect next time
                        *guard = None;
                        return;
                    }
                    debug!(
                        sink = %self.name,
                        routing_key = %routing_key,
                        block = event.block_number,
                        "published event to rabbitmq"
                    );
                }
                Err(e) => {
                    warn!(
                        sink = %self.name,
                        routing_key = %routing_key,
                        error = %e,
                        "failed to publish to rabbitmq, dropping remaining events"
                    );
                    // Drop channel so we reconnect next time
                    *guard = None;
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn template(parts: &[&str]) -> String {
        parts.concat()
    }

    #[test]
    fn resolve_routing_key_with_placeholders() {
        let sink = RabbitMqSink::new(
            "test".to_string(),
            "amqp://localhost".to_string(),
            "exchange".to_string(),
            template(&["{table}", ".", "{event}"]),
        );
        let key = sink.resolve_routing_key("usdc_transfers", "Transfer");
        assert_eq!(key, "usdc_transfers.Transfer");
    }

    #[test]
    fn resolve_routing_key_no_placeholders() {
        let sink = RabbitMqSink::new(
            "test".to_string(),
            "amqp://localhost".to_string(),
            "exchange".to_string(),
            "static.key".to_string(),
        );
        let key = sink.resolve_routing_key("usdc_transfers", "Transfer");
        assert_eq!(key, "static.key");
    }

    #[test]
    fn resolve_routing_key_table_only() {
        let sink = RabbitMqSink::new(
            "test".to_string(),
            "amqp://localhost".to_string(),
            "exchange".to_string(),
            template(&["events.", "{table}"]),
        );
        let key = sink.resolve_routing_key("trove_updated_weth", "TroveUpdated");
        assert_eq!(key, "events.trove_updated_weth");
    }
}
