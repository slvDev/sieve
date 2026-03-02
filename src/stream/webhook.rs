//! HTTP webhook sink — delivers block notifications as JSON POST requests.

use super::{BlockNotification, StreamSink};
use async_trait::async_trait;
use std::time::Duration;
use tracing::{debug, warn};

/// HTTP webhook sink that POSTs block notifications as JSON.
///
/// Best-effort delivery: failures are logged but never propagated.
/// No retries in v1.
pub struct WebhookSink {
    name: String,
    url: String,
    client: reqwest::Client,
}

impl WebhookSink {
    /// Create a new webhook sink with the given name and URL.
    #[must_use]
    pub fn new(name: String, url: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Self { name, url, client }
    }
}

#[async_trait]
impl StreamSink for WebhookSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn notify(&self, notification: &BlockNotification) {
        debug!(
            sink = %self.name,
            url = %self.url,
            block = notification.block_number,
            tables = notification.tables.len(),
            "sending webhook notification"
        );

        match self.client.post(&self.url).json(notification).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    debug!(
                        sink = %self.name,
                        block = notification.block_number,
                        status = resp.status().as_u16(),
                        "webhook delivered"
                    );
                } else {
                    warn!(
                        sink = %self.name,
                        block = notification.block_number,
                        status = resp.status().as_u16(),
                        "webhook returned non-success status"
                    );
                }
            }
            Err(e) => {
                warn!(
                    sink = %self.name,
                    block = notification.block_number,
                    error = %e,
                    "webhook delivery failed"
                );
            }
        }
    }
}
