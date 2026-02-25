//! Handler system — user-defined event processing.
//!
//! Users implement the EventHandler trait to process decoded events.
//!
//! ```ignore
//! trait EventHandler: Send + Sync {
//!     fn handle(&self, event: &DecodedEvent, db: &Transaction) -> eyre::Result<()>;
//! }
//! ```
//!
//! Handlers receive decoded events and a database transaction,
//! allowing them to write to user-defined tables.
