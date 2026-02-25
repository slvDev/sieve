//! Database layer — SQLite storage for indexed events.
//!
//! Responsibilities:
//! - Open/create SQLite database
//! - Run user-defined schema migrations
//! - Provide transaction interface for handlers
//! - Internal tables:
//!   - _sieve_checkpoints: track which blocks have been indexed
//!   - _sieve_meta: indexer metadata (config hash, last block, etc.)
//! - Reorg support: DELETE WHERE block_number > N
