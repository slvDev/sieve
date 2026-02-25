//! Batch fetching from peers — adapted from SHiNode.
//!
//! Reference: shinode/node/src/sync/historical/fetch.rs + fetch_task.rs
//!
//! Fetches headers and receipts in batches from assigned peers.
//! Returns raw receipts to the processing pipeline.
