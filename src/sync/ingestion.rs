//! Shared ordered ingestion: factory discovery, decoding, commits and notifications.

use super::engine::SyncOutcome;
use crate::chain::ChainTypes;
use crate::config::IndexConfig;
use crate::config::Selector;
use crate::db::{self, Database};
use crate::decode::DecodedParam;
use crate::handler::{
    CallRegistry, DecodedCall, EventContext, HandlerRegistry, NativeTransfer, TransferRegistry,
};
use crate::metrics::SieveMetrics;
use crate::sync::{BlockPayload, FetchItem, SkippedHeader};
use crate::toml_config::ResolvedFactory;
use crate::types::{BlockNumber, TxIndex};
use crate::{decode, filter};

use alloy_consensus::transaction::SignerRecoverable;
use alloy_consensus::transaction::TxHashRef;
use alloy_consensus::Transaction;
use alloy_consensus::TxReceipt;
use alloy_dyn_abi::JsonAbiExt;
use alloy_primitives::{Address, TxKind, B256};
use eyre::WrapErr;
use reth_chainspec::EthChainSpec;
use reth_primitives_traits::SealedHeader;
use sqlx::Postgres;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{debug, info, instrument, warn};

const PAYLOAD_CHANNEL_SIZE: usize = 8192;
/// Maximum number of blocks to batch in a single DB transaction.
const BATCH_SIZE: usize = 256;

/// Channel buffer between processing workers and the DB writer.
const PROCESSED_CHANNEL_SIZE: usize = 8192;

/// How long to wait for more payloads before flushing a partial batch.
const BATCH_FLUSH_TIMEOUT: Duration = Duration::from_millis(500);

/// Source-independent handles used by the ordered ingestion stages.
#[derive(Clone)]
pub struct IngestionContext {
    pub config: Arc<IndexConfig>,
    pub db: Arc<Database>,
    pub handlers: Arc<HandlerRegistry>,
    pub metrics: Arc<SieveMetrics>,
    pub stop_rx: watch::Receiver<bool>,
    pub factories: Arc<Vec<ResolvedFactory>>,
    pub transfer_handlers: Arc<TransferRegistry>,
    pub call_handlers: Arc<CallRegistry>,
    pub stream_dispatcher: Option<Arc<crate::stream::StreamDispatcher>>,
    pub event_table_map: Arc<HashMap<String, (String, String)>>,
    pub is_backfill: bool,
    pub receipt_tables: Arc<HashSet<String>>,
    pub bloom_filter: Option<Arc<crate::filter::BloomFilter>>,
    pub verbose: bool,
    pub worker_count: usize,
    /// Progress-only callback; ingestion never requests data from peers.
    pub peer_count: Arc<dyn Fn() -> usize + Send + Sync>,
}

impl IngestionContext {
    /// Attach peers after archive ingestion while preserving handler and factory
    /// state, the database, shutdown channel, and stream configuration.
    pub fn into_sync<C: ChainTypes>(
        self,
        pool: Arc<crate::p2p::PeerPool<C>>,
    ) -> super::SyncContext<C> {
        super::SyncContext {
            pool,
            config: self.config,
            db: self.db,
            handlers: self.handlers,
            metrics: self.metrics,
            stop_rx: self.stop_rx,
            factories: self.factories,
            transfer_handlers: self.transfer_handlers,
            call_handlers: self.call_handlers,
            stream_dispatcher: self.stream_dispatcher,
            event_table_map: self.event_table_map,
            is_backfill: self.is_backfill,
            receipt_tables: self.receipt_tables,
            bloom_filter: self.bloom_filter,
            head_seen_rx: None,
            verbose: self.verbose,
            worker_count: self.worker_count,
        }
    }

    pub fn from_sync<C: ChainTypes>(ctx: &super::SyncContext<C>) -> Self {
        let pool = Arc::clone(&ctx.pool);
        Self {
            config: Arc::clone(&ctx.config),
            db: Arc::clone(&ctx.db),
            handlers: Arc::clone(&ctx.handlers),
            metrics: Arc::clone(&ctx.metrics),
            stop_rx: ctx.stop_rx.clone(),
            factories: Arc::clone(&ctx.factories),
            transfer_handlers: Arc::clone(&ctx.transfer_handlers),
            call_handlers: Arc::clone(&ctx.call_handlers),
            stream_dispatcher: ctx.stream_dispatcher.clone(),
            event_table_map: Arc::clone(&ctx.event_table_map),
            is_backfill: ctx.is_backfill,
            receipt_tables: Arc::clone(&ctx.receipt_tables),
            bloom_filter: ctx.bloom_filter.clone(),
            verbose: ctx.verbose,
            worker_count: ctx.worker_count,
            peer_count: Arc::new(move || pool.len()),
        }
    }
}

use super::validation::{ArchiveEvidence, AuthenticatedSegment};

/// Owns the sequencer, workers, and writer. Sources close their sender and call
/// finish even after acquisition fails, so speculative factory state is restored.
pub struct IngestionPipeline<C: ChainTypes> {
    sender: mpsc::Sender<FetchItem<C>>,
    abort_rx: watch::Receiver<bool>,
    sequencer: tokio::task::JoinHandle<eyre::Result<()>>,
    workers: JoinSet<()>,
    consumer: tokio::task::JoinHandle<eyre::Result<ConsumerStats>>,
    ctx: IngestionContext,
    started: Instant,
}

impl<C: ChainTypes> IngestionPipeline<C> {
    pub async fn start(
        mut ctx: IngestionContext,
        segment: AuthenticatedSegment<C>,
    ) -> eyre::Result<Self> {
        let start = segment.start();
        let end = segment.end();
        eyre::ensure!(
            start <= end && end < i64::MAX as u64,
            "invalid ingestion range"
        );
        // The shared boundary enforces the database seam for every source.
        if !matches!(
            super::canonical::committed_frontier_status(&ctx.db).await?,
            super::canonical::FrontierStatus::Fresh
        ) {
            let checkpoint = ctx
                .db
                .last_checkpoint()
                .await?
                .map_or(0, BlockNumber::as_u64);
            eyre::ensure!(
                start == checkpoint + 1,
                "ingestion must resume at checkpoint + 1"
            );
            let stored = ctx.db.get_block_hash(BlockNumber::new(checkpoint)).await?;
            let parent = segment.get(start).map(|h| h.header().parent_hash);
            eyre::ensure!(
                stored.is_some() && stored == parent,
                "authenticated segment does not connect to committed frontier"
            );
        }
        db::ensure_chain_identity(&ctx.db, C::NAME, C::chain_spec().genesis_hash()).await?;
        db::ensure_factory_coverage(&ctx.db, &ctx.factories, start, false).await?;
        let evidence = segment.archive_evidence().cloned();
        if evidence.is_some() {
            ctx.is_backfill = true;
        }
        let started = Instant::now();
        let (sender, payload_rx) = mpsc::channel(PAYLOAD_CHANNEL_SIZE);
        let (ordered_tx, ordered_rx) = mpsc::channel(PAYLOAD_CHANNEL_SIZE);
        let (processed_tx, processed_rx) = mpsc::channel(PROCESSED_CHANNEL_SIZE);
        let (abort_tx, abort_rx) = watch::channel(false);
        let workers = spawn_processing_workers(
            ordered_rx,
            processed_tx,
            Arc::clone(&ctx.config),
            ctx.worker_count,
        );
        let sequencer = tokio::spawn(run_sequencer(
            payload_rx,
            ordered_tx,
            ctx.clone(),
            segment,
            abort_tx.clone(),
        ));
        let consumer = tokio::spawn(consume_payloads(
            processed_rx,
            ctx.clone(),
            evidence,
            end - start + 1,
            started,
            abort_tx,
            start,
            end,
        ));
        Ok(Self {
            sender,
            abort_rx,
            sequencer,
            workers,
            consumer,
            ctx,
            started,
        })
    }
    pub fn sender(&self) -> mpsc::Sender<FetchItem<C>> {
        self.sender.clone()
    }
    pub fn abort_receiver(&self) -> watch::Receiver<bool> {
        self.abort_rx.clone()
    }
    pub async fn finish(self) -> eyre::Result<SyncOutcome> {
        drop(self.sender);
        let worker_failure = join_pipeline_stages(self.sequencer, self.workers).await;
        let result = match self.consumer.await {
            Ok(result) => result,
            Err(err) => Err(eyre::eyre!("consumer task failed: {err}")),
        };
        let stats = match (result, worker_failure) {
            (Ok(stats), None) => stats,
            (Err(err), _) | (Ok(_), Some(err)) => {
                return Err(finish_rejected_run(
                    &self.ctx.db,
                    &self.ctx.config,
                    &self.ctx.factories,
                    err,
                )
                .await)
            }
        };
        Ok(SyncOutcome {
            blocks_fetched: stats.blocks_fetched,
            total_receipts: stats.total_receipts,
            events_matched: stats.events_matched,
            events_decoded: stats.events_decoded,
            events_stored: stats.events_stored,
            transfers_stored: stats.transfers_stored,
            calls_stored: stats.calls_stored,
            elapsed: self.started.elapsed(),
        })
    }
}

/// Check authentication before any factory discovery or filtering. Payload root
/// checks happened once when ValidatedPayload was constructed, in either source.
fn authenticate_item<C: ChainTypes>(
    item: &FetchItem<C>,
    segment: &AuthenticatedSegment<C>,
    ctx: &IngestionContext,
) -> eyre::Result<()> {
    let number = match item {
        FetchItem::Payload(p) => p.header().number,
        FetchItem::Skipped(s) => s.number,
    };
    let expected = segment
        .get(number)
        .ok_or_else(|| eyre::eyre!("block {number} outside authenticated segment"))?;
    match item {
        FetchItem::Payload(p) => eyre::ensure!(
            p.header() == expected.header(),
            "payload header differs from authenticated header at {number}"
        ),
        FetchItem::Skipped(s) => {
            eyre::ensure!(
                s.hash == expected.hash() && s.parent_hash == expected.header().parent_hash,
                "skipped header differs from authenticated header"
            );
            eyre::ensure!(
                ctx.factories.is_empty()
                    && ctx.transfer_handlers.is_empty()
                    && ctx.call_handlers.is_empty(),
                "cannot skip payloads with factories, calls, or transfers"
            );
            eyre::ensure!(
                ctx.bloom_filter
                    .as_ref()
                    .is_some_and(|b| !b.header_may_match(expected.header())),
                "payload skip is not authorized by bloom filter"
            );
        }
    }
    Ok(())
}

/// Accumulated stats from the payload consumer.
#[derive(Debug, Default)]
struct ConsumerStats {
    blocks_fetched: u64,
    total_receipts: u64,
    events_matched: u64,
    events_decoded: u64,
    events_stored: u64,
    transfers_stored: u64,
    calls_stored: u64,
}

/// Outcome of processing events from a single block.
#[derive(Debug)]
struct ProcessOutcome {
    block_number: u64,
    block_timestamp: u64,
    matched: u64,
    decoded: u64,
    stored: u64,
    transfers: u64,
    calls: u64,
    /// Per-table insert counts: `(table_name, event_name, count)`.
    table_counts: Vec<(String, String, u64)>,
    /// Per-event decoded payloads for streaming (only populated when streams are configured).
    event_payloads: Vec<crate::stream::EventPayload>,
}

/// CPU-processed block ready for DB storage.
///
/// Created by [`prepare_block`], consumed by [`flush_batch`].
/// Keeps the original [`BlockPayload`] for transfer/call scanning.
struct ProcessedBlock<C: ChainTypes> {
    block_number: BlockNumber,
    block_hash: B256,
    factory_discoveries: Vec<filter::FactoryDiscovery>,
    decoded_events: Vec<decode::DecodedEvent>,
    matched_count: u64,
    receipt_count: u64,
    payload: BlockPayload<C>,
}

/// Item flowing from the processing workers to the DB writer.
enum ProcessedItem<C: ChainTypes> {
    /// Fully processed block ready for storage.
    Block(Box<ProcessedBlock<C>>),
    /// Bloom-skipped block: hash record only.
    Skipped(SkippedHeader),
}

impl<C: ChainTypes> ProcessedItem<C> {
    const fn number(&self) -> u64 {
        match self {
            Self::Block(block) => block.block_number.as_u64(),
            Self::Skipped(skipped) => skipped.number,
        }
    }

    const fn hash(&self) -> B256 {
        match self {
            Self::Block(block) => block.block_hash,
            Self::Skipped(skipped) => skipped.hash,
        }
    }

    const fn parent_hash(&self) -> B256 {
        match self {
            Self::Block(block) => block.payload.header().parent_hash,
            Self::Skipped(skipped) => skipped.parent_hash,
        }
    }
}

/// Reorders out-of-order items into strict block-number order.
///
/// Used twice in the pipeline: by the sequencer (so factory discovery and
/// registration happen in block order before any later block is filtered)
/// and by the DB writer (so only a contiguous, anchored prefix is ever
/// committed and notified — no unverified row is observable via the API
/// or dispatched to webhooks/queues).
struct ReorderBuffer<T> {
    /// Next block number expected by the contiguous prefix.
    next: u64,
    /// Items received ahead of the contiguous frontier.
    pending: std::collections::BTreeMap<u64, T>,
}

/// Hard cap on buffered out-of-order items before the run is aborted.
///
/// The scheduler's look-ahead is normally far smaller; hitting this means
/// one block is persistently unfetchable while later blocks stream in, and
/// aborting (checkpoint intact) is safer than growing without bound.
const MAX_REORDER_PENDING: usize = 16_384;

impl<T> ReorderBuffer<T> {
    const fn new(start: u64) -> Self {
        Self {
            next: start,
            pending: std::collections::BTreeMap::new(),
        }
    }

    /// Buffer an item; ignores duplicates below the contiguous frontier.
    fn push(&mut self, number: u64, item: T) {
        if number >= self.next {
            self.pending.insert(number, item);
        } else {
            debug!(
                block = number,
                next = self.next,
                "dropping duplicate item below contiguous frontier"
            );
        }
    }

    /// Drain the contiguous run starting at `next` into `out`.
    fn drain_contiguous(&mut self, out: &mut Vec<T>) {
        while let Some(item) = self.pending.remove(&self.next) {
            self.next = self.next.saturating_add(1);
            out.push(item);
        }
    }

    /// Number of buffered out-of-order items.
    fn buffered(&self) -> usize {
        self.pending.len()
    }
}

/// Item flowing from the sequencer to the processing workers.
///
/// Factory discovery has already happened (in block order), so workers
/// only filter and decode — which is order-independent.
enum OrderedItem<C: ChainTypes> {
    /// Payload plus the factory children it created (already registered).
    Payload {
        payload: Box<BlockPayload<C>>,
        factory_discoveries: Vec<filter::FactoryDiscovery>,
    },
    /// Bloom-skipped block: hash record only.
    Skipped(SkippedHeader),
}

// ── Processing workers ───────────────────────────────────────────────

/// Spawn N parallel workers that read raw payloads, run CPU-bound
/// filter+decode, and send `ProcessedBlock`s to the DB writer.
#[expect(
    clippy::needless_pass_by_value,
    reason = "Arc/Sender are cloned into spawned tasks"
)]
fn spawn_processing_workers<C: ChainTypes>(
    ordered_rx: mpsc::Receiver<OrderedItem<C>>,
    processed_tx: mpsc::Sender<ProcessedItem<C>>,
    config: Arc<IndexConfig>,
    num_workers: usize,
) -> JoinSet<()> {
    let num_workers = num_workers.max(1);

    info!(num_workers, "spawning block processing workers");

    let ordered_rx = Arc::new(tokio::sync::Mutex::new(ordered_rx));
    let mut workers = JoinSet::new();

    for _ in 0..num_workers {
        let rx = Arc::clone(&ordered_rx);
        let tx = processed_tx.clone();
        let cfg = Arc::clone(&config);

        workers.spawn(async move {
            loop {
                let item = {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                };
                let Some(item) = item else { break };
                let processed = match item {
                    OrderedItem::Payload {
                        payload,
                        factory_discoveries,
                    } => ProcessedItem::Block(Box::new(prepare_block(
                        *payload,
                        &cfg,
                        factory_discoveries,
                    ))),
                    OrderedItem::Skipped(skipped) => ProcessedItem::Skipped(skipped),
                };
                if tx.send(processed).await.is_err() {
                    break;
                }
            }
        });
    }

    workers
}

/// Sequencing stage between fetch and the processing workers.
///
/// With factories configured, items are reordered into strict block-number
/// order and factory discovery/registration runs here, sequentially —
/// guaranteeing that when a worker filters block N+1, every child created
/// up to and including block N is already registered. Without factories
/// there is no cross-block dependency, so items pass through unordered
/// (avoiding head-of-line blocking; the DB writer restores order anyway).
///
/// # Errors
///
/// Returns an error (after firing the abort signal) if the reorder buffer
/// exceeds [`MAX_REORDER_PENDING`].
async fn run_sequencer<C: ChainTypes>(
    mut payload_rx: mpsc::Receiver<FetchItem<C>>,
    ordered_tx: mpsc::Sender<OrderedItem<C>>,
    ctx: IngestionContext,
    segment: AuthenticatedSegment<C>,
    abort_tx: watch::Sender<bool>,
) -> eyre::Result<()> {
    let config = &ctx.config;
    let factories = &ctx.factories;
    let start_block = segment.start();
    if factories.is_empty() {
        while let Some(item) = payload_rx.recv().await {
            if let Err(err) = authenticate_item(&item, &segment, &ctx) {
                let _ = abort_tx.send(true);
                return Err(err);
            }
            let out = match item {
                FetchItem::Payload(payload) => OrderedItem::Payload {
                    payload: Box::new(payload.into_inner()),
                    factory_discoveries: Vec::new(),
                },
                FetchItem::Skipped(skipped) => OrderedItem::Skipped(skipped),
            };
            if ordered_tx.send(out).await.is_err() {
                return Ok(());
            }
        }
        return Ok(());
    }

    let mut buffer: ReorderBuffer<FetchItem<C>> = ReorderBuffer::new(start_block);
    let mut ready = Vec::new();
    while let Some(item) = payload_rx.recv().await {
        if let Err(err) = authenticate_item(&item, &segment, &ctx) {
            let _ = abort_tx.send(true);
            return Err(err);
        }
        let number = match &item {
            FetchItem::Payload(payload) => payload.header().number,
            FetchItem::Skipped(skipped) => skipped.number,
        };
        buffer.push(number, item);
        buffer.drain_contiguous(&mut ready);
        if buffer.buffered() > MAX_REORDER_PENDING {
            let _ = abort_tx.send(true);
            return Err(eyre::eyre!(
                "sequencer buffer exceeded {MAX_REORDER_PENDING} items waiting for block {}; \
                 aborting run (a block appears unfetchable)",
                buffer.next
            ));
        }
        for item in std::mem::take(&mut ready) {
            let out = match item {
                FetchItem::Payload(payload) => {
                    let payload = Box::new(payload.into_inner());
                    // FAIL-CLOSED: an undecodable matching creation event
                    // aborts the run — coverage must not advance past it.
                    let factory_discoveries = match scan_and_register(&payload, config, factories) {
                        Ok(discoveries) => discoveries,
                        Err(err) => {
                            let _ = abort_tx.send(true);
                            return Err(err);
                        }
                    };
                    OrderedItem::Payload {
                        payload,
                        factory_discoveries,
                    }
                }
                FetchItem::Skipped(skipped) => OrderedItem::Skipped(skipped),
            };
            if ordered_tx.send(out).await.is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}

// ── DB writer (payload consumer) ────────────────────────────────────

#[expect(
    clippy::too_many_arguments,
    reason = "grouping these into a struct would add complexity without benefit"
)]
#[instrument(skip_all)]
async fn consume_payloads<C: ChainTypes>(
    mut processed_rx: mpsc::Receiver<ProcessedItem<C>>,
    ingestion: IngestionContext,
    archive_evidence: Option<ArchiveEvidence>,
    total_blocks: u64,
    started_at: Instant,
    abort_tx: watch::Sender<bool>,
    start_block: u64,
    end_block: u64,
) -> eyre::Result<ConsumerStats> {
    let mut stats = ConsumerStats::default();
    let mut last_log = Instant::now();
    let mut max_indexed_block: u64 = 0;
    let mut batch: Vec<ProcessedItem<C>> = Vec::with_capacity(BATCH_SIZE);
    let mut reorder = ReorderBuffer::new(start_block);
    let coverage_keys = db::FactoryCoverageKeys::new(&ingestion.factories);

    let ctx = ProcessContext {
        config: &ingestion.config,
        db: &ingestion.db,
        handlers: &ingestion.handlers,
        transfer_handlers: &ingestion.transfer_handlers,
        call_handlers: &ingestion.call_handlers,
        event_table_map: &ingestion.event_table_map,
        has_streams: ingestion.stream_dispatcher.is_some(),
        receipt_tables: &ingestion.receipt_tables,
        coverage_keys: &coverage_keys,
        archive_evidence: archive_evidence.as_ref(),
    };

    loop {
        // Block indefinitely when batch is empty; use timeout when non-empty
        let processed = if batch.is_empty() {
            processed_rx.recv().await
        } else {
            match tokio::time::timeout(BATCH_FLUSH_TIMEOUT, processed_rx.recv()).await {
                Ok(processed) => processed,
                Err(_timeout) => {
                    flush_batch(
                        &mut batch,
                        &ctx,
                        &mut stats,
                        &ingestion.metrics,
                        &mut max_indexed_block,
                        ingestion.stream_dispatcher.as_ref(),
                        ingestion.is_backfill,
                        &abort_tx,
                    )
                    .await?;
                    continue;
                }
            }
        };

        // Channel closed: flush remaining batch and exit
        let Some(processed) = processed else {
            if !batch.is_empty() {
                flush_batch(
                    &mut batch,
                    &ctx,
                    &mut stats,
                    &ingestion.metrics,
                    &mut max_indexed_block,
                    ingestion.stream_dispatcher.as_ref(),
                    ingestion.is_backfill,
                    &abort_tx,
                )
                .await?;
            }
            // The channel closed. Unless an external stop cut the run
            // short, the contiguous frontier must have reached the end of
            // the range — anything else means blocks were lost in flight
            // (e.g. a panicked worker) and success must not be reported.
            if !*ingestion.stop_rx.borrow() && reorder.next != end_block.saturating_add(1) {
                let _ = abort_tx.send(true);
                return Err(eyre::eyre!(
                    "sync range incomplete: contiguous frontier stopped at {} but the range \
                     ends at {end_block} ({} items still buffered out of order)",
                    reorder.next,
                    reorder.buffered()
                ));
            }
            break;
        };

        if let ProcessedItem::Block(block) = &processed {
            stats.blocks_fetched = stats.blocks_fetched.saturating_add(1);
            stats.total_receipts = stats.total_receipts.saturating_add(block.receipt_count);
        }

        buffer_item(&mut reorder, &mut batch, processed, &abort_tx)?;

        if batch.len() >= BATCH_SIZE {
            flush_batch(
                &mut batch,
                &ctx,
                &mut stats,
                &ingestion.metrics,
                &mut max_indexed_block,
                ingestion.stream_dispatcher.as_ref(),
                ingestion.is_backfill,
                &abort_tx,
            )
            .await?;
        }

        log_sync_progress(
            &stats,
            &mut last_log,
            total_blocks,
            &*ingestion.peer_count,
            started_at,
            ingestion.verbose,
            &ingestion.stop_rx,
        );
    }

    Ok(stats)
}

/// Shared references for the DB writer (reduces argument counts).
struct ProcessContext<'a> {
    config: &'a IndexConfig,
    db: &'a Database,
    handlers: &'a HandlerRegistry,
    transfer_handlers: &'a TransferRegistry,
    call_handlers: &'a CallRegistry,
    /// Maps `"contract:event"` key → `(table_name, event_name)`.
    event_table_map: &'a HashMap<String, (String, String)>,
    /// Whether streams are configured (gates event payload collection).
    has_streams: bool,
    /// Table names with `include_receipts = true` (for streaming enrichment).
    receipt_tables: &'a HashSet<String>,
    /// Configured factory identities whose coverage advances with the checkpoint.
    coverage_keys: &'a db::FactoryCoverageKeys,
    archive_evidence: Option<&'a ArchiveEvidence>,
}

/// Log sync progress every 2 seconds.
fn log_sync_progress(
    stats: &ConsumerStats,
    last_log: &mut Instant,
    total_blocks: u64,
    peer_count: &dyn Fn() -> usize,
    started_at: Instant,
    verbose: bool,
    stop_rx: &watch::Receiver<bool>,
) {
    if *stop_rx.borrow() || last_log.elapsed() < Duration::from_secs(2) {
        return;
    }
    if verbose {
        info!(
            blocks_fetched = stats.blocks_fetched,
            total_receipts = stats.total_receipts,
            events_matched = stats.events_matched,
            events_decoded = stats.events_decoded,
            events_stored = stats.events_stored,
            transfers_stored = stats.transfers_stored,
            calls_stored = stats.calls_stored,
            "sync progress"
        );
    } else {
        crate::ui::print_sync_progress(
            stats.blocks_fetched,
            total_blocks,
            peer_count(),
            started_at,
        );
    }
    *last_log = Instant::now();
}

/// Compute the sealed hash for a block header.
fn compute_block_hash(header: &reth_primitives_traits::Header) -> alloy_primitives::B256 {
    SealedHeader::seal_slow(header.clone()).hash()
}

/// CPU-only block processing: factory pre-scan, filter, and decode.
///
/// Factory children were already discovered and registered by the
/// sequencer (in strict block order), so this stage is order-independent
/// and safe to run on parallel workers. DB persistence of the discoveries
/// is deferred to [`flush_batch`].
fn prepare_block<C: ChainTypes>(
    payload: BlockPayload<C>,
    config: &IndexConfig,
    factory_discoveries: Vec<filter::FactoryDiscovery>,
) -> ProcessedBlock<C> {
    let block_number = BlockNumber::new(payload.header().number);
    let block_hash = compute_block_hash(payload.header());
    let receipt_count = payload.receipts().len() as u64;

    // Filter + decode
    let matched = filter::filter_block(&payload, config);
    let matched_count = matched.len() as u64;
    let decoded_events = decode_matched_logs(&matched, config);

    ProcessedBlock {
        block_number,
        block_hash,
        factory_discoveries,
        decoded_events,
        matched_count,
        receipt_count,
        payload,
    }
}

/// Scan a payload for factory creation events and register the children
/// in-memory.
///
/// MUST be called in strict block-number order (the sequencer's job):
/// filtering of any later block depends on every earlier registration.
fn scan_and_register<C: ChainTypes>(
    payload: &BlockPayload<C>,
    config: &IndexConfig,
    factories: &[ResolvedFactory],
) -> eyre::Result<Vec<filter::FactoryDiscovery>> {
    let discoveries = filter::scan_factory_events(payload, factories)?;
    for discovery in &discoveries {
        register_factory_child_in_memory(config, discovery);
    }
    Ok(discoveries)
}

/// Register a factory child in-memory only (no DB write).
fn register_factory_child_in_memory(config: &IndexConfig, discovery: &filter::FactoryDiscovery) {
    let Some(contract_idx) = config
        .contracts
        .iter()
        .position(|c| c.name == discovery.child_contract_name)
    else {
        warn!(
            factory = %discovery.child_contract_name,
            "factory child references unknown contract"
        );
        return;
    };

    if config.register_factory_child(discovery.child_address, contract_idx) {
        info!(
            factory = %discovery.child_contract_name,
            child = ?discovery.child_address,
            block = discovery.block_number,
            "registered new factory child"
        );
    }
}

/// Flush a batch of processed blocks to the database in a single transaction.
///
/// Opens one Postgres transaction, stores all block hashes, events,
/// transfers, calls, and factory children. Updates the checkpoint once
/// with the maximum block number (using GREATEST). Dispatches stream
/// notifications after commit.
#[expect(
    clippy::too_many_arguments,
    reason = "grouping these into a struct would add complexity without benefit"
)]
async fn flush_batch<C: ChainTypes>(
    batch: &mut Vec<ProcessedItem<C>>,
    ctx: &ProcessContext<'_>,
    stats: &mut ConsumerStats,
    metrics: &SieveMetrics,
    max_indexed_block: &mut u64,
    stream_dispatcher: Option<&Arc<crate::stream::StreamDispatcher>>,
    is_backfill: bool,
    abort_tx: &watch::Sender<bool>,
) -> eyre::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let batch_len = batch.len() as u64;

    // Integrity gate: every adjacency whose other side is known (same batch
    // or already stored) must link. A violation means a peer served fork
    // data (or a reorg raced the fetch) — abort the run instead of storing.
    // Speculative in-memory state (factory children) is discarded by
    // `IngestionPipeline::finish` once the workers have stopped.
    match verify_batch_anchors(batch, ctx.db).await? {
        AnchorCheck::Ok => {}
        AnchorCheck::ParentMismatch {
            block,
            actual,
            expected,
        } => {
            let _ = abort_tx.send(true);
            return Err(eyre::eyre!(
                "block {block} parent-hash mismatch: header claims parent {actual} but chain has \
                 {expected} — refusing to store batch (fork data from peer or concurrent reorg)"
            ));
        }
        AnchorCheck::ChildMismatch {
            parent_block,
            child_block,
        } => {
            // A previously committed descendant does not link to the newly
            // verified parent. Roll stored state back to the last committed
            // contiguous checkpoint (the batch itself is uncommitted, so
            // the target must not include any of its blocks), then abort so
            // the retry refetches everything above it.
            let _ = abort_tx.send(true);
            let committed = batch
                .iter()
                .map(ProcessedItem::number)
                .min()
                .unwrap_or(child_block)
                .saturating_sub(1);
            rollback_committed_to(ctx, committed).await?;
            return Err(eyre::eyre!(
                "stored block {child_block} does not link to verified parent {parent_block}; \
                 rolled back to committed checkpoint {committed} (fork data from peer or \
                 concurrent reorg)"
            ));
        }
    }

    // The batch is a contiguous run by construction (reorder buffer), so
    // committing it moves the checkpoint to its last block. Nothing beyond
    // the contiguous prefix is ever committed or notified.
    let checkpoint_to = batch.iter().map(ProcessedItem::number).max();

    match flush_batch_inner(batch, ctx, checkpoint_to).await {
        Ok(outcomes) => {
            update_batch_stats(stats, metrics, max_indexed_block, &outcomes, batch_len);
            dispatch_batch_notifications(stream_dispatcher, outcomes, is_backfill);
        }
        Err(err) => {
            let _ = abort_tx.send(true);
            return Err(err.wrap_err(format!("failed to flush batch of {batch_len} blocks")));
        }
    }
    batch.clear();
    Ok(())
}

/// Buffer an item and drain the contiguous prefix into the flush batch.
///
/// Only the contiguous prefix ever reaches the flush batch: rows and
/// notifications for out-of-order blocks must not be published until every
/// predecessor is committed and anchored.
///
/// # Errors
///
/// Returns an error (and fires the abort signal) if the out-of-order
/// buffer exceeds [`MAX_REORDER_PENDING`].
fn buffer_item<C: ChainTypes>(
    reorder: &mut ReorderBuffer<ProcessedItem<C>>,
    batch: &mut Vec<ProcessedItem<C>>,
    processed: ProcessedItem<C>,
    abort_tx: &watch::Sender<bool>,
) -> eyre::Result<()> {
    reorder.push(processed.number(), processed);
    reorder.drain_contiguous(batch);
    if reorder.buffered() > MAX_REORDER_PENDING {
        let _ = abort_tx.send(true);
        return Err(eyre::eyre!(
            "reorder buffer exceeded {MAX_REORDER_PENDING} items waiting for block {}; \
             aborting run (a block appears unfetchable)",
            reorder.next
        ));
    }
    Ok(())
}

/// Roll back all indexed state above `block` in one transaction (used when
/// a committed descendant fails adjacency verification).
async fn rollback_committed_to(ctx: &ProcessContext<'_>, block: u64) -> eyre::Result<()> {
    let target = BlockNumber::new(block);
    let mut tx = ctx.db.begin().await?;
    db::verification::ensure_rollback_above_archive(&mut tx, target).await?;
    ctx.handlers.rollback_all(target, &mut tx).await?;
    ctx.transfer_handlers.rollback_all(target, &mut tx).await?;
    ctx.call_handlers.rollback_all(target, &mut tx).await?;
    db::rollback_factory_children(&mut tx, target, ctx.config).await?;
    db::rollback_to(&mut tx, target).await?;
    tx.commit()
        .await
        .wrap_err("failed to commit adjacency rollback")?;
    warn!(block, "rolled back stored state above verified parent");
    Ok(())
}

/// Wait for the sequencer and workers to drain, surfacing any failure.
///
/// A panicked or failed stage may have lost blocks — that must fail the
/// run, never be silently ignored. (All worker processed_tx clones
/// dropping lets the DB writer see channel close.)
async fn join_pipeline_stages(
    sequencer_handle: tokio::task::JoinHandle<eyre::Result<()>>,
    mut worker_set: JoinSet<()>,
) -> Option<eyre::Report> {
    let mut failure: Option<eyre::Report> = match sequencer_handle.await {
        Ok(Ok(())) => None,
        Ok(Err(err)) => Some(err),
        Err(join_err) => Some(eyre::eyre!("sequencer task failed: {join_err}")),
    };
    while let Some(joined) = worker_set.join_next().await {
        if let Err(join_err) = joined {
            failure = Some(eyre::eyre!("processing worker failed: {join_err}"));
        }
    }
    failure
}

/// Clean up after a sync run whose consumer failed (rejected batch or
/// panic): discard speculative in-memory factory state by reloading the
/// committed set from the database.
///
/// Must only be called once the processing workers are joined — they
/// register factory children speculatively, and a late registration would
/// survive the rebuild. Returns the error to propagate: the original one,
/// or a non-retriable [`crate::sync::FactoryStateError`] if the rebuild
/// itself failed and speculative state may still be live.
async fn finish_rejected_run(
    db: &Database,
    config: &IndexConfig,
    factories: &[ResolvedFactory],
    err: eyre::Report,
) -> eyre::Report {
    match db::rebuild_factory_children(db, config, factories).await {
        Ok(children) => {
            debug!(
                children,
                "restored committed factory children after rejected batch"
            );
            err
        }
        Err(rebuild_err) => crate::sync::FactoryStateError::report(&err, &rebuild_err),
    }
}

/// Verify parent-hash linkage for every block in the batch.
///
/// Builds a map of known canonical hashes from the batch itself plus any
/// previously stored hashes for parents outside the batch, then checks each
/// block's `parent_hash` against it. Parents with no known hash (e.g.
/// bloom-skipped blocks that never stored one) are skipped.
///
/// # Errors
///
/// Returns an error on the first linkage violation, or if the stored-hash
/// lookup fails.
async fn verify_batch_anchors<C: ChainTypes>(
    batch: &[ProcessedItem<C>],
    db: &Database,
) -> eyre::Result<AnchorCheck> {
    let mut known_hashes: HashMap<u64, B256> =
        batch.iter().map(|b| (b.number(), b.hash())).collect();
    let mut known_parents: HashMap<u64, B256> = batch
        .iter()
        .map(|b| (b.number(), b.parent_hash()))
        .collect();

    let triples: Vec<(u64, B256, B256)> = batch
        .iter()
        .map(|b| (b.number(), b.hash(), b.parent_hash()))
        .collect();

    // Resolve neighbors outside the batch from previously stored rows:
    // the parent's hash (for the parent-direction check) and the child's
    // parent hash (for the child-direction check).
    for &(number, _, _) in &triples {
        if let Some(parent) = number.checked_sub(1) {
            if let std::collections::hash_map::Entry::Vacant(entry) = known_hashes.entry(parent) {
                if let Some(hash) = db.get_block_hash(BlockNumber::new(parent)).await? {
                    entry.insert(hash);
                }
            }
        }
        if let Some(child) = number.checked_add(1) {
            if let std::collections::hash_map::Entry::Vacant(entry) = known_parents.entry(child) {
                if let Some(parent_hash) =
                    db::get_block_parent_hash(db, BlockNumber::new(child)).await?
                {
                    entry.insert(parent_hash);
                }
            }
        }
    }

    Ok(find_anchor_violation(
        &triples,
        &known_hashes,
        &known_parents,
    ))
}

/// Outcome of batch adjacency verification.
#[derive(Debug, PartialEq, Eq)]
enum AnchorCheck {
    /// All known adjacencies link.
    Ok,
    /// A batch block's parent hash contradicts the known parent hash.
    ParentMismatch {
        block: u64,
        actual: B256,
        expected: B256,
    },
    /// A known child's parent hash contradicts a batch block's hash.
    ChildMismatch { parent_block: u64, child_block: u64 },
}

/// Check every adjacency of the batch in both directions.
///
/// `triples` holds `(number, hash, parent_hash)` for each batch item in any
/// order; `known_hashes`/`known_parents` map block number → hash / parent
/// hash for every block whose value is known (batch plus stored neighbors).
/// Unknown neighbors are skipped — they are checked when their side flushes.
fn find_anchor_violation(
    triples: &[(u64, B256, B256)],
    known_hashes: &HashMap<u64, B256>,
    known_parents: &HashMap<u64, B256>,
) -> AnchorCheck {
    for &(number, hash, parent_hash) in triples {
        if let Some(parent_number) = number.checked_sub(1) {
            if let Some(&expected) = known_hashes.get(&parent_number) {
                if parent_hash != expected {
                    return AnchorCheck::ParentMismatch {
                        block: number,
                        actual: parent_hash,
                        expected,
                    };
                }
            }
        }
        if let Some(child_number) = number.checked_add(1) {
            if let Some(&child_parent) = known_parents.get(&child_number) {
                if child_parent != hash {
                    return AnchorCheck::ChildMismatch {
                        parent_block: number,
                        child_block: child_number,
                    };
                }
            }
        }
    }
    AnchorCheck::Ok
}

/// Inner flush: open one transaction, store all blocks, commit.
///
/// Phase 0: batch store all block hashes in one UNNEST query.
/// Phase 1: per-block preparation — factory children, build event contexts,
/// scan transfers/calls, compute outcomes.
/// Phase 2: batch insert — multi-row INSERT all events/transfers/calls.
/// Phase 3: checkpoint + commit.
async fn flush_batch_inner<C: ChainTypes>(
    batch: &[ProcessedItem<C>],
    ctx: &ProcessContext<'_>,
    checkpoint_to: Option<u64>,
) -> eyre::Result<Vec<ProcessOutcome>> {
    let mut tx = ctx.db.begin().await?;
    let mut outcomes = Vec::with_capacity(batch.len());
    let mut all_events: Vec<(&decode::DecodedEvent, EventContext)> = Vec::new();
    let mut all_transfers: Vec<(NativeTransfer, EventContext)> = Vec::new();
    let mut all_calls: Vec<(DecodedCall, EventContext)> = Vec::new();

    // Batch store all block hashes (payload AND bloom-skipped) in one
    // UNNEST query, including parent hashes for adjacency verification.
    let block_numbers: Vec<i64> = batch.iter().map(|b| b.number() as i64).collect();
    let block_hashes: Vec<Vec<u8>> = batch.iter().map(|b| b.hash().as_slice().to_vec()).collect();
    let parent_hashes: Vec<Vec<u8>> = batch
        .iter()
        .map(|b| b.parent_hash().as_slice().to_vec())
        .collect();
    db::store_block_hashes_batch(&mut tx, &block_numbers, &block_hashes, &parent_hashes).await?;

    // Phase 1: per-block preparation (payload blocks only)
    for item in batch {
        let ProcessedItem::Block(block) = item else {
            continue;
        };
        let outcome = prepare_block_outcome(
            block,
            ctx,
            &mut tx,
            &mut all_events,
            &mut all_transfers,
            &mut all_calls,
        )
        .await?;
        outcomes.push(outcome);
    }

    // Phase 2: batch inserts
    let event_refs: Vec<(&decode::DecodedEvent, &EventContext)> =
        all_events.iter().map(|(e, c)| (*e, c)).collect();
    ctx.handlers.batch_dispatch(&event_refs, &mut tx).await?;
    if !all_transfers.is_empty() {
        let transfer_refs: Vec<(&NativeTransfer, &EventContext)> =
            all_transfers.iter().map(|(t, c)| (t, c)).collect();
        ctx.transfer_handlers
            .batch_dispatch(&transfer_refs, &mut tx)
            .await?;
    }
    if !all_calls.is_empty() {
        let call_refs: Vec<(&DecodedCall, &EventContext)> =
            all_calls.iter().map(|(c, ctx)| (c, ctx)).collect();
        ctx.call_handlers
            .batch_dispatch(&call_refs, &mut tx)
            .await?;
    }

    // Phase 3: checkpoint (contiguous prefix only) + commit. Factory coverage
    // and the authenticated frontier advance with indexed rows. Archive trust
    // inputs remain distinct from a peer-quorum decision.
    if let Some(checkpoint) = checkpoint_to {
        db::update_checkpoint(&mut tx, BlockNumber::new(checkpoint)).await?;
        db::advance_factory_coverage(&mut tx, ctx.coverage_keys, BlockNumber::new(checkpoint))
            .await?;
        let checkpoint_hash = batch
            .iter()
            .find(|item| item.number() == checkpoint)
            .map(ProcessedItem::hash)
            .ok_or_else(|| {
                eyre::eyre!("checkpoint block {checkpoint} not found in committed batch")
            })?;
        if let Some(evidence) = ctx.archive_evidence {
            db::verification::advance_archive_frontier(
                &mut tx,
                checkpoint,
                checkpoint_hash,
                evidence,
            )
            .await?;
        } else {
            db::advance_verified_frontier(&mut tx, BlockNumber::new(checkpoint), &checkpoint_hash)
                .await?;
        }
    }

    tx.commit()
        .await
        .wrap_err("failed to commit batch transaction")?;

    Ok(outcomes)
}

/// Prepare one block's outcome and accumulate events/transfers/calls for batch insert.
///
/// Stores factory children (small, per-block). Accumulates events, transfers,
/// and calls into the shared vecs for Phase 2 batch insert. Block hashes are
/// stored in bulk by the caller via `store_block_hashes_batch`.
async fn prepare_block_outcome<'a, C: ChainTypes>(
    block: &'a ProcessedBlock<C>,
    ctx: &ProcessContext<'_>,
    tx: &mut sqlx::Transaction<'_, Postgres>,
    all_events: &mut Vec<(&'a decode::DecodedEvent, EventContext)>,
    all_transfers: &mut Vec<(NativeTransfer, EventContext)>,
    all_calls: &mut Vec<(DecodedCall, EventContext)>,
) -> eyre::Result<ProcessOutcome> {
    // Block hashes are batched in flush_batch_inner via store_block_hashes_batch.

    // Persist factory discoveries within the batch transaction, bound to
    // the full factory identity that discovered them.
    for d in &block.factory_discoveries {
        db::store_factory_child(tx, d).await?;
    }

    let mut stored_count = 0u64;
    let mut table_counts: HashMap<String, (String, u64)> = HashMap::new();
    let mut event_payloads: Vec<crate::stream::EventPayload> = Vec::new();
    let mut sender_cache: HashMap<TxIndex, Address> = HashMap::new();

    // Events: count matches and accumulate for batch insert
    for event in &block.decoded_events {
        let event_context =
            build_event_context(&block.payload, block.block_hash, event, &mut sender_cache)?;
        let dispatched = ctx
            .handlers
            .matching_count(&event.contract_name, &event.event_name);
        if dispatched > 0 {
            track_event_table(ctx.event_table_map, event, dispatched, &mut table_counts);
            if ctx.has_streams {
                collect_event_payload(
                    ctx.event_table_map,
                    event,
                    &event_context,
                    ctx.receipt_tables,
                    &mut event_payloads,
                );
            }
            all_events.push((event, event_context));
        }
        stored_count = stored_count.saturating_add(dispatched);
    }

    // Transfers: scan and accumulate for batch insert
    let transfer_count = accumulate_transfers(
        &block.payload,
        block.block_hash,
        &mut sender_cache,
        ctx,
        all_transfers,
        &mut table_counts,
        &mut event_payloads,
    )?;

    // Calls: scan and accumulate for batch insert
    let call_count = accumulate_calls(
        &block.payload,
        block.block_hash,
        &mut sender_cache,
        ctx,
        all_calls,
        &mut table_counts,
        &mut event_payloads,
    )?;

    let table_counts = table_counts
        .into_iter()
        .map(|(table, (event, count))| (table, event, count))
        .collect();

    Ok(ProcessOutcome {
        block_number: block.block_number.as_u64(),
        block_timestamp: block.payload.header().timestamp,
        matched: block.matched_count,
        decoded: block.decoded_events.len() as u64,
        stored: stored_count,
        transfers: transfer_count,
        calls: call_count,
        table_counts,
        event_payloads,
    })
}

/// Update consumer stats and Prometheus metrics after a successful batch flush.
fn update_batch_stats(
    stats: &mut ConsumerStats,
    metrics: &SieveMetrics,
    max_indexed_block: &mut u64,
    outcomes: &[ProcessOutcome],
    batch_len: u64,
) {
    metrics.blocks_indexed.inc_by(batch_len);
    for outcome in outcomes {
        stats.events_matched = stats.events_matched.saturating_add(outcome.matched);
        stats.events_decoded = stats.events_decoded.saturating_add(outcome.decoded);
        stats.events_stored = stats.events_stored.saturating_add(outcome.stored);
        stats.transfers_stored = stats.transfers_stored.saturating_add(outcome.transfers);
        stats.calls_stored = stats.calls_stored.saturating_add(outcome.calls);
        metrics.events_matched.inc_by(outcome.matched);
        metrics.events_stored.inc_by(outcome.stored);
        metrics.transfers_stored.inc_by(outcome.transfers);
        metrics.calls_stored.inc_by(outcome.calls);

        if outcome.block_number > *max_indexed_block {
            *max_indexed_block = outcome.block_number;
            metrics.indexed_block.set(outcome.block_number as i64);
            metrics.last_block_timestamp.store(
                outcome.block_timestamp,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
    }
}

/// Dispatch stream notifications for all blocks in a flushed batch.
///
/// Takes ownership of `outcomes` to avoid cloning `event_payloads`.
fn dispatch_batch_notifications(
    stream_dispatcher: Option<&Arc<crate::stream::StreamDispatcher>>,
    outcomes: Vec<ProcessOutcome>,
    is_backfill: bool,
) {
    let Some(dispatcher) = stream_dispatcher else {
        return;
    };
    for outcome in outcomes {
        if !outcome.table_counts.is_empty() {
            let tables = outcome
                .table_counts
                .into_iter()
                .map(|(name, event, count)| crate::stream::TableNotification { name, event, count })
                .collect();
            dispatcher.send(
                crate::stream::BlockNotification {
                    block_number: outcome.block_number,
                    block_timestamp: outcome.block_timestamp,
                    tables,
                },
                is_backfill,
            );
        }
        if !outcome.event_payloads.is_empty() {
            dispatcher.send_events(outcome.event_payloads, is_backfill);
        }
    }
}

/// Track an event's table in the table_counts map.
fn track_event_table(
    event_table_map: &HashMap<String, (String, String)>,
    event: &decode::DecodedEvent,
    dispatched: u64,
    table_counts: &mut HashMap<String, (String, u64)>,
) {
    let key = format!("{}:{}", event.contract_name, event.event_name);
    if let Some((table, event_name)) = event_table_map.get(&key) {
        let entry = table_counts
            .entry(table.clone())
            .or_insert_with(|| (event_name.clone(), 0));
        entry.1 = entry.1.saturating_add(dispatched);
    }
}

/// Build an [`EventPayload`] from a decoded event and push it to the payloads vec.
fn collect_event_payload(
    event_table_map: &HashMap<String, (String, String)>,
    event: &decode::DecodedEvent,
    context: &EventContext,
    receipt_tables: &HashSet<String>,
    payloads: &mut Vec<crate::stream::EventPayload>,
) {
    let key = format!("{}:{}", event.contract_name, event.event_name);
    let Some((table, event_name)) = event_table_map.get(&key) else {
        return;
    };

    let mut data = serde_json::Map::new();
    for param in event.indexed.iter().chain(event.body.iter()) {
        data.insert(
            param.name.clone(),
            crate::stream::dyn_sol_to_json(&param.value),
        );
    }

    let include = receipt_tables.contains(table);
    payloads.push(crate::stream::EventPayload {
        table: table.clone(),
        event: event_name.clone(),
        contract_name: event.contract_name.clone(),
        contract: Address::to_checksum(&event.contract_address, None),
        block_number: event.block_number.as_u64(),
        block_timestamp: event.block_timestamp,
        tx_hash: format!("{:#x}", event.tx_hash),
        log_index: Some(event.log_index.as_u32()),
        tx_index: event.tx_index.as_u32(),
        tx_from: Some(Address::to_checksum(&context.tx_from, None)),
        tx_value: include.then(|| context.tx_value.to_string()),
        tx_gas_price: include.then_some(context.tx_gas_price as u64),
        gas_used: include.then_some(context.tx_gas_used),
        nonce: include.then_some(context.tx_nonce),
        cumulative_gas_used: include.then_some(context.cumulative_gas_used),
        status: include.then_some(context.tx_status),
        data,
    });
}

/// Build an [`EventPayload`] from a native transfer.
fn build_transfer_payload(
    transfer: &NativeTransfer,
    context: &EventContext,
    table_name: &str,
    include: bool,
) -> crate::stream::EventPayload {
    let mut data = serde_json::Map::new();
    data.insert(
        "from_address".to_string(),
        serde_json::Value::String(Address::to_checksum(&transfer.from_address, None)),
    );
    data.insert(
        "to_address".to_string(),
        serde_json::Value::String(Address::to_checksum(&transfer.to_address, None)),
    );
    data.insert(
        "value".to_string(),
        serde_json::Value::String(transfer.value.to_string()),
    );

    crate::stream::EventPayload {
        table: table_name.to_string(),
        event: "transfer".to_string(),
        contract_name: String::new(),
        contract: "0x0000000000000000000000000000000000000000".to_string(),
        block_number: transfer.block_number.as_u64(),
        block_timestamp: context.block_timestamp,
        tx_hash: format!("{:#x}", transfer.tx_hash),
        log_index: None,
        tx_index: transfer.tx_index.as_u32(),
        tx_from: Some(Address::to_checksum(&context.tx_from, None)),
        tx_value: include.then(|| context.tx_value.to_string()),
        tx_gas_price: include.then_some(context.tx_gas_price as u64),
        gas_used: include.then_some(context.tx_gas_used),
        nonce: include.then_some(context.tx_nonce),
        cumulative_gas_used: include.then_some(context.cumulative_gas_used),
        status: include.then_some(context.tx_status),
        data,
    }
}

/// Build an [`EventPayload`] from a decoded function call.
fn build_call_payload(
    call: &DecodedCall,
    context: &EventContext,
    table_name: &str,
    include: bool,
) -> crate::stream::EventPayload {
    let mut data = serde_json::Map::new();
    for param in &call.params {
        data.insert(
            param.name.clone(),
            crate::stream::dyn_sol_to_json(&param.value),
        );
    }

    crate::stream::EventPayload {
        table: table_name.to_string(),
        event: call.function_name.clone(),
        contract_name: call.contract_name.clone(),
        contract: Address::to_checksum(&call.contract_address, None),
        block_number: call.block_number.as_u64(),
        block_timestamp: context.block_timestamp,
        tx_hash: format!("{:#x}", call.tx_hash),
        log_index: None,
        tx_index: call.tx_index.as_u32(),
        tx_from: Some(Address::to_checksum(&context.tx_from, None)),
        tx_value: include.then(|| context.tx_value.to_string()),
        tx_gas_price: include.then_some(context.tx_gas_price as u64),
        gas_used: include.then_some(context.tx_gas_used),
        nonce: include.then_some(context.tx_nonce),
        cumulative_gas_used: include.then_some(context.cumulative_gas_used),
        status: include.then_some(context.tx_status),
        data,
    }
}

/// Scan native transfers and accumulate for batch insert (no DB writes).
fn accumulate_transfers<C: ChainTypes>(
    payload: &BlockPayload<C>,
    block_hash: B256,
    sender_cache: &mut HashMap<TxIndex, Address>,
    ctx: &ProcessContext<'_>,
    all_transfers: &mut Vec<(NativeTransfer, EventContext)>,
    table_counts: &mut HashMap<String, (String, u64)>,
    event_payloads: &mut Vec<crate::stream::EventPayload>,
) -> eyre::Result<u64> {
    if ctx.transfer_handlers.is_empty() {
        return Ok(0);
    }
    scan_transfers(
        payload,
        block_hash,
        sender_cache,
        ctx.transfer_handlers,
        ctx.has_streams,
        all_transfers,
        event_payloads,
        table_counts,
        ctx.receipt_tables,
    )
}

/// Scan function calls and accumulate for batch insert (no DB writes).
fn accumulate_calls<C: ChainTypes>(
    payload: &BlockPayload<C>,
    block_hash: B256,
    sender_cache: &mut HashMap<TxIndex, Address>,
    ctx: &ProcessContext<'_>,
    all_calls: &mut Vec<(DecodedCall, EventContext)>,
    table_counts: &mut HashMap<String, (String, u64)>,
    event_payloads: &mut Vec<crate::stream::EventPayload>,
) -> eyre::Result<u64> {
    if ctx.call_handlers.is_empty() {
        return Ok(0);
    }
    scan_calls(
        payload,
        block_hash,
        sender_cache,
        ctx.config,
        ctx.call_handlers,
        ctx.has_streams,
        all_calls,
        event_payloads,
        table_counts,
        ctx.receipt_tables,
    )
}

/// Compute per-transaction gas used from cumulative gas values.
///
/// For the first transaction in a block, `gas_used == cumulative_gas_used`.
/// For subsequent transactions, `gas_used = cumulative[i] - cumulative[i-1]`.
fn compute_gas_used<R: TxReceipt>(receipts: &[R], tx_idx: usize) -> u64 {
    let cumulative = receipts
        .get(tx_idx)
        .map_or(0, TxReceipt::cumulative_gas_used);
    if tx_idx == 0 {
        cumulative
    } else {
        cumulative.saturating_sub(
            receipts
                .get(tx_idx - 1)
                .map_or(0, TxReceipt::cumulative_gas_used),
        )
    }
}

/// Build an [`EventContext`] from a block payload for a given decoded event.
///
/// Caches sender recovery per `tx_index` to avoid redundant ECDSA work
/// when multiple events originate from the same transaction.
fn build_event_context<C: ChainTypes>(
    payload: &BlockPayload<C>,
    block_hash: B256,
    event: &decode::DecodedEvent,
    sender_cache: &mut HashMap<TxIndex, Address>,
) -> eyre::Result<EventContext> {
    let tx_signed = payload
        .body()
        .transactions
        .get(event.tx_index.as_u32() as usize)
        .ok_or_else(|| {
            eyre::eyre!(
                "tx_index {} out of range for block {}",
                event.tx_index,
                payload.header().number
            )
        })?;

    let tx_from = if let Some(&cached) = sender_cache.get(&event.tx_index) {
        cached
    } else {
        let sender = tx_signed
            .recover_signer_unchecked()
            .wrap_err("failed to recover tx sender")?;
        sender_cache.insert(event.tx_index, sender);
        sender
    };

    let tx_to = match tx_signed.kind() {
        TxKind::Call(addr) => Some(addr),
        TxKind::Create => None,
    };

    let tx_idx_usize = event.tx_index.as_u32() as usize;
    let cumulative = payload
        .receipts()
        .get(tx_idx_usize)
        .map_or(0, TxReceipt::cumulative_gas_used);
    let gas_used = compute_gas_used(payload.receipts(), tx_idx_usize);

    Ok(EventContext {
        block_timestamp: payload.header().timestamp,
        block_hash,
        tx_from,
        tx_to,
        tx_value: tx_signed.value(),
        tx_gas_price: tx_signed.effective_gas_price(payload.header().base_fee_per_gas),
        tx_gas_used: gas_used,
        tx_nonce: tx_signed.nonce(),
        cumulative_gas_used: cumulative,
        tx_status: true,
    })
}

/// Scan block transactions for native ETH transfers (no DB writes).
///
/// Iterates all transactions in the block body, skipping zero-value,
/// contract-creation, and reverted transactions. Accumulates matching
/// transfers for batch insert.
///
/// Returns the number of transfers matched.
#[expect(
    clippy::too_many_arguments,
    reason = "per-table counting adds table_counts"
)]
fn scan_transfers<C: ChainTypes>(
    payload: &BlockPayload<C>,
    block_hash: B256,
    sender_cache: &mut HashMap<TxIndex, Address>,
    transfer_handlers: &TransferRegistry,
    has_streams: bool,
    all_transfers: &mut Vec<(NativeTransfer, EventContext)>,
    event_payloads: &mut Vec<crate::stream::EventPayload>,
    table_counts: &mut HashMap<String, (String, u64)>,
    receipt_tables: &HashSet<String>,
) -> eyre::Result<u64> {
    let block_number = BlockNumber::new(payload.header().number);
    let receipts = payload.receipts();
    let mut count = 0u64;

    for (tx_idx, tx_signed) in payload.body().transactions.iter().enumerate() {
        let tx_index = TxIndex::from_usize(tx_idx);
        if tx_signed.value().is_zero() {
            continue;
        }
        if !receipts.get(tx_idx).is_some_and(TxReceipt::status) {
            continue;
        }
        let to_address = match tx_signed.kind() {
            TxKind::Call(addr) => addr,
            TxKind::Create => continue,
        };

        let from_address = if let Some(&cached) = sender_cache.get(&tx_index) {
            cached
        } else {
            let sender = tx_signed
                .recover_signer_unchecked()
                .wrap_err("failed to recover tx sender for transfer")?;
            sender_cache.insert(tx_index, sender);
            sender
        };

        let transfer = NativeTransfer {
            block_number,
            tx_hash: *tx_signed.tx_hash(),
            tx_index,
            from_address,
            to_address,
            value: tx_signed.value(),
        };

        let matched_tables = transfer_handlers.matched_table_names(&transfer);
        if matched_tables.is_empty() {
            continue;
        }

        let context = EventContext {
            block_timestamp: payload.header().timestamp,
            block_hash,
            tx_from: from_address,
            tx_to: Some(to_address),
            tx_value: tx_signed.value(),
            tx_gas_price: tx_signed.effective_gas_price(payload.header().base_fee_per_gas),
            tx_gas_used: compute_gas_used(receipts, tx_idx),
            tx_nonce: tx_signed.nonce(),
            cumulative_gas_used: receipts
                .get(tx_idx)
                .map_or(0, TxReceipt::cumulative_gas_used),
            tx_status: true,
        };

        let dispatched = matched_tables.len() as u64;
        for table_name in &matched_tables {
            let entry = table_counts
                .entry((*table_name).to_string())
                .or_insert_with(|| ("transfer".to_string(), 0));
            entry.1 = entry.1.saturating_add(1);
            if has_streams {
                let include = receipt_tables.contains(*table_name);
                event_payloads.push(build_transfer_payload(
                    &transfer, &context, table_name, include,
                ));
            }
        }

        all_transfers.push((transfer, context));
        count = count.saturating_add(dispatched);
    }

    Ok(count)
}

/// Scan block transactions for function calls (no DB writes).
///
/// Iterates all transactions in the block body, matching calldata selectors
/// against configured functions. Accumulates matching calls for batch insert.
///
/// Returns the number of calls matched.
#[expect(
    clippy::too_many_arguments,
    reason = "per-table counting adds table_counts"
)]
fn scan_calls<C: ChainTypes>(
    payload: &BlockPayload<C>,
    block_hash: B256,
    sender_cache: &mut HashMap<TxIndex, Address>,
    config: &IndexConfig,
    call_handlers: &CallRegistry,
    has_streams: bool,
    all_calls: &mut Vec<(DecodedCall, EventContext)>,
    event_payloads: &mut Vec<crate::stream::EventPayload>,
    table_counts: &mut HashMap<String, (String, u64)>,
    receipt_tables: &HashSet<String>,
) -> eyre::Result<u64> {
    let block_number = BlockNumber::new(payload.header().number);
    let receipts = payload.receipts();
    let mut count = 0u64;

    for (tx_idx, tx_signed) in payload.body().transactions.iter().enumerate() {
        let tx_index = TxIndex::from_usize(tx_idx);
        let input = tx_signed.input();

        if input.len() < 4 {
            continue;
        }

        let to_address = match tx_signed.kind() {
            TxKind::Call(addr) => addr,
            TxKind::Create => continue,
        };

        let Some(contract) = config.contract_for_address(&to_address) else {
            continue;
        };

        let selector = Selector::from_slice(&input[..4]);

        let Some(function) = contract.functions.get(&selector) else {
            continue;
        };

        if !receipts.get(tx_idx).is_some_and(TxReceipt::status) {
            continue;
        }

        let decoded_values = match function.abi_decode_input(&input[4..]) {
            Ok(values) => values,
            Err(e) => {
                warn!(
                    block = block_number.as_u64(),
                    tx_index = tx_index.as_u32(),
                    function = %function.name,
                    error = %e,
                    "failed to decode function call"
                );
                continue;
            }
        };

        let params: Vec<DecodedParam> = function
            .inputs
            .iter()
            .zip(decoded_values)
            .map(|(input_param, value)| DecodedParam {
                name: input_param.name.clone(),
                solidity_type: input_param.ty.clone(),
                value,
            })
            .collect();

        let from_address = if let Some(&cached) = sender_cache.get(&tx_index) {
            cached
        } else {
            let sender = tx_signed
                .recover_signer_unchecked()
                .wrap_err("failed to recover tx sender for call")?;
            sender_cache.insert(tx_index, sender);
            sender
        };

        let decoded_call = DecodedCall {
            function_name: function.name.clone(),
            contract_name: contract.name.clone(),
            params,
            block_number,
            tx_hash: *tx_signed.tx_hash(),
            tx_index,
            contract_address: to_address,
        };

        let matched_tables = call_handlers
            .matched_table_names(&decoded_call.contract_name, &decoded_call.function_name);
        if matched_tables.is_empty() {
            continue;
        }

        let context = EventContext {
            block_timestamp: payload.header().timestamp,
            block_hash,
            tx_from: from_address,
            tx_to: Some(to_address),
            tx_value: tx_signed.value(),
            tx_gas_price: tx_signed.effective_gas_price(payload.header().base_fee_per_gas),
            tx_gas_used: compute_gas_used(receipts, tx_idx),
            tx_nonce: tx_signed.nonce(),
            cumulative_gas_used: receipts
                .get(tx_idx)
                .map_or(0, TxReceipt::cumulative_gas_used),
            tx_status: true,
        };

        count = count.saturating_add(track_call_tables(
            &matched_tables,
            &decoded_call,
            &context,
            has_streams,
            event_payloads,
            table_counts,
            receipt_tables,
        ));

        all_calls.push((decoded_call, context));
    }

    Ok(count)
}

/// Track per-table call counts and optionally build stream payloads.
fn track_call_tables(
    matched_tables: &[&str],
    decoded_call: &DecodedCall,
    context: &EventContext,
    has_streams: bool,
    event_payloads: &mut Vec<crate::stream::EventPayload>,
    table_counts: &mut HashMap<String, (String, u64)>,
    receipt_tables: &HashSet<String>,
) -> u64 {
    for table_name in matched_tables {
        let entry = table_counts
            .entry((*table_name).to_string())
            .or_insert_with(|| (decoded_call.function_name.clone(), 0));
        entry.1 = entry.1.saturating_add(1);
        if has_streams {
            let include = receipt_tables.contains(*table_name);
            event_payloads.push(build_call_payload(
                decoded_call,
                context,
                table_name,
                include,
            ));
        }
    }
    matched_tables.len() as u64
}

/// Decode matched logs into events, logging any decode failures.
fn decode_matched_logs(
    matched: &[filter::FilteredLog],
    config: &crate::config::IndexConfig,
) -> Vec<decode::DecodedEvent> {
    let mut decoded_events = Vec::new();
    for filtered_log in matched {
        let Some(contract) = config.contract_for_address(&filtered_log.log.address) else {
            continue;
        };
        match decode::decode_log(filtered_log, contract) {
            Ok(decoded) => {
                debug!("{decoded}");
                decoded_events.push(decoded);
            }
            Err(e) => {
                warn!(
                    block = filtered_log.block_number.as_u64(),
                    tx_index = filtered_log.tx_index.as_u32(),
                    error = %e,
                    "failed to decode log"
                );
            }
        }
    }
    decoded_events
}

#[cfg(test)]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions in tests are idiomatic"
)]
mod tests {
    use super::*;

    fn make_receipt(cumulative: u64) -> reth_ethereum_primitives::Receipt {
        reth_ethereum_primitives::Receipt {
            cumulative_gas_used: cumulative,
            ..Default::default()
        }
    }

    #[test]
    fn compute_gas_used_first_tx() {
        let receipts = vec![make_receipt(21_000)];
        assert_eq!(compute_gas_used(&receipts, 0), 21_000);
    }

    #[test]
    fn anchor_violation_none_for_linked_blocks() {
        let h1 = B256::repeat_byte(0x01);
        let h2 = B256::repeat_byte(0x02);
        let known_hashes: HashMap<u64, B256> = [(10, h1), (11, h2)].into_iter().collect();
        let known_parents: HashMap<u64, B256> = std::iter::once((11, h1)).collect();
        // Block 11 claims parent h1 (hash of block 10) — linked.
        let triples = vec![(11u64, h2, h1)];
        assert_eq!(
            find_anchor_violation(&triples, &known_hashes, &known_parents),
            AnchorCheck::Ok
        );
    }

    #[test]
    fn anchor_violation_detected_against_stored_parent() {
        let stored_parent = B256::repeat_byte(0x01);
        let forged_parent = B256::repeat_byte(0xEE);
        let known_hashes: HashMap<u64, B256> = std::iter::once((10, stored_parent)).collect();
        let known_parents: HashMap<u64, B256> = HashMap::new();
        let triples = vec![(11u64, B256::repeat_byte(0x02), forged_parent)];
        assert_eq!(
            find_anchor_violation(&triples, &known_hashes, &known_parents),
            AnchorCheck::ParentMismatch {
                block: 11,
                actual: forged_parent,
                expected: stored_parent,
            }
        );
    }

    #[test]
    fn anchor_violation_detected_against_stored_child() {
        // A stored child (block 12) claims a parent hash that does not
        // match the newly verified block 11 — the stored segment is bad.
        let my_hash = B256::repeat_byte(0x02);
        let child_parent = B256::repeat_byte(0xEE);
        let known_hashes: HashMap<u64, B256> = std::iter::once((11, my_hash)).collect();
        let known_parents: HashMap<u64, B256> = std::iter::once((12, child_parent)).collect();
        let triples = vec![(11u64, my_hash, B256::repeat_byte(0x01))];
        assert_eq!(
            find_anchor_violation(&triples, &known_hashes, &known_parents),
            AnchorCheck::ChildMismatch {
                parent_block: 11,
                child_block: 12,
            }
        );
    }

    #[test]
    fn anchor_violation_skips_unknown_neighbors() {
        let known_hashes: HashMap<u64, B256> = HashMap::new();
        let known_parents: HashMap<u64, B256> = HashMap::new();
        // Neither neighbor of block 11 is known — no check possible.
        let triples = vec![(11u64, B256::repeat_byte(0x02), B256::repeat_byte(0xEE))];
        assert_eq!(
            find_anchor_violation(&triples, &known_hashes, &known_parents),
            AnchorCheck::Ok
        );
    }

    #[test]
    fn anchor_violation_skips_genesis() {
        let known_hashes: HashMap<u64, B256> = HashMap::new();
        let known_parents: HashMap<u64, B256> = HashMap::new();
        let triples = vec![(0u64, B256::repeat_byte(0x01), B256::ZERO)];
        assert_eq!(
            find_anchor_violation(&triples, &known_hashes, &known_parents),
            AnchorCheck::Ok
        );
    }

    /// Regression test for the parallel-worker factory race: a child
    /// created in block N must already be registered when block N+1 is
    /// filtered, even when N+1 arrives first. This is the sequencer's
    /// ordering contract (reorder → scan+register per block, in order →
    /// forward), exercised synchronously.
    #[test]
    fn sequencer_registers_children_before_later_blocks() -> eyre::Result<()> {
        use crate::chain::EthereumChain;
        use crate::test_utils::{build_test_transaction, make_log, make_receipt};
        use alloy_primitives::address;

        let child_abi = r#"[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]"#;
        let factory_abi_json = r#"[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"pool","type":"address"}],"name":"PoolCreated","type":"event"}]"#;

        let child_contract =
            crate::config::ContractConfig::new("Pool", Address::ZERO, child_abi, &["Transfer"])?;
        let config = IndexConfig::new(vec![child_contract]);

        let factory_abi: alloy_json_abi::JsonAbi =
            serde_json::from_str(factory_abi_json).map_err(|e| eyre::eyre!("abi parse: {e}"))?;
        let creation_event = factory_abi
            .events
            .get("PoolCreated")
            .and_then(|v| v.first())
            .ok_or_else(|| eyre::eyre!("no PoolCreated"))?;

        let factory_addr = address!("1F98431c8aD98523631AE4a59f267346ea31F984");
        let child_addr = address!("8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8");
        let factory = ResolvedFactory {
            factory_address: factory_addr,
            creation_event: creation_event.clone(),
            creation_selector: creation_event.selector(),
            child_address_param: "pool".to_owned(),
            child_contract_name: "Pool".to_owned(),
            start_block: 100,
        };

        // Block 100: pool creation. Block 101: the pool emits a Transfer.
        let creation_log = make_log(
            factory_addr,
            vec![
                creation_event.selector(),
                B256::left_padding_from(child_addr.as_slice()),
            ],
            alloy_primitives::Bytes::new(),
        );
        let creation_payload = BlockPayload::<EthereumChain>::new(
            reth_primitives_traits::Header {
                number: 100,
                ..Default::default()
            },
            alloy_consensus::BlockBody {
                transactions: vec![build_test_transaction()],
                ..Default::default()
            },
            vec![make_receipt(vec![creation_log])],
        );

        let transfer_selector: B256 =
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                .parse()
                .map_err(|e| eyre::eyre!("parse: {e}"))?;
        let transfer_log = make_log(
            child_addr,
            vec![
                transfer_selector,
                B256::repeat_byte(0x01),
                B256::repeat_byte(0x02),
            ],
            alloy_primitives::Bytes::from_static(&[0u8; 32]),
        );
        let child_payload = BlockPayload::<EthereumChain>::new(
            reth_primitives_traits::Header {
                number: 101,
                ..Default::default()
            },
            alloy_consensus::BlockBody {
                transactions: vec![build_test_transaction()],
                ..Default::default()
            },
            vec![make_receipt(vec![transfer_log])],
        );

        // Control: before registration the child's event does not match.
        assert_eq!(filter::filter_block(&child_payload, &config).len(), 0);

        // Simulate out-of-order arrival: block 101 first, then block 100.
        let mut buffer: ReorderBuffer<BlockPayload<EthereumChain>> = ReorderBuffer::new(100);
        let mut ready = Vec::new();
        buffer.push(101, child_payload);
        buffer.drain_contiguous(&mut ready);
        assert!(
            ready.is_empty(),
            "later block must wait for its predecessor"
        );

        buffer.push(100, creation_payload);
        buffer.drain_contiguous(&mut ready);
        assert_eq!(ready.len(), 2);

        // Ordered processing (the sequencer loop): registration of block
        // 100's child happens before block 101 is filtered.
        let mut matched_in_child_block = 0;
        for payload in &ready {
            let disc = scan_and_register(payload, &config, std::slice::from_ref(&factory))?;
            if payload.header().number == 100 {
                assert_eq!(disc.len(), 1, "block 100 must discover the child");
                assert!(
                    config.contract_for_address(&child_addr).is_some(),
                    "child must be registered after block 100"
                );
            }
            if payload.header().number == 101 {
                matched_in_child_block = filter::filter_block(payload, &config).len();
            }
        }
        assert_eq!(matched_in_child_block, 1);
        Ok(())
    }

    fn skipped(number: u64) -> ProcessedItem<crate::chain::EthereumChain> {
        ProcessedItem::Skipped(crate::sync::SkippedHeader {
            number,
            hash: B256::repeat_byte(0x01),
            parent_hash: B256::repeat_byte(0x02),
        })
    }

    #[test]
    fn reorder_buffer_releases_contiguous_prefix_only() {
        let mut buffer = ReorderBuffer::new(10);
        let mut batch = Vec::new();

        // Out-of-order blocks beyond a hole stay buffered.
        buffer.push(12, skipped(12));
        buffer.push(13, skipped(13));
        buffer.drain_contiguous(&mut batch);
        assert!(batch.is_empty());
        assert_eq!(buffer.buffered(), 2);

        // Hole filled: the whole run drains in order.
        buffer.push(10, skipped(10));
        buffer.push(11, skipped(11));
        buffer.drain_contiguous(&mut batch);
        let numbers: Vec<u64> = batch.iter().map(ProcessedItem::number).collect();
        assert_eq!(numbers, vec![10, 11, 12, 13]);
        assert_eq!(buffer.buffered(), 0);

        // Duplicates below the frontier are dropped.
        buffer.push(9, skipped(9));
        assert_eq!(buffer.buffered(), 0);

        // Next contiguous block flows straight through.
        batch.clear();
        buffer.push(14, skipped(14));
        buffer.drain_contiguous(&mut batch);
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn compute_gas_used_second_tx() {
        let receipts = vec![make_receipt(21_000), make_receipt(63_000)];
        assert_eq!(compute_gas_used(&receipts, 1), 42_000);
    }

    #[test]
    fn compute_gas_used_third_tx() {
        let receipts = vec![
            make_receipt(50_000),
            make_receipt(120_000),
            make_receipt(200_000),
        ];
        assert_eq!(compute_gas_used(&receipts, 0), 50_000);
        assert_eq!(compute_gas_used(&receipts, 1), 70_000);
        assert_eq!(compute_gas_used(&receipts, 2), 80_000);
    }

    #[test]
    fn compute_gas_used_out_of_bounds() {
        let receipts = vec![make_receipt(21_000)];
        assert_eq!(compute_gas_used(&receipts, 5), 0);
    }

    #[test]
    fn compute_gas_used_empty_receipts() {
        let receipts: Vec<reth_ethereum_primitives::Receipt> = vec![];
        assert_eq!(compute_gas_used(&receipts, 0), 0);
    }
}
