//! Generated Base fixtures exercise the real shared sequencer and PostgreSQL writer.
#![expect(clippy::panic_in_result_fn, reason = "test assertions")]
use super::ingestion::*;
use super::validation::{
    verify_archive_recovery, ArchiveEvidence, ArchiveFrontier, ArchiveRecoveryReader,
    AuthenticatedSegment, ValidatedPayload,
};
use super::{BlockPayload, FetchItem};
use crate::chain::{BaseChain, ChainTypes};
use crate::handler::{EventContext, EventHandler};
use alloy_consensus::{proofs, BlockBody, Receipt, Signed, TxLegacy};
use alloy_primitives::{Address, Bytes, Signature, TxKind, B256, U256};
use reth_chainspec::EthChainSpec;
use reth_primitives_traits::{Header, SealedHeader};
use std::{collections::HashSet, sync::Arc};
use tokio::sync::{mpsc, watch};

const ABI: &str = r#"[{"type":"event","name":"Ping","anonymous":false,"inputs":[{"name":"value","type":"uint256","indexed":false}]},{"type":"event","name":"Created","anonymous":false,"inputs":[{"name":"child","type":"address","indexed":true}]}]"#;
const CHILD: Address = Address::repeat_byte(0x11);
const FACTORY: Address = Address::repeat_byte(0x22);

fn factory() -> eyre::Result<crate::toml_config::ResolvedFactory> {
    let abi: alloy_json_abi::JsonAbi = serde_json::from_str(ABI)?;
    let event = abi.events["Created"][0].clone();
    Ok(crate::toml_config::ResolvedFactory {
        factory_address: FACTORY,
        creation_selector: event.selector(),
        creation_event: event,
        child_address_param: "child".into(),
        child_address_source: crate::config::ParamSource::Indexed(0),
        child_contract_name: "Child".into(),
        start_block: 100,
    })
}

pub fn fixture() -> eyre::Result<Vec<BlockPayload<BaseChain>>> {
    let abi: alloy_json_abi::JsonAbi = serde_json::from_str(ABI)?;
    let f = factory()?;
    let mut blocks = Vec::new();
    for number in 100..=104 {
        let mut logs = Vec::new();
        if number == 100 {
            logs.push(crate::test_utils::make_log(
                FACTORY,
                vec![
                    f.creation_selector,
                    B256::left_padding_from(CHILD.as_slice()),
                ],
                Bytes::new(),
            ));
        }
        if number < 102 {
            logs.push(crate::test_utils::make_log(
                CHILD,
                vec![abi.events["Ping"][0].selector()],
                Bytes::copy_from_slice(&U256::from(number).to_be_bytes::<32>()),
            ));
        }
        blocks.push(logs);
    }
    Ok(chain(blocks))
}

/// Linked, commitment-valid Base blocks from 100; non-empty blocks hold one tx carrying the logs.
fn chain(blocks: Vec<Vec<alloy_primitives::Log>>) -> Vec<BlockPayload<BaseChain>> {
    chain_with_calls(blocks.into_iter().map(|logs| (logs, None)).collect())
}

/// One block's logs plus an optional `(to, input)` call.
type Block = (Vec<alloy_primitives::Log>, Option<(Address, Bytes)>);

/// Like [`chain`]; a block's optional call becomes its transaction.
fn chain_with_calls(blocks: Vec<Block>) -> Vec<BlockPayload<BaseChain>> {
    let mut parent = B256::repeat_byte(3);
    let mut result = Vec::with_capacity(blocks.len());
    for (number, (logs, call)) in (100..).zip(blocks) {
        let (transactions, receipts) = if logs.is_empty() && call.is_none() {
            (vec![], vec![])
        } else {
            let (to, input) = call.map_or((TxKind::Create, Bytes::new()), |(to, input)| {
                (TxKind::Call(to), input)
            });
            let tx = TxLegacy {
                chain_id: Some(8453),
                nonce: number,
                gas_limit: 21_000,
                to,
                input,
                ..Default::default()
            };
            let signed =
                Signed::new_unhashed(tx, Signature::new(U256::from(1), U256::from(1), false));
            (
                vec![op_alloy_consensus::OpTxEnvelope::Legacy(signed)],
                vec![op_alloy_consensus::OpReceipt::Legacy(Receipt {
                    status: true.into(),
                    cumulative_gas_used: 21_000,
                    logs,
                })],
            )
        };
        let body = BlockBody {
            transactions,
            ..Default::default()
        };
        let mut header = Header {
            number,
            parent_hash: parent,
            timestamp: 1_690_000_000,
            transactions_root: proofs::calculate_transaction_root(&body.transactions),
            ommers_hash: proofs::calculate_ommers_root(&body.ommers),
            ..Default::default()
        };
        header.receipts_root = BaseChain::receipts_root(&receipts, &header);
        parent = SealedHeader::seal_slow(header.clone()).hash();
        result.push(BlockPayload::new(header, body, receipts));
    }
    result
}

fn evidence(headers: &[Header]) -> eyre::Result<ArchiveEvidence> {
    let last = headers.last().ok_or_else(|| eyre::eyre!("empty fixture"))?;
    Ok(ArchiveEvidence {
        manifest_sha256: B256::repeat_byte(9),
        genesis_hash: BaseChain::chain_spec().genesis_hash(),
        first_block: 100,
        anchor_block: last.number,
        anchor_hash: SealedHeader::seal_slow(last.clone()).hash(),
    })
}

struct Reader(Vec<Header>);
impl ArchiveRecoveryReader for Reader {
    fn headers<'a>(
        &'a self,
        _: &ArchiveEvidence,
    ) -> eyre::Result<Box<dyn Iterator<Item = eyre::Result<Header>> + 'a>> {
        Ok(Box::new(self.0.iter().cloned().map(Ok)))
    }
}

struct Recorder {
    fail: bool,
}
#[async_trait::async_trait]
impl EventHandler for Recorder {
    fn name(&self) -> &'static str {
        "phase2"
    }
    fn table_name(&self) -> &'static str {
        "phase2_events"
    }
    fn matches(&self, contract: &str, topic0: B256) -> bool {
        contract == "Child" && topic0 == alloy_primitives::keccak256("Ping(uint256)")
    }
    async fn handle(
        &self,
        event: &crate::decode::DecodedEvent,
        context: &EventContext,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> eyre::Result<()> {
        eyre::ensure!(!self.fail, "injected handler failure");
        sqlx::query(
            "INSERT INTO phase2_events (block_number, block_hash, value) VALUES ($1, $2, $3)",
        )
        .bind(event.block_number.as_u64() as i64)
        .bind(context.block_hash.as_slice())
        .bind(format!("{:?}", event.body))
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
    async fn rollback(
        &self,
        number: crate::types::BlockNumber,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> eyre::Result<()> {
        sqlx::query("DELETE FROM phase2_events WHERE block_number > $1")
            .bind(number.as_u64() as i64)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }
}

struct Sink {
    db: Arc<crate::db::Database>,
    tx: mpsc::UnboundedSender<(u64, u64)>,
}
#[async_trait::async_trait]
impl crate::stream::StreamSink for Sink {
    fn name(&self) -> &'static str {
        "phase2"
    }
    async fn notify(&self, n: &crate::stream::BlockNotification) {
        let checkpoint = self
            .db
            .last_checkpoint()
            .await
            .ok()
            .flatten()
            .map_or(0, crate::types::BlockNumber::as_u64);
        let _ = self.tx.send((n.block_number, checkpoint));
    }
}

const RESET_SYNC_STATE: &str = "TRUNCATE _sieve_archive_groups, _sieve_archive_job, _sieve_archive_frontier, _sieve_canonical, _sieve_block_hashes, _sieve_factory_children, _sieve_factories; UPDATE _sieve_checkpoints SET block_number = 0;";

pub async fn context(
    fail: bool,
) -> eyre::Result<(
    IngestionContext,
    mpsc::UnboundedReceiver<(u64, u64)>,
    mpsc::UnboundedReceiver<(u64, u64)>,
)> {
    let url = std::env::var("SIEVE_PHASE2_DATABASE_URL")?;
    eyre::ensure!(
        url.ends_with("/sieve_phase2"),
        "use an isolated sieve_phase2 database"
    );
    let db = Arc::new(crate::db::Database::connect(&url).await?);
    crate::db::create_internal_tables(&db).await?;
    sqlx::raw_sql(&format!("{RESET_SYNC_STATE} CREATE TABLE IF NOT EXISTS phase2_events (block_number BIGINT, block_hash BYTEA, value TEXT); TRUNCATE phase2_events;")).execute(db.pool()).await?;
    crate::db::ensure_chain_identity(&db, "base", BaseChain::chain_spec().genesis_hash()).await?;
    let config = Arc::new(crate::config::IndexConfig::new(vec![
        crate::config::ContractConfig::new("Child", Address::ZERO, ABI, &["Ping"])?,
    ]));
    let factories = Arc::new(vec![factory()?]);
    crate::db::ensure_factory_coverage(&db, &factories, 100, false).await?;
    let (_stop_tx, stop_rx) = watch::channel(false);
    let (tx, rx) = mpsc::unbounded_channel();
    let (live_tx, live_rx) = mpsc::unbounded_channel();
    let streams = crate::stream::StreamDispatcher::new(
        vec![
            (
                Box::new(Sink {
                    db: Arc::clone(&db),
                    tx,
                }),
                true,
            ),
            (
                Box::new(Sink {
                    db: Arc::clone(&db),
                    tx: live_tx,
                }),
                false,
            ),
        ],
        32,
    );
    Ok((
        IngestionContext {
            config,
            db,
            factories,
            handlers: Arc::new(crate::handler::HandlerRegistry::new(vec![Box::new(
                Recorder { fail },
            )])),
            transfer_handlers: Arc::new(crate::handler::TransferRegistry::new(vec![])),
            call_handlers: Arc::new(crate::handler::CallRegistry::new(vec![])),
            metrics: Arc::new(crate::metrics::SieveMetrics::new()),
            stop_rx,
            stream_dispatcher: Some(Arc::new(streams)),
            is_backfill: false,
            receipt_tables: Arc::new(HashSet::new()),
            bloom_filter: None,
            verbose: true,
            worker_count: 2,
            peer_count: Arc::new(|| 0),
        },
        rx,
        live_rx,
    ))
}

async fn messages<T>(mut rx: mpsc::UnboundedReceiver<T>) -> eyre::Result<Vec<T>> {
    Ok(
        tokio::time::timeout(std::time::Duration::from_secs(3), async move {
            let mut result = Vec::new();
            while let Some(message) = rx.recv().await {
                result.push(message);
            }
            result
        })
        .await?,
    )
}

#[tokio::test]
#[ignore = "requires isolated SIEVE_PHASE2_DATABASE_URL; run serially"]
async fn source_equivalence_recovery_and_rollback() -> eyre::Result<()> {
    let mut expected = None;
    for archive in [false, true] {
        let (ctx, rx, live_rx) = context(false).await?;
        let mut payloads = fixture()?;
        let headers: Vec<_> = payloads.iter().map(|p| p.header().clone()).collect();
        let proof = evidence(&headers)?;
        let segment = if archive {
            AuthenticatedSegment::archive(proof.clone(), headers.iter().cloned().map(Ok), 100, 102)?
        } else {
            AuthenticatedSegment::<BaseChain>::peer(Arc::new(super::canonical::fixture_chain(
                headers[..3].to_vec(),
            )?))
        };
        let pipeline = IngestionPipeline::start(ctx.clone(), segment).await?;
        let tx = pipeline.sender();
        // Later child event arrives before its factory creation. Same-block
        // child events, the following block, and an empty block all commit.
        payloads.truncate(3);
        payloads.swap(0, 1);
        for payload in payloads {
            tx.send(FetchItem::Payload(Box::new(
                ValidatedPayload::new(payload).map_err(|e| eyre::eyre!(e))?,
            )))
            .await
            .map_err(|_| eyre::eyre!("pipeline closed"))?;
        }
        drop(tx);
        let outcome = pipeline.finish().await?;
        assert_eq!(outcome.events_stored, 2);
        assert_eq!(
            ctx.db
                .last_checkpoint()
                .await?
                .map(crate::types::BlockNumber::as_u64),
            Some(102)
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _sieve_factory_children")
                .fetch_one(ctx.db.pool())
                .await?,
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT covered_through FROM _sieve_factories")
                .fetch_one(ctx.db.pool())
                .await?,
            102
        );
        let rows: Vec<(i64, Vec<u8>, String)> = sqlx::query_as(
            "SELECT block_number, block_hash, value FROM phase2_events ORDER BY block_number",
        )
        .fetch_all(ctx.db.pool())
        .await?;
        if let Some(expected) = &expected {
            assert_eq!(&rows, expected);
        } else {
            expected = Some(rows);
        }
        let state = super::canonical::committed_frontier_status(&ctx.db).await?;
        if archive {
            check_archive_recovery_and_rollback(&ctx, state, &headers).await?;
        } else {
            assert!(matches!(
                state,
                super::canonical::FrontierStatus::Committed {
                    checkpoint: 102,
                    ..
                }
            ));
        }
        drop(ctx);
        let notifications = messages(rx).await?;
        assert_eq!(
            notifications.iter().map(|n| n.0).collect::<Vec<_>>(),
            vec![100, 101]
        );
        assert!(notifications
            .iter()
            .all(|(block, checkpoint)| checkpoint >= block));
        assert_eq!(messages(live_rx).await?.len(), if archive { 0 } else { 2 });
    }
    Ok(())
}

async fn check_archive_recovery_and_rollback(
    ctx: &IngestionContext,
    state: super::canonical::FrontierStatus,
    headers: &[Header],
) -> eyre::Result<()> {
    let super::canonical::FrontierStatus::Archive(frontier) = state else {
        eyre::bail!("archive provenance lost")
    };
    verify_archive_recovery(&frontier, &Reader(headers.to_vec()))?;
    let sync = sync_context(ctx);
    let policy = super::canonical::QuorumPolicy {
        peer_wait: std::time::Duration::ZERO,
        rounds: 1,
        ..Default::default()
    };
    assert!(super::engine::verify_or_recover_frontier(&sync, &policy)
        .await
        .is_err());
    super::engine::verify_or_recover_frontier_with_archive(
        &sync,
        &policy,
        Some(&Reader(headers.to_vec())),
    )
    .await?;
    drop(sync);
    let mut bad = headers.to_vec();
    bad[3].extra_data = Bytes::from_static(b"tampered");
    assert!(verify_archive_recovery(&frontier, &Reader(bad)).is_err());
    assert!(verify_archive_recovery(&frontier, &Reader(headers[..3].to_vec())).is_err());
    check_peer_tail_and_rollback(ctx, frontier, headers).await
}

async fn check_peer_tail_and_rollback(
    ctx: &IngestionContext,
    frontier: ArchiveFrontier,
    headers: &[Header],
) -> eyre::Result<()> {
    // A later peer segment retains the archive floor, and rollback to
    // that exact floor restores archive startup semantics.
    let peer = AuthenticatedSegment::<BaseChain>::peer(Arc::new(super::canonical::fixture_chain(
        headers[3..].to_vec(),
    )?));
    let pipeline = IngestionPipeline::start(ctx.clone(), peer).await?;
    let tx = pipeline.sender();
    for payload in fixture()?.into_iter().skip(3) {
        tx.send(FetchItem::Payload(Box::new(
            ValidatedPayload::new(payload).map_err(|e| eyre::eyre!(e))?,
        )))
        .await
        .map_err(|_| eyre::eyre!("pipeline closed"))?;
    }
    drop(tx);
    pipeline.finish().await?;
    assert!(matches!(
        super::canonical::committed_frontier_status(&ctx.db).await?,
        super::canonical::FrontierStatus::Committed {
            checkpoint: 104,
            ..
        }
    ));
    assert_eq!(
        crate::db::verification::archive_frontier(&ctx.db).await?,
        Some(frontier)
    );
    // A peer-backed tail still needs quorum, even with valid archive evidence.
    let sync = sync_context(ctx);
    let policy = super::canonical::QuorumPolicy {
        peer_wait: std::time::Duration::ZERO,
        rounds: 1,
        ..Default::default()
    };
    assert!(super::engine::verify_or_recover_frontier_with_archive(
        &sync,
        &policy,
        Some(&Reader(headers.to_vec()))
    )
    .await
    .is_err());
    drop(sync);
    // Refusal happens before speculative factory children are removed.
    assert!(super::follow::rollback_to_ancestor(
        &ctx.db,
        &ctx.handlers,
        &ctx.transfer_handlers,
        &ctx.call_handlers,
        &ctx.config,
        99
    )
    .await
    .is_err());
    assert!(ctx.config.contract_for_address(&CHILD).is_some());
    let mut transaction = ctx.db.begin().await?;
    assert!(
        crate::db::rollback_to(&mut transaction, crate::types::BlockNumber::new(101))
            .await
            .is_err()
    );
    transaction.rollback().await?;
    let mut transaction = ctx.db.begin().await?;
    crate::db::rollback_to(&mut transaction, crate::types::BlockNumber::new(102)).await?;
    transaction.commit().await?;
    assert!(matches!(
        super::canonical::committed_frontier_status(&ctx.db).await?,
        super::canonical::FrontierStatus::Archive(_)
    ));
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated SIEVE_PHASE2_DATABASE_URL; run serially"]
async fn failed_commit_restores_factories_and_publishes_nothing() -> eyre::Result<()> {
    let (ctx, rx, live_rx) = context(true).await?;
    let payloads = fixture()?;
    let headers: Vec<_> = payloads.iter().map(|p| p.header().clone()).collect();
    let segment =
        AuthenticatedSegment::archive(evidence(&headers)?, headers.into_iter().map(Ok), 100, 102)?;
    let pipeline = IngestionPipeline::start(ctx.clone(), segment).await?;
    let tx = pipeline.sender();
    for payload in payloads.into_iter().take(3) {
        tx.send(FetchItem::Payload(Box::new(
            ValidatedPayload::new(payload).map_err(|e| eyre::eyre!(e))?,
        )))
        .await
        .map_err(|_| eyre::eyre!("pipeline closed"))?;
    }
    drop(tx);
    assert!(pipeline.finish().await.is_err());
    assert!(ctx.config.contract_for_address(&CHILD).is_none());
    assert!(!ctx.db.has_indexed_state().await?);
    assert!(crate::db::verification::archive_frontier(&ctx.db)
        .await?
        .is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM phase2_events")
            .fetch_one(ctx.db.pool())
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _sieve_factory_children")
            .fetch_one(ctx.db.pool())
            .await?,
        0
    );
    drop(ctx);
    assert!(messages(rx).await?.is_empty());
    assert!(messages(live_rx).await?.is_empty());
    Ok(())
}

#[test]
fn archive_authentication_rejects_bad_or_incomplete_evidence() -> eyre::Result<()> {
    let payloads = fixture()?;
    let headers: Vec<_> = payloads.iter().map(|p| p.header().clone()).collect();
    let proof = evidence(&headers)?;
    let frontier = ArchiveFrontier {
        block: 102,
        hash: SealedHeader::seal_slow(headers[2].clone()).hash(),
        evidence: proof.clone(),
    };
    verify_archive_recovery(&frontier, &Reader(headers.clone()))?;
    let mut bad_anchor = proof.clone();
    bad_anchor.anchor_hash = B256::ZERO;
    assert!(
        AuthenticatedSegment::archive(bad_anchor, headers.iter().cloned().map(Ok), 100, 102)
            .is_err()
    );
    let mut wrong_chain = proof.clone();
    wrong_chain.genesis_hash = B256::ZERO;
    assert!(
        AuthenticatedSegment::archive(wrong_chain, headers.iter().cloned().map(Ok), 100, 102)
            .is_err()
    );
    assert!(AuthenticatedSegment::archive(
        proof.clone(),
        headers[..4].iter().cloned().map(Ok),
        100,
        102
    )
    .is_err());
    let mut unordered = headers.clone();
    unordered.swap(0, 1);
    assert!(
        AuthenticatedSegment::archive(proof.clone(), unordered.into_iter().map(Ok), 100, 102)
            .is_err()
    );
    assert!(AuthenticatedSegment::archive(proof, headers.into_iter().map(Ok), 99, 102).is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated SIEVE_PHASE2_DATABASE_URL; run serially"]
async fn rejected_inputs_cannot_discover_children_or_skip_gaps() -> eyre::Result<()> {
    for case in ["foreign_header", "unauthorized_skip", "missing_predecessor"] {
        let (ctx, rx, live_rx) = context(false).await?;
        let mut payloads = fixture()?;
        let headers: Vec<_> = payloads.iter().map(|p| p.header().clone()).collect();
        let segment = AuthenticatedSegment::archive(
            evidence(&headers)?,
            headers.iter().cloned().map(Ok),
            100,
            102,
        )?;
        let pipeline = IngestionPipeline::start(ctx.clone(), segment).await?;
        let tx = pipeline.sender();
        let item = match case {
            "foreign_header" => {
                let p = payloads.remove(0);
                let mut header = p.header().clone();
                header.extra_data = Bytes::from_static(b"other branch");
                FetchItem::Payload(Box::new(
                    ValidatedPayload::new(BlockPayload::new(
                        header,
                        p.body().clone(),
                        p.receipts().to_vec(),
                    ))
                    .map_err(|e| eyre::eyre!(e))?,
                ))
            }
            "unauthorized_skip" => FetchItem::Skipped(super::SkippedHeader {
                number: 100,
                hash: SealedHeader::seal_slow(headers[0].clone()).hash(),
                parent_hash: headers[0].parent_hash,
            }),
            _ => FetchItem::Payload(Box::new(
                ValidatedPayload::new(payloads.remove(1)).map_err(|e| eyre::eyre!(e))?,
            )),
        };
        tx.send(item)
            .await
            .map_err(|_| eyre::eyre!("pipeline closed"))?;
        drop(tx);
        assert!(pipeline.finish().await.is_err(), "accepted {case}");
        assert!(ctx.config.contract_for_address(&CHILD).is_none());
        assert!(!ctx.db.has_indexed_state().await?);
        assert!(crate::db::verification::archive_frontier(&ctx.db)
            .await?
            .is_none());
        drop(ctx);
        assert!(messages(rx).await?.is_empty());
        assert!(messages(live_rx).await?.is_empty());
    }
    Ok(())
}

#[test]
fn payload_commitment_gate_rejects_corrupted_base_receipts() -> eyre::Result<()> {
    let payload = fixture()?.remove(0);
    assert!(ValidatedPayload::<BaseChain>::new(BlockPayload::new(
        payload.header().clone(),
        payload.body().clone(),
        vec![]
    ))
    .is_err());
    Ok(())
}

fn sync_context(ctx: &IngestionContext) -> super::SyncContext<BaseChain> {
    super::SyncContext {
        pool: Arc::new(crate::p2p::PeerPool::new_empty()),
        config: Arc::clone(&ctx.config),
        db: Arc::clone(&ctx.db),
        handlers: Arc::clone(&ctx.handlers),
        metrics: Arc::clone(&ctx.metrics),
        stop_rx: ctx.stop_rx.clone(),
        factories: Arc::clone(&ctx.factories),
        transfer_handlers: Arc::clone(&ctx.transfer_handlers),
        call_handlers: Arc::clone(&ctx.call_handlers),
        stream_dispatcher: ctx.stream_dispatcher.clone(),
        is_backfill: ctx.is_backfill,
        receipt_tables: Arc::clone(&ctx.receipt_tables),
        bloom_filter: ctx.bloom_filter.clone(),
        head_seen_rx: None,
        verbose: true,
        worker_count: 2,
    }
}

#[tokio::test]
#[ignore = "requires isolated SIEVE_PHASE2_DATABASE_URL; run serially"]
async fn authenticated_bloom_skips_commit_all_hashes() -> eyre::Result<()> {
    let (mut ctx, rx, live_rx) = context(false).await?;
    ctx.factories = Arc::new(vec![]);
    ctx.bloom_filter = Some(Arc::new(crate::filter::BloomFilter::new(vec![CHILD])));
    let headers: Vec<_> = fixture()?.iter().map(|p| p.header().clone()).collect();
    let segment = AuthenticatedSegment::<BaseChain>::peer(Arc::new(
        super::canonical::fixture_chain(headers[2..].to_vec())?,
    ));
    let pipeline = IngestionPipeline::start(ctx.clone(), segment).await?;
    let tx = pipeline.sender();
    for header in headers[2..].iter().rev() {
        tx.send(FetchItem::Skipped(super::SkippedHeader {
            number: header.number,
            hash: SealedHeader::seal_slow(header.clone()).hash(),
            parent_hash: header.parent_hash,
        }))
        .await
        .map_err(|_| eyre::eyre!("pipeline closed"))?;
    }
    drop(tx);
    pipeline.finish().await?;
    assert_eq!(
        ctx.db
            .last_checkpoint()
            .await?
            .map(crate::types::BlockNumber::as_u64),
        Some(104)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _sieve_block_hashes")
            .fetch_one(ctx.db.pool())
            .await?,
        3
    );
    assert_eq!(
        ctx.db.verified_frontier().await?,
        Some((104, SealedHeader::seal_slow(headers[4].clone()).hash()))
    );
    drop(ctx);
    assert!(messages(rx).await?.is_empty());
    assert!(messages(live_rx).await?.is_empty());
    Ok(())
}

/// Context indexing `config` (resolved against `dir`) into freshly created tables.
pub async fn configured_context(
    dir: &std::path::Path,
    first_block: u64,
    config: &crate::toml_config::SieveConfig,
) -> eyre::Result<IngestionContext> {
    use crate::handler::{
        CallHandler, CallRegistry, ConfigDrivenHandler, HandlerRegistry, TransferHandler,
        TransferRegistry,
    };
    let (mut ctx, _, _) = context(false).await?;
    let resolved = crate::toml_config::resolve_config(config, dir)?;
    for (table, ddl) in resolved
        .resolved_events
        .iter()
        .map(|e| (&e.table_name, &e.create_table_sql))
        .chain(
            resolved
                .calls
                .iter()
                .map(|e| (&e.table_name, &e.create_table_sql)),
        )
        .chain(
            resolved
                .transfers
                .iter()
                .map(|e| (&e.table_name, &e.create_table_sql)),
        )
    {
        sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
            .execute(ctx.db.pool())
            .await?;
        sqlx::query(ddl).execute(ctx.db.pool()).await?;
    }
    sqlx::query("TRUNCATE _sieve_factories")
        .execute(ctx.db.pool())
        .await?;
    crate::db::ensure_factory_coverage(&ctx.db, &resolved.factories, first_block, false).await?;
    ctx.receipt_tables = Arc::new(crate::build_receipt_tables(
        &resolved.resolved_events,
        &resolved.transfers,
        &resolved.calls,
    ));
    ctx.handlers = Arc::new(HandlerRegistry::new(
        resolved
            .resolved_events
            .into_iter()
            .map(|e| Box::new(ConfigDrivenHandler::new(e)) as Box<dyn EventHandler>)
            .collect(),
    ));
    ctx.call_handlers = Arc::new(CallRegistry::new(
        resolved.calls.into_iter().map(CallHandler::new).collect(),
    ));
    ctx.transfer_handlers = Arc::new(TransferRegistry::new(
        resolved
            .transfers
            .into_iter()
            .map(TransferHandler::new)
            .collect(),
    ));
    ctx.config = Arc::new(resolved.index_config);
    ctx.factories = Arc::new(resolved.factories);
    ctx.stream_dispatcher = None;
    ctx.is_backfill = true;
    Ok(ctx)
}

/// `(table, event, Some(count))` per block summary entry, `(table, event, None)` per payload.
type Delivery = (String, String, Option<u64>);

struct Capture(mpsc::UnboundedSender<Delivery>);
#[async_trait::async_trait]
impl crate::stream::StreamSink for Capture {
    fn name(&self) -> &'static str {
        "capture"
    }
    async fn notify(&self, n: &crate::stream::BlockNotification) {
        for t in &n.tables {
            let _ = self
                .0
                .send((t.name.clone(), t.event.clone(), Some(t.count)));
        }
    }
    async fn notify_events(&self, events: &[crate::stream::EventPayload]) {
        for e in events {
            let _ = self.0.send((e.table.clone(), e.event.clone(), None));
        }
    }
}

/// Overload-fixture context whose stream deliveries are captured.
async fn overload_context(
    file: &str,
) -> eyre::Result<(IngestionContext, mpsc::UnboundedReceiver<Delivery>)> {
    let dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/event-overloads");
    let config = crate::toml_config::load_config(&dir.join(file))?;
    let mut ctx = configured_context(&dir, 100, &config).await?;
    let (tx, rx) = mpsc::unbounded_channel();
    ctx.stream_dispatcher = Some(Arc::new(crate::stream::StreamDispatcher::new(
        vec![(Box::new(Capture(tx)), true)],
        32,
    )));
    Ok((ctx, rx))
}

/// Ingest `payloads` (any order) through a peer-authenticated segment.
async fn ingest(
    ctx: &IngestionContext,
    payloads: Vec<BlockPayload<BaseChain>>,
    headers: &[Header],
) -> eyre::Result<super::engine::SyncOutcome> {
    let segment = AuthenticatedSegment::<BaseChain>::peer(Arc::new(
        super::canonical::fixture_chain(headers.to_vec())?,
    ));
    let pipeline = IngestionPipeline::start(ctx.clone(), segment).await?;
    let tx = pipeline.sender();
    for payload in payloads {
        tx.send(FetchItem::Payload(Box::new(
            ValidatedPayload::new(payload).map_err(|e| eyre::eyre!(e))?,
        )))
        .await
        .map_err(|_| eyre::eyre!("pipeline closed"))?;
    }
    drop(tx);
    pipeline.finish().await
}

/// `(table, block_number, log_index, columns joined as text)` rows per table.
async fn table_rows(
    db: &crate::db::Database,
    tables: &[(&str, &str)],
) -> eyre::Result<Vec<(String, i64, i32, String)>> {
    let mut rows = Vec::new();
    for (table, columns) in tables {
        let sql = format!(
            "SELECT '{table}', block_number, log_index, concat_ws(',', {columns}) \
             FROM {table} ORDER BY block_number, log_index"
        );
        rows.extend(sqlx::query_as(&sql).fetch_all(db.pool()).await?);
    }
    Ok(rows)
}

/// Sorted deliveries: one summary entry (count 1) and one payload per `(table, event)`.
fn deliveries_once_per_table(targets: &[(&str, &str)]) -> Vec<Delivery> {
    let mut deliveries: Vec<Delivery> = targets
        .iter()
        .flat_map(|&(table, event)| {
            [None, Some(1)].map(|count| (table.to_owned(), event.to_owned(), count))
        })
        .collect();
    deliveries.sort();
    deliveries
}

const CL_COLUMNS: &str = r#""user", token_id::text, liquidity_to_stake::text"#;
const CLASSIC_DEPOSIT_COLUMNS: &str = r#""from", "to", amount::text"#;
const CLASSIC_WITHDRAW_COLUMNS: &str = r#""from", amount::text"#;

#[tokio::test]
#[ignore = "requires isolated SIEVE_PHASE2_DATABASE_URL; run serially"]
async fn overloaded_events_store_one_row_per_selector() -> eyre::Result<()> {
    use crate::toml_config::event_selection_tests::{gauge_logs, FROM, TO, USER};
    let (ctx, rx) = overload_context("sieve.toml").await?;
    let payloads = chain(vec![gauge_logs(ctx.config.contracts[0].address)]);
    let headers: Vec<_> = payloads.iter().map(|p| p.header().clone()).collect();
    let tables = [
        ("gauge_cl_deposit", CL_COLUMNS),
        ("gauge_classic_deposit", CLASSIC_DEPOSIT_COLUMNS),
        ("gauge_cl_withdraw", CL_COLUMNS),
        ("gauge_classic_withdraw", CLASSIC_WITHDRAW_COLUMNS),
    ];
    let (user, from, to) = (
        USER.to_checksum(None),
        FROM.to_checksum(None),
        TO.to_checksum(None),
    );
    let expected = [
        ("gauge_cl_deposit", 0, format!("{user},7,1000")),
        ("gauge_classic_deposit", 1, format!("{from},{to},500")),
        ("gauge_cl_withdraw", 2, format!("{user},7,1000")),
        ("gauge_classic_withdraw", 3, format!("{from},250")),
    ]
    .map(|(table, log, columns)| (table.to_owned(), 100, log, columns));
    // Replaying the block over reset sync state stores nothing twice.
    for run in 0..2 {
        if run == 1 {
            sqlx::raw_sql(RESET_SYNC_STATE)
                .execute(ctx.db.pool())
                .await?;
        }
        assert_eq!(
            ingest(&ctx, payloads.clone(), &headers)
                .await?
                .events_stored,
            4
        );
        assert_eq!(table_rows(&ctx.db, &tables).await?, expected);
    }
    super::follow::rollback_to_ancestor(
        &ctx.db,
        &ctx.handlers,
        &ctx.transfer_handlers,
        &ctx.call_handlers,
        &ctx.config,
        99,
    )
    .await?;
    assert!(table_rows(&ctx.db, &tables).await?.is_empty());
    drop(ctx);
    let mut deliveries = messages(rx).await?;
    deliveries.sort();
    let once = deliveries_once_per_table(&[
        ("gauge_cl_deposit", "Deposit"),
        ("gauge_classic_deposit", "Deposit"),
        ("gauge_cl_withdraw", "Withdraw"),
        ("gauge_classic_withdraw", "Withdraw"),
    ]);
    let twice: Vec<_> = once.iter().flat_map(|d| [d.clone(), d.clone()]).collect();
    assert_eq!(deliveries, twice);
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated SIEVE_PHASE2_DATABASE_URL; run serially"]
async fn factory_children_route_overloads_and_fan_out() -> eyre::Result<()> {
    use crate::toml_config::event_selection_tests::{gauge_logs, FROM, TO, USER};
    let (ctx, rx) = overload_context("factory.toml").await?;
    let voter = ctx.factories[0].factory_address;
    let created = |gauge: Address| {
        crate::test_utils::make_log(
            voter,
            vec![
                ctx.factories[0].creation_selector,
                B256::left_padding_from(gauge.as_slice()),
            ],
            Bytes::new(),
        )
    };
    let (cl, classic) = (Address::repeat_byte(0x41), Address::repeat_byte(0x42));
    let [cl_deposit, _, cl_withdraw, _] =
        <[_; 4]>::try_from(gauge_logs(cl)).map_err(|_| eyre::eyre!("four gauge logs"))?;
    let [_, classic_deposit, _, classic_withdraw] =
        <[_; 4]>::try_from(gauge_logs(classic)).map_err(|_| eyre::eyre!("four gauge logs"))?;
    let mut payloads = chain(vec![
        vec![created(cl), cl_deposit.clone(), cl_withdraw],
        vec![created(classic), classic_deposit.clone(), classic_withdraw],
        vec![cl_deposit, classic_deposit],
    ]);
    let headers: Vec<_> = payloads.iter().map(|p| p.header().clone()).collect();
    // Later child events arrive before both creation blocks.
    payloads.rotate_right(1);
    assert_eq!(ingest(&ctx, payloads, &headers).await?.events_stored, 8);

    let (cl, classic) = (cl.to_checksum(None), classic.to_checksum(None));
    let (user, from, to) = (
        USER.to_checksum(None),
        FROM.to_checksum(None),
        TO.to_checksum(None),
    );
    let cl_row = format!("{cl},{user},7,1000");
    let deposit_row = format!("{classic},{from},{to},500");
    let classic_deposit = format!("contract_address, {CLASSIC_DEPOSIT_COLUMNS}");
    let expected = [
        ("gauge_cl_deposit", 100, 1, &cl_row),
        ("gauge_cl_deposit", 102, 0, &cl_row),
        ("gauge_classic_deposit", 101, 1, &deposit_row),
        ("gauge_classic_deposit", 102, 1, &deposit_row),
        ("gauge_classic_deposit_copy", 101, 1, &deposit_row),
        ("gauge_classic_deposit_copy", 102, 1, &deposit_row),
        ("gauge_cl_withdraw", 100, 2, &cl_row),
        (
            "gauge_classic_withdraw",
            101,
            2,
            &format!("{classic},{from},250"),
        ),
    ]
    .map(|(table, block, log, columns)| (table.to_owned(), block, log, columns.clone()));
    let cl_columns = format!("contract_address, {CL_COLUMNS}");
    let tables = [
        ("gauge_cl_deposit", cl_columns.as_str()),
        ("gauge_classic_deposit", classic_deposit.as_str()),
        ("gauge_classic_deposit_copy", classic_deposit.as_str()),
        ("gauge_cl_withdraw", cl_columns.as_str()),
        (
            "gauge_classic_withdraw",
            &format!("contract_address, {CLASSIC_WITHDRAW_COLUMNS}"),
        ),
    ];
    assert_eq!(table_rows(&ctx.db, &tables).await?, expected);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _sieve_factory_children")
            .fetch_one(ctx.db.pool())
            .await?,
        2
    );
    drop(ctx);

    // Block 101 fans one classic Deposit out to two tables, each counted and streamed once.
    let mut deliveries = messages(rx).await?;
    deliveries.sort();
    let mut expected = [
        deliveries_once_per_table(&[
            ("gauge_cl_deposit", "Deposit"),
            ("gauge_cl_withdraw", "Withdraw"),
        ]),
        deliveries_once_per_table(&[
            ("gauge_classic_deposit", "Deposit"),
            ("gauge_classic_deposit_copy", "Deposit"),
            ("gauge_classic_withdraw", "Withdraw"),
        ]),
        deliveries_once_per_table(&[
            ("gauge_cl_deposit", "Deposit"),
            ("gauge_classic_deposit", "Deposit"),
            ("gauge_classic_deposit_copy", "Deposit"),
        ]),
    ]
    .concat();
    expected.sort();
    assert_eq!(deliveries, expected);
    Ok(())
}

/// Captures every streamed payload.
struct Payloads(mpsc::UnboundedSender<crate::stream::EventPayload>);
#[async_trait::async_trait]
impl crate::stream::StreamSink for Payloads {
    fn name(&self) -> &'static str {
        "payloads"
    }
    async fn notify(&self, _: &crate::stream::BlockNotification) {}
    async fn notify_events(&self, events: &[crate::stream::EventPayload]) {
        for e in events {
            let _ = self.0.send(e.clone());
        }
    }
}

/// Every user column of each event and call table, as JSON, sorted like `structured_rows`.
///
/// JSON columns come back as stored; other columns as JSON strings (booleans as booleans).
async fn stored_rows(
    db: &crate::db::Database,
    resolved: &crate::toml_config::ResolvedConfig,
) -> eyre::Result<Vec<crate::decode::tests::Row>> {
    let tables = resolved
        .resolved_events
        .iter()
        .map(|e| (&e.table_name, &e.columns, "log_index", "log_index"))
        .chain(
            resolved
                .calls
                .iter()
                .map(|c| (&c.table_name, &c.columns, "-1", "tx_index")),
        );
    let mut rows = Vec::new();
    for (table, columns, log_index, order) in tables {
        let exprs: Vec<String> = columns
            .iter()
            .map(|c| match c.pg_type.as_str() {
                "jsonb" => format!("\"{}\"", c.column_name),
                "boolean" => format!("to_jsonb(\"{}\")", c.column_name),
                _ => format!("to_jsonb(\"{}\"::text)", c.column_name),
            })
            .collect();
        let sql = format!(
            "SELECT {log_index}, jsonb_build_array({}) FROM {table} ORDER BY block_number, {order}",
            exprs.join(", ")
        );
        let fetched: Vec<(i32, sqlx::types::Json<serde_json::Value>)> =
            sqlx::query_as(&sql).fetch_all(db.pool()).await?;
        for (log, sqlx::types::Json(values)) in fetched {
            let serde_json::Value::Array(values) = values else {
                return Err(eyre::eyre!("{table}: expected a JSON array row"));
            };
            let mut row: Vec<(String, serde_json::Value)> = columns
                .iter()
                .map(|c| c.column_name.clone())
                .zip(values)
                .collect();
            // The explicit `type = "text"` column holds serialized JSON text.
            if table == "gauges_allocated_text" {
                let serde_json::Value::String(text) = &row[1].1 else {
                    return Err(eyre::eyre!("allocations text column is not text"));
                };
                row[1].1 = serde_json::from_str(text)?;
            }
            rows.push((table.clone(), log, row));
        }
    }
    rows.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
    Ok(rows)
}

/// Run a GraphQL query with variables; errors fail the call.
async fn graphql(
    schema: &async_graphql::dynamic::Schema,
    query: &str,
    variables: serde_json::Value,
) -> eyre::Result<serde_json::Value> {
    let response = schema
        .execute(
            async_graphql::Request::new(query)
                .variables(async_graphql::Variables::from_json(variables)),
        )
        .await;
    eyre::ensure!(response.errors.is_empty(), "{:?}", response.errors);
    Ok(response.data.into_json()?)
}

/// Structured fixture: resolved config, configured context, and its block 100 payload.
async fn structured_context() -> eyre::Result<(
    crate::toml_config::ResolvedConfig,
    IngestionContext,
    Vec<BlockPayload<BaseChain>>,
)> {
    use crate::decode::tests::{allocate_gauges_call, structured_dir, structured_logs};
    let dir = structured_dir();
    let config = crate::toml_config::load_config(&dir.join("sieve.toml"))?;
    let resolved = crate::toml_config::resolve_config(&config, &dir)?;
    let ctx = configured_context(&dir, 100, &config).await?;
    let payloads = chain_with_calls(vec![(structured_logs(), Some(allocate_gauges_call()))]);
    Ok((resolved, ctx, payloads))
}

#[tokio::test]
#[ignore = "requires isolated SIEVE_PHASE2_DATABASE_URL; run serially"]
#[expect(
    clippy::too_many_lines,
    reason = "storage, replay, GraphQL, rollback and stream assertions"
)]
async fn structured_values_store_and_stream_as_json() -> eyre::Result<()> {
    use crate::decode::tests::structured_rows;
    let (resolved, mut ctx, payloads) = structured_context().await?;
    let (tx, rx) = mpsc::unbounded_channel();
    ctx.stream_dispatcher = Some(Arc::new(crate::stream::StreamDispatcher::new(
        vec![(Box::new(Payloads(tx)), true)],
        32,
    )));
    let headers: Vec<_> = payloads.iter().map(|p| p.header().clone()).collect();
    let expected = structured_rows();
    // Replaying the block over reset sync state stores nothing twice.
    for run in 0..2 {
        if run == 1 {
            sqlx::raw_sql(RESET_SYNC_STATE)
                .execute(ctx.db.pool())
                .await?;
        }
        let outcome = ingest(&ctx, payloads.clone(), &headers).await?;
        assert_eq!((outcome.events_stored, outcome.calls_stored), (9, 1));
        assert_eq!(stored_rows(&ctx.db, &resolved).await?, expected);
    }
    let scalar = |sql: &'static str| sqlx::query_scalar::<_, String>(sql).fetch_one(ctx.db.pool());
    assert_eq!(
        scalar(
            "SELECT allocations->0->>'allocated' FROM gauges_allocated ORDER BY log_index LIMIT 1"
        )
        .await?,
        "1000"
    );
    assert_eq!(
        scalar("SELECT param_4::text FROM pool_created").await?,
        "17"
    );
    assert_eq!(
        scalar("SELECT pg_typeof(allocations)::text FROM gauges_allocated_text LIMIT 1").await?,
        "text"
    );

    let schema = crate::api::build_schema(
        &resolved.resolved_events,
        &resolved.transfers,
        &resolved.calls,
        ctx.db.pool().clone(),
    )?;
    let page = |after: &str| {
        format!(
            "{{ gauges_allocated(orderBy: allocations, orderDirection: ASC, first: 1{after}) \
             {{ nodes {{ token_id allocations }} pageInfo {{ hasNextPage endCursor }} }} \
             pool_created(first: 2) {{ nodes {{ param_4 }} }} }}"
        )
    };
    let allocations = |token: &str| {
        expected
            .iter()
            .find(|(table, _, cols)| table == "gauges_allocated" && cols[0].1 == token)
            .map(|(_, _, cols)| cols[1].1.clone())
            .ok_or_else(|| eyre::eyre!("no expected row for token {token}"))
    };
    // Text order follows the database collation; two pages must cover both rows once.
    let mut pages = Vec::new();
    let mut after = String::new();
    for has_next in [true, false] {
        let result = graphql(&schema, &page(&after), serde_json::json!({})).await?;
        assert_eq!(result["pool_created"]["nodes"][0]["param_4"], "17");
        let connection = &result["gauges_allocated"];
        assert_eq!(connection["pageInfo"]["hasNextPage"], has_next);
        let node = &connection["nodes"][0];
        let (Some(token), Some(text)) = (node["token_id"].as_str(), node["allocations"].as_str())
        else {
            return Err(eyre::eyre!("expected String fields, got {node}"));
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(text)?,
            allocations(token)?
        );
        pages.push((token.to_owned(), text.to_owned()));
        let cursor = connection["pageInfo"]["endCursor"]
            .as_str()
            .ok_or_else(|| eyre::eyre!("no endCursor"))?;
        after = format!(", after: \"{cursor}\"");
    }
    pages.sort();
    let tokens: Vec<&str> = pages.iter().map(|(token, _)| token.as_str()).collect();
    assert_eq!(tokens, ["7", "8"]);
    let filtered = graphql(
        &schema,
        "query($a: String, $b: String) { \
           eq: gauges_allocated(where: { allocations: $a }) { nodes { token_id } } \
           any: gauges_allocated(where: { allocations_in: [$a, $b] }, orderBy: token_id) \
             { nodes { token_id } } }",
        serde_json::json!({ "a": pages[0].1, "b": pages[1].1 }),
    )
    .await?;
    assert_eq!(
        filtered,
        serde_json::json!({
            "eq": { "nodes": [{ "token_id": "7" }] },
            "any": { "nodes": [{ "token_id": "8" }, { "token_id": "7" }] },
        })
    );

    super::follow::rollback_to_ancestor(
        &ctx.db,
        &ctx.handlers,
        &ctx.transfer_handlers,
        &ctx.call_handlers,
        &ctx.config,
        99,
    )
    .await?;
    assert!(stored_rows(&ctx.db, &resolved).await?.is_empty());
    drop(ctx);

    // Stream data uses raw or generated ABI names and equals the stored JSON.
    let delivered = messages(rx).await?;
    assert_eq!(delivered.len(), 2 * expected.len());
    for payload in &delivered {
        let log = payload.log_index.map_or(-1, |i| i as i32);
        let (_, _, row) = expected
            .iter()
            .find(|(table, l, _)| *table == payload.table && *l == log)
            .ok_or_else(|| eyre::eyre!("unexpected payload {} {log}", payload.table))?;
        let columns = resolved
            .resolved_events
            .iter()
            .map(|e| (&e.table_name, &e.columns))
            .chain(resolved.calls.iter().map(|c| (&c.table_name, &c.columns)))
            .find(|(table, _)| **table == payload.table)
            .map(|(_, columns)| columns)
            .ok_or_else(|| eyre::eyre!("no table {}", payload.table))?;
        for (col, (_, stored)) in columns.iter().zip(row) {
            assert_eq!(
                payload.data.get(&col.param_name),
                Some(stored),
                "{} {}",
                payload.table,
                col.param_name
            );
        }
    }
    let pool = delivered
        .iter()
        .find(|p| p.table == "pool_created")
        .ok_or_else(|| eyre::eyre!("no pool_created payload"))?;
    let mut keys: Vec<&str> = pool.data.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["param_4", "pool", "stable", "token0", "token1"]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires isolated SIEVE_PHASE2_DATABASE_URL; run serially"]
#[expect(
    clippy::too_many_lines,
    reason = "legacy DDL setup, batch and single-row writes, GraphQL reads"
)]
async fn legacy_text_columns_keep_debug_rows_and_take_json_text() -> eyre::Result<()> {
    use crate::decode::tests::{structured_logs, structured_rows};
    use crate::handler::ConfigDrivenHandler;
    const DEBUG_ROW: &str = "Array([Tuple([Address(0x1111111111111111111111111111111111111111), \
                             Uint(1000, 128), Bytes(0x)])])";
    let (resolved, ctx, payloads) = structured_context().await?;
    let event = resolved
        .resolved_events
        .iter()
        .find(|e| e.table_name == "gauges_allocated")
        .ok_or_else(|| eyre::eyre!("no gauges_allocated"))?;
    let call = &resolved.calls[0];
    // v0.3.4 created these columns as text and stored Rust Debug text.
    for (table, ddl, column) in [
        (&event.table_name, &event.create_table_sql, "allocations"),
        (&call.table_name, &call.create_table_sql, "gauges"),
    ] {
        let legacy = ddl.replace(
            &format!("\"{column}\" jsonb"),
            &format!("\"{column}\" text"),
        );
        eyre::ensure!(legacy != *ddl, "{table} DDL has no jsonb {column}");
        sqlx::raw_sql(&format!("DROP TABLE {table}; {legacy}"))
            .execute(ctx.db.pool())
            .await?;
    }
    sqlx::query(
        "INSERT INTO gauges_allocated (block_number, tx_hash, tx_index, log_index, token_id, \
         allocations, emissions_per_vp) VALUES (50, '\\x00', 0, 0, 1, $1, 0)",
    )
    .bind(DEBUG_ROW)
    .execute(ctx.db.pool())
    .await?;
    sqlx::query(
        "INSERT INTO allocate_gauges_calls (block_number, tx_hash, tx_index, token_id, gauges) \
         VALUES (50, '\\x00', 0, 1, $1)",
    )
    .bind(DEBUG_ROW)
    .execute(ctx.db.pool())
    .await?;
    // Startup DDL leaves existing tables alone.
    crate::db::create_user_tables(&ctx.db, &resolved.resolved_events).await?;
    crate::db::create_call_tables(&ctx.db, &resolved.calls).await?;

    // Batch path: block 100 through the pipeline.
    let headers: Vec<_> = payloads.iter().map(|p| p.header().clone()).collect();
    ingest(&ctx, payloads, &headers).await?;
    assert_eq!(
        ctx.db.last_checkpoint().await?,
        Some(crate::types::BlockNumber::new(100))
    );

    // Single-row path: block 101 through the handler's own INSERT.
    let log = structured_logs()[1].clone();
    let contract = ctx
        .config
        .contract_for_address(&log.address)
        .ok_or_else(|| eyre::eyre!("no LeafVoter"))?;
    let decoded = crate::decode::decode_log(
        &crate::filter::FilteredLog {
            log,
            block_number: crate::types::BlockNumber::new(101),
            block_timestamp: 0,
            tx_hash: B256::ZERO,
            tx_index: crate::types::TxIndex::new(0),
            log_index: crate::types::LogIndex::new(0),
        },
        contract,
    )?;
    let context = EventContext {
        block_timestamp: 0,
        block_hash: B256::ZERO,
        tx_from: Address::ZERO,
        tx_to: None,
        tx_value: U256::ZERO,
        tx_gas_price: 0,
        tx_gas_used: 0,
        tx_nonce: 0,
        cumulative_gas_used: 0,
        tx_status: true,
    };
    let mut tx = ctx.db.begin().await?;
    ConfigDrivenHandler::new(event.clone())
        .handle(&decoded, &context, &mut tx)
        .await?;
    tx.commit().await?;

    let expected = structured_rows();
    let allocations = |table: &str, log: i32| {
        expected
            .iter()
            .find(|(t, l, _)| t == table && *l == log)
            .map(|(_, _, cols)| cols[1].1.clone())
            .ok_or_else(|| eyre::eyre!("no expected {table} {log}"))
    };
    let stored: Vec<(i64, String)> = sqlx::query_as(
        "SELECT block_number, allocations FROM gauges_allocated ORDER BY block_number, log_index",
    )
    .fetch_all(ctx.db.pool())
    .await?;
    assert_eq!(stored.len(), 4);
    assert_eq!(stored[0], (50, DEBUG_ROW.to_owned()));
    for ((block, text), log) in stored[1..].iter().zip([1, 2, 1]) {
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(text)?,
            allocations("gauges_allocated", log)?,
            "block {block}"
        );
    }
    let calls: Vec<(i64, String)> = sqlx::query_as(
        "SELECT block_number, gauges FROM allocate_gauges_calls ORDER BY block_number",
    )
    .fetch_all(ctx.db.pool())
    .await?;
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0], (50, DEBUG_ROW.to_owned()));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&calls[1].1)?,
        allocations("allocate_gauges_calls", -1)?
    );

    // GraphQL reads legacy Debug and JSON text rows as Strings, filtering and sorting as text.
    let schema = crate::api::build_schema(
        &resolved.resolved_events,
        &resolved.transfers,
        &resolved.calls,
        ctx.db.pool().clone(),
    )?;
    let result = graphql(
        &schema,
        "query($debug: String) { \
           sorted: gauges_allocated(orderBy: allocations, orderDirection: ASC, first: 1) \
             { nodes { block_number allocations } } \
           debug: allocate_gauges_calls(where: { gauges: $debug }) { nodes { block_number } } }",
        serde_json::json!({ "debug": DEBUG_ROW }),
    )
    .await?;
    assert_eq!(
        result,
        serde_json::json!({
            "sorted": { "nodes": [{ "block_number": "50", "allocations": DEBUG_ROW }] },
            "debug": { "nodes": [{ "block_number": "50" }] },
        })
    );
    Ok(())
}
