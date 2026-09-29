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
use alloy_primitives::{Address, Bytes, Signature, B256, U256};
use reth_chainspec::EthChainSpec;
use reth_primitives_traits::{Header, SealedHeader};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
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
        child_contract_name: "Child".into(),
        start_block: 100,
    })
}

fn fixture() -> eyre::Result<Vec<BlockPayload<BaseChain>>> {
    let abi: alloy_json_abi::JsonAbi = serde_json::from_str(ABI)?;
    let f = factory()?;
    let mut parent = B256::repeat_byte(3);
    let mut result = Vec::new();
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
        let (transactions, receipts) = if logs.is_empty() {
            (vec![], vec![])
        } else {
            let tx = TxLegacy {
                chain_id: Some(8453),
                nonce: number,
                gas_limit: 21_000,
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
    Ok(result)
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
    fn matches(&self, contract: &str, event: &str) -> bool {
        contract == "Child" && event == "Ping"
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

async fn context(
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
    sqlx::raw_sql("TRUNCATE _sieve_archive_frontier, _sieve_canonical, _sieve_block_hashes, _sieve_factory_children, _sieve_factories; UPDATE _sieve_checkpoints SET block_number = 0; CREATE TABLE IF NOT EXISTS phase2_events (block_number BIGINT, block_hash BYTEA, value TEXT); TRUNCATE phase2_events;").execute(db.pool()).await?;
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
            event_table_map: Arc::new(HashMap::from([(
                "Child:Ping".into(),
                ("phase2_events".into(), "Ping".into()),
            )])),
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

async fn messages(mut rx: mpsc::UnboundedReceiver<(u64, u64)>) -> eyre::Result<Vec<(u64, u64)>> {
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
        event_table_map: Arc::clone(&ctx.event_table_map),
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
