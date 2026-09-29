//! Small independent RPC references and generated jars; not producer snapshot evidence.
#![expect(clippy::panic_in_result_fn, reason = "acceptance assertions")]
use super::{
    manifest::{Archive, OutputFile},
    plan::Group,
    reader::{self, HeaderReader},
    runtime::ArchiveConfig,
    staging::Staging,
};
use crate::{
    chain::{BaseChain, ChainKind, ChainTypes},
    sync::{
        validation::{
            ArchiveEvidence, ArchiveRecoveryReader, AuthenticatedArchive, ValidatedPayload,
        },
        BlockPayload,
    },
};
use alloy_consensus::BlockBody;
use alloy_eips::eip4895::Withdrawals;
use alloy_primitives::{Address, B256, U256};
use eyre::{eyre, Result};
use op_alloy_consensus::{OpReceipt, OpTxEnvelope};
use reth_codecs::Compact;
use reth_nippy_jar::{NippyJar, NippyJarWriter};
use reth_primitives_traits::{Header, SealedHeader, SignerRecoverable};
use reth_static_file_types::{SegmentHeader, SegmentRangeInclusive, StaticFileSegment};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
};
use tokio::sync::watch;

const REFERENCES: [&str; 5] = [
    include_str!("../../tests/fixtures/base-reference/5000000.json"),
    include_str!("../../tests/fixtures/base-reference/9101527.json"),
    include_str!("../../tests/fixtures/base-reference/30008527.json"),
    include_str!("../../tests/fixtures/base-reference/47810527.json"),
    include_str!("../../tests/fixtures/base-reference/51925326.json"),
];

fn reference(raw: &str) -> Result<(Value, BlockPayload<BaseChain>)> {
    let value: Value = serde_json::from_str(raw)?;
    let header: Header = serde_json::from_value(value["block"].clone())?;
    let transactions = serde_json::from_value(value["block"]["transactions"].clone())?;
    let receipts = serde_json::from_value(value["receipts"].clone())?;
    let body = BlockBody {
        transactions,
        withdrawals: header.withdrawals_root.map(|_| Withdrawals::default()),
        ..Default::default()
    };
    Ok((value, BlockPayload::new(header, body, receipts)))
}

const TOKEN: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const TOKEN_ABI: &str = r#"[
 {"type":"event","name":"Transfer","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]},
 {"type":"event","name":"Approval","anonymous":false,"inputs":[{"name":"owner","type":"address","indexed":true},{"name":"spender","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]},
 {"type":"function","name":"transfer","stateMutability":"nonpayable","inputs":[{"name":"recipient","type":"address"},{"name":"amount","type":"uint256"}],"outputs":[{"name":"","type":"bool"}]},
 {"type":"function","name":"approve","stateMutability":"nonpayable","inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],"outputs":[{"name":"","type":"bool"}]}
]"#;

async fn reference_context(jars: &Jars) -> Result<crate::sync::ingestion::IngestionContext> {
    fs::write(jars.root.0.join("token.json"), TOKEN_ABI)?;
    let fields = [
        "block_timestamp",
        "block_hash",
        "tx_from",
        "tx_to",
        "tx_value",
        "tx_gas_price",
    ];
    let config = json!({
        "chain":"base",
        "contracts":[{"name":"USDC", "address":TOKEN, "abi":"token.json", "start_block":0, "include_receipts":true,
            "events":[{"name":"Transfer","table":"phase6_events","context":fields},{"name":"Approval","table":"phase6_approvals","context":fields}],
            "calls":[{"name":"transfer","table":"phase6_calls","context":fields},{"name":"approve","table":"phase6_approve_calls","context":fields}]}],
        "transfers":[{"name":"ETH","table":"phase6_transfers","start_block":0,"context":fields,"include_receipts":true}]
    });
    configured_context(jars, config).await
}

async fn configured_context(
    jars: &Jars,
    config: Value,
) -> Result<crate::sync::ingestion::IngestionContext> {
    use crate::handler::{
        CallHandler, CallRegistry, ConfigDrivenHandler, HandlerRegistry, TransferHandler,
        TransferRegistry,
    };
    let (mut ctx, _, _) = crate::sync::ingestion_tests::context(false).await?;
    let config = serde_json::from_value(config)?;
    let resolved = crate::toml_config::resolve_config(&config, &jars.root.0)?;
    for ddl in resolved
        .resolved_events
        .iter()
        .map(|e| &e.create_table_sql)
        .chain(resolved.calls.iter().map(|e| &e.create_table_sql))
        .chain(resolved.transfers.iter().map(|e| &e.create_table_sql))
    {
        sqlx::query(ddl).execute(ctx.db.pool()).await?;
    }
    let tables = resolved
        .resolved_events
        .iter()
        .map(|e| e.table_name.clone())
        .chain(resolved.calls.iter().map(|e| e.table_name.clone()))
        .chain(resolved.transfers.iter().map(|e| e.table_name.clone()))
        .collect::<Vec<_>>();
    sqlx::query(&format!("TRUNCATE {}", tables.join(", ")))
        .execute(ctx.db.pool())
        .await?;
    sqlx::query("TRUNCATE _sieve_factories")
        .execute(ctx.db.pool())
        .await?;
    crate::db::ensure_factory_coverage(
        &ctx.db,
        &resolved.factories,
        jars.evidence.first_block,
        false,
    )
    .await?;
    ctx.event_table_map = Arc::new(
        resolved
            .resolved_events
            .iter()
            .map(|e| {
                (
                    format!("{}:{}", e.contract_name, e.event_name),
                    (e.table_name.clone(), e.event_name.clone()),
                )
            })
            .collect(),
    );
    ctx.receipt_tables = Arc::new(HashSet::from_iter(tables));
    ctx.handlers = Arc::new(HandlerRegistry::new(
        resolved
            .resolved_events
            .into_iter()
            .map(|e| Box::new(ConfigDrivenHandler::new(e)) as Box<dyn crate::handler::EventHandler>)
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

fn hex_number(value: &Value) -> Result<u64> {
    Ok(u64::from_str_radix(
        value
            .as_str()
            .ok_or_else(|| eyre!("hex quantity"))?
            .trim_start_matches("0x"),
        16,
    )?)
}

fn checksum(value: &Value) -> Result<String> {
    let address: Address = serde_json::from_value(value.clone())?;
    Ok(address.to_checksum(None))
}

fn uint_word(value: &str) -> Result<String> {
    Ok(U256::from_str_radix(value.trim_start_matches("0x"), 16)?.to_string())
}

fn address_word(value: &str) -> Result<String> {
    let address: Address = value
        .get(
            value
                .len()
                .checked_sub(40)
                .ok_or_else(|| eyre!("short address word"))?..,
        )
        .ok_or_else(|| eyre!("address word"))?
        .parse()?;
    Ok(address.to_checksum(None))
}

// Expected rows use RPC indices/quantities and fixed ABI words, never Sieve's
// decoder, context builder, sender recovery, or source-equivalence output.
#[expect(
    clippy::too_many_lines,
    reason = "independent expected event, call, and transfer rows"
)]
fn rpc_rows(value: &Value) -> Result<BTreeMap<&'static str, Vec<Value>>> {
    let mut rows = BTreeMap::from_iter(
        [
            "phase6_events",
            "phase6_approvals",
            "phase6_calls",
            "phase6_approve_calls",
            "phase6_transfers",
        ]
        .map(|name| (name, vec![])),
    );
    let transactions = value["block"]["transactions"]
        .as_array()
        .ok_or_else(|| eyre!("transactions"))?;
    let receipts = value["receipts"]
        .as_array()
        .ok_or_else(|| eyre!("receipts"))?;
    for (index, (tx, receipt)) in transactions.iter().zip(receipts).enumerate() {
        if hex_number(&receipt["status"])? == 0 {
            continue;
        }
        let common = json!({
            "block_number":hex_number(&value["block"]["number"])?,
            "tx_hash":format!("\\x{}",tx["hash"].as_str().ok_or_else(|| eyre!("hash"))?.trim_start_matches("0x")),
            "tx_index":index,
            "block_timestamp":hex_number(&value["block"]["timestamp"])?,
            "block_hash":format!("\\x{}",value["block"]["hash"].as_str().ok_or_else(|| eyre!("hash"))?.trim_start_matches("0x")),
            "tx_from":checksum(&tx["from"])?,
            "tx_to":if tx["to"].is_null() { None } else { Some(checksum(&tx["to"])?) },
            "tx_value":uint_word(tx["value"].as_str().ok_or_else(|| eyre!("value"))?)?,
            "tx_gas_price":hex_number(&receipt["effectiveGasPrice"])?,
            "tx_gas_used":hex_number(&receipt["gasUsed"])?,
            "tx_nonce":hex_number(&tx["nonce"])?,
            "cumulative_gas_used":hex_number(&receipt["cumulativeGasUsed"])?,
            "tx_status":true
        });
        if !tx["to"].is_null() && hex_number(&tx["value"])? > 0 {
            let mut row = common.clone();
            row["from_address"] = json!(checksum(&tx["from"])?);
            row["to_address"] = json!(checksum(&tx["to"])?);
            row["value"] = common["tx_value"].clone();
            rows.get_mut("phase6_transfers")
                .ok_or_else(|| eyre!("transfer table"))?
                .push(row);
        }
        if tx["to"]
            .as_str()
            .is_some_and(|to| to.eq_ignore_ascii_case(TOKEN))
        {
            let input = tx["input"].as_str().ok_or_else(|| eyre!("input"))?;
            let call = match input.get(..10) {
                Some("0xa9059cbb") => Some(("phase6_calls", "recipient")),
                Some("0x095ea7b3") => Some(("phase6_approve_calls", "spender")),
                _ => None,
            };
            if let Some((table, parameter)) = call {
                let mut row = common.clone();
                row[parameter] = json!(address_word(
                    input.get(10..74).ok_or_else(|| eyre!("call address"))?
                )?);
                row["amount"] = json!(uint_word(
                    input.get(74..138).ok_or_else(|| eyre!("call amount"))?
                )?);
                rows.get_mut(table)
                    .ok_or_else(|| eyre!("call table"))?
                    .push(row);
            }
        }
        // Sieve's public LogIndex is receipt-local; RPC logIndex is block-global.
        // Count every reference log, including logs outside the configured token.
        for (log_position, log) in receipt["logs"]
            .as_array()
            .ok_or_else(|| eyre!("logs"))?
            .iter()
            .enumerate()
        {
            if !log["address"]
                .as_str()
                .is_some_and(|a| a.eq_ignore_ascii_case(TOKEN))
            {
                continue;
            }
            let event = match log["topics"][0].as_str() {
                Some("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef") => {
                    Some(("phase6_events", "from", "to"))
                }
                Some("0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925") => {
                    Some(("phase6_approvals", "owner", "spender"))
                }
                _ => None,
            };
            if let Some((table, first, second)) = event {
                let mut row = common.clone();
                row["log_index"] = json!(log_position);
                row[first] = json!(address_word(
                    log["topics"][1]
                        .as_str()
                        .ok_or_else(|| eyre!("indexed address"))?
                )?);
                row[second] = json!(address_word(
                    log["topics"][2]
                        .as_str()
                        .ok_or_else(|| eyre!("indexed address"))?
                )?);
                row["value"] = json!(uint_word(
                    log["data"].as_str().ok_or_else(|| eyre!("event value"))?
                )?);
                rows.get_mut(table)
                    .ok_or_else(|| eyre!("event table"))?
                    .push(row);
            }
        }
    }
    Ok(rows)
}

#[tokio::test]
#[ignore = "requires isolated SIEVE_PHASE2_DATABASE_URL; run serially"]
async fn archive_indexed_rows_match_independent_rpc_events_calls_transfers_and_context(
) -> Result<()> {
    use crate::sync::ingestion::IngestionPipeline;
    let mut totals = HashMap::<&str, usize>::new();
    for raw in REFERENCES {
        let (value, payload) = reference(raw)?;
        let jars = Jars::new(&payload)?;
        let ctx = reference_context(&jars).await?;
        let reader = HeaderReader::open(
            &jars.stage,
            std::slice::from_ref(&jars.group),
            jars.evidence.clone(),
        )?;
        let proof = AuthenticatedArchive::new(
            jars.evidence.clone(),
            reader.headers(&jars.evidence)?,
            &BTreeSet::default(),
        )?;
        let number = payload.header().number;
        let segment = proof.segment(reader.range(number, number)?, number, number)?;
        let pipeline = IngestionPipeline::start(ctx.clone(), segment).await?;
        let send = pipeline.sender();
        send.send(crate::sync::FetchItem::Payload(Box::new(jars.read()?)))
            .await
            .map_err(|_| eyre!("pipeline closed"))?;
        drop(send);
        pipeline.finish().await?;
        for (table, expected) in rpc_rows(&value)? {
            let numeric = if table.contains("calls") {
                "amount"
            } else {
                "value"
            };
            let order = if table == "phase6_events" || table == "phase6_approvals" {
                "tx_index, log_index"
            } else {
                "tx_index"
            };
            let query = format!("SELECT to_jsonb(t) - 'id' || jsonb_build_object('{numeric}', t.{numeric}::text, 'tx_value', t.tx_value::text) FROM {table} t ORDER BY {order}");
            let actual: Vec<Value> = sqlx::query_scalar(&query).fetch_all(ctx.db.pool()).await?;
            assert_eq!(actual, expected, "block {number}, table {table}");
            *totals.entry(table).or_default() += actual.len();
        }
        assert_eq!(
            ctx.db.last_checkpoint().await?,
            Some(crate::types::BlockNumber::new(number))
        );
        assert_eq!(
            ctx.db
                .get_block_hash(crate::types::BlockNumber::new(number))
                .await?,
            Some(payload.header().hash_slow())
        );
    }
    assert!(totals["phase6_events"] > 100);
    assert!(totals["phase6_calls"] >= 2);
    assert!(totals["phase6_approve_calls"] >= 1);
    assert!(totals["phase6_transfers"] >= 10);
    Ok(())
}

struct Root(PathBuf);
impl Drop for Root {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Jars {
    stage: Staging,
    group: Group,
    evidence: ArchiveEvidence,
    root: Root,
}

fn encoded<T: Compact>(value: &T) -> Vec<u8> {
    let mut bytes = Vec::new();
    value.to_compact(&mut bytes);
    bytes
}

impl Jars {
    fn new(payload: &BlockPayload<BaseChain>) -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = Root(std::env::temp_dir().join(format!(
            "sieve-phase6-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )));
        fs::create_dir(&root.0)?;
        let number = payload.header().number;
        let evidence = ArchiveEvidence {
            manifest_sha256: B256::repeat_byte(6),
            genesis_hash: ChainKind::Base.genesis_hash(),
            first_block: number,
            anchor_block: number,
            anchor_hash: payload.header().hash_slow(),
        };
        let config = ArchiveConfig {
            handoff: false,
            handoff_retry_secs: 15,
            manifest: root.0.join("manifest.json"),
            manifest_sha256: String::new(),
            end_block: number,
            checkpoint_hash: evidence.anchor_hash,
            staging_dir: root.0.join("stage"),
            max_staging_bytes: 20_000_000,
            max_download_bytes: 20_000_000,
            min_free_bytes: 0,
            working_space_bytes: 0,
            retries: 0,
            timeout_secs: 2,
            max_transactions_per_block: 100_000,
        };
        let (_stop, rx) = watch::channel(false);
        let stage = Staging::open(config, br#"{"group":"generated-reference"}"#, b"{}", rx)?;
        let archives = [
            write_jar(&stage, payload, "headers", StaticFileSegment::Headers)?,
            write_jar(
                &stage,
                payload,
                "transactions",
                StaticFileSegment::Transactions,
            )?,
            write_jar(&stage, payload, "receipts", StaticFileSegment::Receipts)?,
        ];
        let group = Group {
            archive_range: [number, number],
            available_range: [number, number],
            index_range: [number, number],
            decode_from: number,
            compressed_bytes: 0,
            extracted_bytes: archives.iter().map(|a| a.extracted_bytes).sum(),
            archives,
        };
        Ok(Self {
            stage,
            group,
            evidence,
            root,
        })
    }

    fn read(&self) -> Result<ValidatedPayload<BaseChain>> {
        let reader = HeaderReader::open(
            &self.stage,
            std::slice::from_ref(&self.group),
            self.evidence.clone(),
        )?;
        let proof = AuthenticatedArchive::new(
            self.evidence.clone(),
            reader.headers(&self.evidence)?,
            &BTreeSet::default(),
        )?;
        let number = self.evidence.anchor_block;
        let segment = proof.segment(reader.range(number, number)?, number, number)?;
        let mut result = None;
        reader::scan(&self.stage, &self.group, 100_000, |payload| {
            assert_eq!(
                segment.get(number).map(SealedHeader::hash),
                Some(payload.header().hash_slow())
            );
            result = Some(payload);
            Ok(())
        })?;
        result.ok_or_else(|| eyre!("missing reconstructed reference block"))
    }
}

fn write_jar(
    stage: &Staging,
    payload: &BlockPayload<BaseChain>,
    name: &'static str,
    kind: StaticFileSegment,
) -> Result<Archive> {
    let number = payload.header().number;
    let dir = stage.root.join(name);
    fs::create_dir_all(dir.join("static_files"))?;
    let relative = format!("static_files/static_file_{name}_{number}_{number}");
    let mut rows = vec![Vec::<Vec<u8>>::new(); kind.columns()];
    match kind {
        StaticFileSegment::Headers => {
            rows[0].push(encoded(payload.header()));
            rows[1].push(vec![0]);
            rows[2].push(payload.header().hash_slow().to_vec());
        }
        StaticFileSegment::Transactions => {
            rows[0].extend(payload.body().transactions.iter().map(encoded));
        }
        StaticFileSegment::Receipts => rows[0].extend(payload.receipts().iter().map(encoded)),
        _ => return Err(eyre!("unexpected generated segment")),
    }
    let tx_range = (kind != StaticFileSegment::Headers && !rows[0].is_empty())
        .then(|| SegmentRangeInclusive::new(50, 49 + rows[0].len() as u64));
    let range = SegmentRangeInclusive::new(number, number);
    let header = SegmentHeader::new(range, Some(range), tx_range, kind);
    let mut writer =
        NippyJarWriter::new(NippyJar::new(kind.columns(), &dir.join(&relative), header))?;
    let columns = rows
        .iter()
        .map(|col| {
            col.iter()
                .map(|r| Ok::<&[u8], Box<dyn std::error::Error + Send + Sync>>(r.as_slice()))
        })
        .collect::<Vec<_>>();
    writer.append_rows(columns, rows[0].len() as u64)?;
    writer.commit()?;
    drop(writer);
    let files = ["", ".conf", ".off"]
        .iter()
        .map(|suffix| -> Result<_> {
            let path = format!("{relative}{suffix}");
            let bytes = fs::read(dir.join(&path))?;
            Ok(OutputFile {
                path,
                size: bytes.len() as u64,
                blake3: blake3::hash(&bytes).to_hex().to_string(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let archive = Archive {
        component: name,
        url: format!("https://fixture.invalid/{name}.tar.zst"),
        compressed_bytes: 0,
        extracted_bytes: files.iter().map(|f| f.size).sum(),
        files,
    };
    stage.verify(&archive)?;
    Ok(archive)
}

#[test]
fn rpc_references_survive_compact_jars_and_match_independent_commitments() -> Result<()> {
    let index: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/base-reference/index.json"
    ))?;
    assert_eq!(index.as_array().map(Vec::len), Some(REFERENCES.len()));
    for (raw, entry) in REFERENCES
        .into_iter()
        .zip(index.as_array().ok_or_else(|| eyre!("reference index"))?)
    {
        assert_eq!(
            format!("{:x}", Sha256::digest(raw)),
            entry["sha256"]
                .as_str()
                .ok_or_else(|| eyre!("reference digest"))?
        );
        let (value, payload) = reference(raw)?;
        let rpc_hash: B256 = serde_json::from_value(value["block"]["hash"].clone())?;
        assert_eq!(payload.header().hash_slow(), rpc_hash);
        let decoded = Jars::new(&payload)?.read()?;
        assert_eq!(decoded.header(), payload.header());
        for (tx, rpc_tx) in payload.body().transactions.iter().zip(
            value["block"]["transactions"]
                .as_array()
                .ok_or_else(|| eyre!("reference transactions"))?,
        ) {
            let from: alloy_primitives::Address = serde_json::from_value(rpc_tx["from"].clone())?;
            assert_eq!(tx.recover_signer_unchecked()?, from);
        }
    }
    Ok(())
}

#[test]
fn deposits_zero_gas_and_empty_blocks_decode_across_supported_fork_boundaries() -> Result<()> {
    use alloy_consensus::{proofs, Receipt, Signed, TxLegacy};
    use alloy_primitives::{Bytes, Signature};
    use op_alloy_consensus::{OpDepositReceipt, TxDeposit};
    const CANYON: u64 = 1_704_992_401;
    const ISTHMUS: u64 = 1_746_806_401;
    for (name, activation) in [
        ("Canyon", CANYON),
        ("Ecotone", 1_710_374_401),
        ("Fjord", 1_720_627_201),
        ("Granite", 1_726_070_401),
        ("Holocene", 1_736_445_601),
        ("Isthmus", ISTHMUS),
        ("Jovian", 1_764_691_201),
        ("Azul", 1_779_991_200),
        ("Beryl", 1_782_410_400),
    ] {
        for timestamp in [activation - 1, activation] {
            for empty in [true, false] {
                let transactions = if empty {
                    vec![]
                } else {
                    vec![
                        OpTxEnvelope::from(TxDeposit {
                            source_hash: B256::repeat_byte(9),
                            from: Address::repeat_byte(1),
                            to: Address::repeat_byte(2).into(),
                            gas_limit: 1_000_000,
                            input: Bytes::from_static(b"deposit"),
                            ..Default::default()
                        }),
                        OpTxEnvelope::Legacy(Signed::new_unhashed(
                            TxLegacy {
                                chain_id: Some(8453),
                                gas_limit: 21_000,
                                ..Default::default()
                            },
                            Signature::new(U256::from(1), U256::from(1), false),
                        )),
                    ]
                };
                let receipts = if empty {
                    vec![]
                } else {
                    vec![
                        OpReceipt::Deposit(OpDepositReceipt {
                            inner: Receipt {
                                status: true.into(),
                                cumulative_gas_used: 0,
                                logs: vec![],
                            },
                            deposit_nonce: Some(123),
                            deposit_receipt_version: (timestamp >= CANYON).then_some(1),
                        }),
                        OpReceipt::Legacy(Receipt {
                            status: true.into(),
                            cumulative_gas_used: 21_000,
                            logs: vec![],
                        }),
                    ]
                };
                let body = BlockBody {
                    transactions,
                    withdrawals: (timestamp >= CANYON).then(Withdrawals::default),
                    ..Default::default()
                };
                let mut header = Header {
                    number: 100,
                    timestamp,
                    gas_used: if empty { 0 } else { 21_000 },
                    ommers_hash: proofs::calculate_ommers_root(&body.ommers),
                    transactions_root: proofs::calculate_transaction_root(&body.transactions),
                    withdrawals_root: (timestamp >= CANYON).then_some(if timestamp >= ISTHMUS {
                        B256::repeat_byte(7)
                    } else {
                        alloy_consensus::constants::EMPTY_ROOT_HASH
                    }),
                    ..Default::default()
                };
                header.receipts_root = BaseChain::receipts_root(&receipts, &header);
                let payload = BlockPayload::new(header, body, receipts);
                assert_eq!(
                    Jars::new(&payload)?.read()?.header(),
                    payload.header(),
                    "{name}: {timestamp}, empty={empty}"
                );
                let mut broken = payload.header().clone();
                broken.gas_used += 1;
                assert!(
                    reader::reconstruct(broken, 10, || Err(eyre!("no more transactions"))).is_err()
                );
            }
        }
    }
    Ok(())
}

#[test]
fn malformed_rows_and_payloads_fail_before_a_block_is_emitted() -> Result<()> {
    let (_, payload) = reference(REFERENCES[0])?;
    let mut short = payload.receipts().to_vec();
    short.pop();
    let mismatch = BlockPayload::new(payload.header().clone(), payload.body().clone(), short);
    assert!(Jars::new(&mismatch)?.read().is_err());

    let mut body = payload.body().clone();
    body.transactions.push(body.transactions[0].clone());
    let mut receipts = payload.receipts().to_vec();
    receipts.push(receipts[0].clone());
    let extra = BlockPayload::new(payload.header().clone(), body, receipts);
    assert!(Jars::new(&extra)?.read().is_err());

    let mut wrong_root = payload.header().clone();
    wrong_root.receipts_root = B256::ZERO;
    let wrong = BlockPayload::new(
        wrong_root,
        payload.body().clone(),
        payload.receipts().to_vec(),
    );
    assert!(Jars::new(&wrong)?.read().is_err());

    let mut consumed = 0;
    let transaction = payload.body().transactions[0].clone();
    let receipt = payload.receipts()[0].clone();
    assert!(reader::reconstruct(payload.header().clone(), 0, || {
        consumed += 1;
        Ok((transaction.clone(), receipt.clone()))
    })
    .is_err());
    assert_eq!(consumed, 0);
    Ok(())
}

#[test]
fn isthmus_set_code_authorizations_and_receipts_survive_compact_jars() -> Result<()> {
    use alloy_consensus::{proofs, Receipt, Signed, TxEip7702};
    use alloy_eips::eip7702::{Authorization, SignedAuthorization};
    use alloy_primitives::Signature;
    let authorization = SignedAuthorization::new_unchecked(
        Authorization {
            chain_id: U256::from(8453),
            address: Address::repeat_byte(1),
            nonce: 7,
        },
        0,
        U256::from(1),
        U256::from(1),
    );
    let transaction = TxEip7702 {
        chain_id: 8453,
        nonce: 8,
        gas_limit: 100_000,
        max_fee_per_gas: 2,
        max_priority_fee_per_gas: 1,
        to: Address::repeat_byte(2),
        authorization_list: vec![authorization],
        ..Default::default()
    };
    let body = BlockBody {
        transactions: vec![OpTxEnvelope::Eip7702(Signed::new_unhashed(
            transaction,
            Signature::new(U256::from(1), U256::from(1), false),
        ))],
        withdrawals: Some(Withdrawals::default()),
        ..Default::default()
    };
    let receipts = vec![OpReceipt::Eip7702(Receipt {
        status: true.into(),
        cumulative_gas_used: 46_000,
        logs: vec![],
    })];
    let mut header = Header {
        number: 100,
        timestamp: 1_746_806_401,
        gas_used: 46_000,
        withdrawals_root: Some(B256::repeat_byte(7)),
        ommers_hash: proofs::calculate_ommers_root(&body.ommers),
        transactions_root: proofs::calculate_transaction_root(&body.transactions),
        ..Default::default()
    };
    header.receipts_root = BaseChain::receipts_root(&receipts, &header);
    let payload = BlockPayload::new(header, body, receipts);
    assert_eq!(Jars::new(&payload)?.read()?.header(), payload.header());
    Ok(())
}

fn factory_payload() -> BlockPayload<BaseChain> {
    use alloy_consensus::{proofs, Receipt};
    use alloy_primitives::{Bytes, Log, LogData};
    use op_alloy_consensus::{OpDepositReceipt, TxDeposit};
    let sender = Address::repeat_byte(0x11);
    let factory = Address::repeat_byte(0x22);
    let child = Address::repeat_byte(0x33);
    let recipient = Address::repeat_byte(0x44);
    let transfer = alloy_primitives::keccak256("Transfer(address,address,uint256)");
    let log = |address, topics, value: u64| Log {
        address,
        data: LogData::new_unchecked(
            topics,
            Bytes::copy_from_slice(&U256::from(value).to_be_bytes::<32>()),
        ),
    };
    let mut transactions = Vec::new();
    let mut receipts = Vec::new();
    for index in 0..3u8 {
        let mut input = vec![0xa9, 0x05, 0x9c, 0xbb];
        input.extend_from_slice(B256::left_padding_from(recipient.as_slice()).as_slice());
        input.extend_from_slice(&U256::from(if index == 1 { 42 } else { 99 }).to_be_bytes::<32>());
        transactions.push(OpTxEnvelope::from(TxDeposit {
            source_hash: B256::repeat_byte(index),
            from: sender,
            to: if index == 0 { factory } else { child }.into(),
            value: U256::from([7, 50, 75][index as usize]),
            gas_limit: 100_000,
            input: input.into(),
            ..Default::default()
        }));
        let logs = match index {
            0 => vec![
                Log {
                    address: factory,
                    data: LogData::new_unchecked(
                        vec![
                            alloy_primitives::keccak256("Created(address)"),
                            B256::left_padding_from(child.as_slice()),
                        ],
                        Bytes::new(),
                    ),
                },
                log(
                    child,
                    vec![
                        transfer,
                        B256::left_padding_from(sender.as_slice()),
                        B256::left_padding_from(recipient.as_slice()),
                    ],
                    17,
                ),
            ],
            1 => vec![log(
                child,
                vec![
                    transfer,
                    B256::left_padding_from(sender.as_slice()),
                    B256::left_padding_from(recipient.as_slice()),
                ],
                42,
            )],
            _ => vec![],
        };
        receipts.push(OpReceipt::Deposit(OpDepositReceipt {
            inner: Receipt {
                status: (index != 2).into(),
                cumulative_gas_used: [1_000, 31_000, 50_000][index as usize],
                logs,
            },
            deposit_nonce: Some(300 + u64::from(index)),
            deposit_receipt_version: Some(1),
        }));
    }
    let body = BlockBody {
        transactions,
        withdrawals: Some(Withdrawals::default()),
        ..Default::default()
    };
    let mut header = Header {
        number: 100,
        timestamp: 1_790_640_011,
        gas_used: 50_000,
        withdrawals_root: Some(B256::repeat_byte(7)),
        ommers_hash: proofs::calculate_ommers_root(&body.ommers),
        transactions_root: proofs::calculate_transaction_root(&body.transactions),
        ..Default::default()
    };
    header.receipts_root = BaseChain::receipts_root(&receipts, &header);
    BlockPayload::new(header, body, receipts)
}

#[tokio::test]
#[ignore = "requires isolated SIEVE_PHASE2_DATABASE_URL; run serially"]
#[expect(
    clippy::too_many_lines,
    reason = "literal expected rows for all handlers and factory journal"
)]
async fn same_block_factory_deposit_context_and_reverted_calls_match_literal_reference(
) -> Result<()> {
    use crate::sync::ingestion::IngestionPipeline;
    let payload = factory_payload();
    let jars = Jars::new(&payload)?;
    fs::write(jars.root.0.join("token.json"), TOKEN_ABI)?;
    fs::write(
        jars.root.0.join("factory.json"),
        r#"[{"type":"event","name":"Created","anonymous":false,"inputs":[{"name":"child","type":"address","indexed":true}]}]"#,
    )?;
    let ctx = configured_context(&jars, json!({
        "chain":"base",
        "contracts":[{"name":"TokenChild","abi":"token.json","include_receipts":true,
            "factory":{"address":"0x2222222222222222222222222222222222222222","abi":"factory.json","event":"Created","param":"child","start_block":100},
            "events":[{"name":"Transfer","table":"phase6_children","context":["tx_from","tx_to"]}],
            "calls":[{"name":"transfer","table":"phase6_child_calls","context":["tx_from","tx_to"]}]}],
        "transfers":[{"name":"ETH","table":"phase6_deposit_transfers","include_receipts":true}]
    })).await?;
    let reader = HeaderReader::open(
        &jars.stage,
        std::slice::from_ref(&jars.group),
        jars.evidence.clone(),
    )?;
    let proof = AuthenticatedArchive::new(
        jars.evidence.clone(),
        reader.headers(&jars.evidence)?,
        &BTreeSet::default(),
    )?;
    let pipeline = IngestionPipeline::start(
        ctx.clone(),
        proof.segment(reader.range(100, 100)?, 100, 100)?,
    )
    .await?;
    let send = pipeline.sender();
    send.send(crate::sync::FetchItem::Payload(Box::new(jars.read()?)))
        .await
        .map_err(|_| eyre!("pipeline closed"))?;
    drop(send);
    let outcome = pipeline.finish().await?;
    assert_eq!(
        (
            outcome.events_stored,
            outcome.calls_stored,
            outcome.transfers_stored
        ),
        (2, 1, 2)
    );
    let events: Vec<(i32,i32,String,i64,i64,i64,bool)> = sqlx::query_as("SELECT tx_index,log_index,value::text,tx_gas_used,cumulative_gas_used,tx_nonce,tx_status FROM phase6_children ORDER BY tx_index,log_index").fetch_all(ctx.db.pool()).await?;
    assert_eq!(
        events,
        vec![
            (0, 1, "17".into(), 1_000, 1_000, 0, true),
            (1, 0, "42".into(), 30_000, 31_000, 0, true)
        ]
    );
    let calls: Vec<(i32,String,String,i64,i64,bool)> = sqlx::query_as("SELECT tx_index,recipient,amount::text,tx_gas_used,tx_nonce,tx_status FROM phase6_child_calls").fetch_all(ctx.db.pool()).await?;
    assert_eq!(
        calls,
        vec![(
            1,
            "0x4444444444444444444444444444444444444444".into(),
            "42".into(),
            30_000,
            0,
            true
        )]
    );
    let transfers: Vec<(i32,String,String,String,i64,i64)> = sqlx::query_as("SELECT tx_index,from_address,to_address,value::text,tx_gas_used,tx_nonce FROM phase6_deposit_transfers ORDER BY tx_index").fetch_all(ctx.db.pool()).await?;
    assert_eq!(
        transfers,
        vec![
            (
                0,
                "0x1111111111111111111111111111111111111111".into(),
                "0x2222222222222222222222222222222222222222".into(),
                "7".into(),
                1_000,
                0
            ),
            (
                1,
                "0x1111111111111111111111111111111111111111".into(),
                "0x3333333333333333333333333333333333333333".into(),
                "50".into(),
                30_000,
                0
            )
        ]
    );
    let child: (Vec<u8>, i64) =
        sqlx::query_as("SELECT child_address,block_number FROM _sieve_factory_children")
            .fetch_one(ctx.db.pool())
            .await?;
    assert_eq!(child, (vec![0x33; 20], 100));
    let coverage: i64 = sqlx::query_scalar("SELECT covered_through FROM _sieve_factories")
        .fetch_one(ctx.db.pool())
        .await?;
    assert_eq!(coverage, 100);
    let addresses: Vec<String> =
        sqlx::query_scalar("SELECT contract_address FROM phase6_children ORDER BY tx_index")
            .fetch_all(ctx.db.pool())
            .await?;
    assert_eq!(
        addresses,
        vec!["0x3333333333333333333333333333333333333333"; 2]
    );
    Ok(())
}
