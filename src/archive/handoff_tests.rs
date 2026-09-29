#![expect(clippy::panic_in_result_fn, reason = "test assertions")]
use super::*;
use crate::{p2p::NetworkPeer, sync::BlockPayload};
use alloy_consensus::{ReceiptWithBloom, TxReceipt};
use parking_lot::RwLock;
use reth_eth_wire::EthVersion;
use reth_eth_wire_types::{
    BlockBodies, BlockHashOrNumber, BlockHeaders, HeadersDirection, Receipts,
};
use reth_network_api::{PeerId, PeerRequest, PeerRequestSender};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

pub struct Peers {
    pub pool: Arc<PeerPool<BaseChain>>,
    pub blocks: Arc<RwLock<Vec<BlockPayload<BaseChain>>>>,
    pub bodies: Arc<AtomicBool>,
    pub receipts: Arc<AtomicBool>,
    pub earliest: Arc<AtomicU64>,
}

impl Peers {
    #[expect(clippy::too_many_lines, reason = "mock peer protocol responder")]
    pub fn new(payloads: Vec<BlockPayload<BaseChain>>, count: u8) -> Self {
        let blocks = Arc::new(RwLock::new(payloads));
        let bodies = Arc::new(AtomicBool::new(true));
        let receipts = Arc::new(AtomicBool::new(true));
        let earliest = Arc::new(AtomicU64::new(0));
        let mut peers = Vec::new();
        for n in 1..=count {
            let peer_id = PeerId::repeat_byte(n);
            let (tx, mut rx) = tokio::sync::mpsc::channel::<
                PeerRequest<<BaseChain as crate::chain::ChainTypes>::Net>,
            >(64);
            peers.push(NetworkPeer {
                peer_id,
                messages: PeerRequestSender::new(peer_id, tx),
                eth_version: EthVersion::Eth68,
                head_number: blocks.read().last().map_or(0, |p| p.header().number),
                earliest_block: Some(0),
                last_success: tokio::time::Instant::now(),
            });
            let blocks = Arc::clone(&blocks);
            let bodies = Arc::clone(&bodies);
            let receipts = Arc::clone(&receipts);
            let earliest = Arc::clone(&earliest);
            tokio::spawn(async move {
                while let Some(request) = rx.recv().await {
                    let blocks = blocks.read();
                    let available = |p: &&BlockPayload<BaseChain>| {
                        p.header().number >= earliest.load(Ordering::Relaxed)
                    };
                    match request {
                        PeerRequest::GetBlockHeaders { request, response } => {
                            let start = match request.start_block {
                                BlockHashOrNumber::Number(n) => Some(n),
                                BlockHashOrNumber::Hash(h) => blocks
                                    .iter()
                                    .find(|p| p.header().hash_slow() == h)
                                    .map(|p| p.header().number),
                            };
                            let mut result = Vec::new();
                            if let Some(mut number) = start {
                                for _ in 0..request.limit {
                                    let Some(p) = blocks
                                        .iter()
                                        .filter(available)
                                        .find(|p| p.header().number == number)
                                    else {
                                        break;
                                    };
                                    result.push(p.header().clone());
                                    let next = if request.direction == HeadersDirection::Rising {
                                        number.checked_add(u64::from(request.skip) + 1)
                                    } else {
                                        number.checked_sub(u64::from(request.skip) + 1)
                                    };
                                    let Some(n) = next else {
                                        break;
                                    };
                                    number = n;
                                }
                            }
                            let _ = response.send(Ok(BlockHeaders(result)));
                        }
                        PeerRequest::GetBlockBodies { request, response } => {
                            let result = request
                                .0
                                .iter()
                                .filter_map(|hash| {
                                    bodies
                                        .load(Ordering::Relaxed)
                                        .then(|| {
                                            blocks
                                                .iter()
                                                .filter(available)
                                                .find(|p| p.header().hash_slow() == *hash)
                                                .map(|p| p.body().clone())
                                        })
                                        .flatten()
                                })
                                .collect();
                            let _ = response.send(Ok(BlockBodies(result)));
                        }
                        PeerRequest::GetReceipts { request, response } => {
                            let result = request
                                .0
                                .iter()
                                .filter_map(|hash| {
                                    receipts
                                        .load(Ordering::Relaxed)
                                        .then(|| {
                                            blocks
                                                .iter()
                                                .filter(available)
                                                .find(|p| p.header().hash_slow() == *hash)
                                                .map(|p| {
                                                    p.receipts()
                                                        .iter()
                                                        .cloned()
                                                        .map(|receipt| ReceiptWithBloom {
                                                            logs_bloom: receipt.bloom(),
                                                            receipt,
                                                        })
                                                        .collect()
                                                })
                                        })
                                        .flatten()
                                })
                                .collect();
                            let _ = response.send(Ok(Receipts(result)));
                        }
                        _ => {}
                    }
                }
            });
        }
        Self {
            pool: Arc::new(PeerPool::fixture(peers)),
            blocks,
            bodies,
            receipts,
            earliest,
        }
    }
}

pub fn policy() -> canonical::QuorumPolicy {
    canonical::QuorumPolicy {
        peer_wait: Duration::ZERO,
        rounds: 1,
        round_delay: Duration::ZERO,
        ..Default::default()
    }
}

#[tokio::test]
async fn bridge_requires_quorum_and_actual_bodies_and_receipts() -> Result<()> {
    let blocks = crate::sync::ingestion_tests::fixture()?;
    let bridge = Bridge {
        end: 100,
        hash: blocks[0].header().hash_slow(),
        retry_secs: 1,
    };
    let (_tx, stop) = watch::channel(false);
    let peers = Peers::new(blocks.clone(), 3);
    peers.earliest.store(101, Ordering::Relaxed);
    bridge.probe(&peers.pool, &policy(), &stop).await?;
    peers.receipts.store(false, Ordering::Relaxed);
    assert!(bridge.probe(&peers.pool, &policy(), &stop).await.is_err());
    peers.receipts.store(true, Ordering::Relaxed);
    peers.bodies.store(false, Ordering::Relaxed);
    assert!(bridge.probe(&peers.pool, &policy(), &stop).await.is_err());
    peers.bodies.store(true, Ordering::Relaxed);
    let wrong = Bridge {
        hash: B256::ZERO,
        ..bridge
    };
    assert!(wrong.probe(&peers.pool, &policy(), &stop).await.is_err());
    let sparse = Peers::new(blocks, 2);
    assert!(bridge.probe(&sparse.pool, &policy(), &stop).await.is_err());
    peers.earliest.store(102, Ordering::Relaxed);
    assert!(bridge.probe(&peers.pool, &policy(), &stop).await.is_err());
    Ok(())
}
