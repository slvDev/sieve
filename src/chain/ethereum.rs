//! Ethereum mainnet chain wiring.

use super::ChainTypes;
use alloy_consensus::BlockBody;
use alloy_primitives::B256;
use reth_chainspec::{ChainSpec, MAINNET};
use reth_eth_wire::EthNetworkPrimitives;
use reth_ethereum_primitives::{EthPrimitives, Receipt, TransactionSigned};
use reth_primitives_traits::Header;
use std::sync::Arc;

/// Ethereum mainnet.
#[derive(Debug, Clone, Copy)]
pub struct EthereumChain;

impl ChainTypes for EthereumChain {
    type SignedTx = TransactionSigned;
    type Receipt = Receipt;
    type Primitives = EthPrimitives;
    type Net = EthNetworkPrimitives;
    type Spec = ChainSpec;

    const NAME: &'static str = "mainnet";

    fn chain_spec() -> Arc<ChainSpec> {
        MAINNET.clone()
    }

    fn receipts_root(receipts: &[Receipt], _header: &Header) -> B256 {
        Receipt::calculate_receipt_root_no_memo(receipts)
    }

    fn withdrawals_valid(header: &Header, body: &BlockBody<TransactionSigned>) -> bool {
        body.calculate_withdrawals_root() == header.withdrawals_root
    }
}
