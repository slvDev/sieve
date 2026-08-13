//! OP mainnet chain wiring (OP-stack L2, chain id 10).
//!
//! Unlike Base, OP mainnet remains in the OP superchain: its execution
//! peers run standard devp2p discovery (stock discv4 + discv5, no custom
//! packet protocol id) and the pinned reth's `bootnodes()` already maps
//! the chain to the shared OP-stack bootnode set — so the default network
//! configuration applies unchanged.
//!
//! One thing differs from the pinned reth's built-in `OP_MAINNET`, whose
//! fork schedule ends at Jovian (2025-12-02): the Karst hardfork
//! (superchain Upgrade 19) activated on 2026-07-08. Karst enables a
//! subset of Osaka EIPs on L2 plus governance contract upgrades via
//! network upgrade transactions — no new transaction type and no wire
//! changes, so the pinned OP envelope types decode post-Karst blocks
//! fine, but the fork must be in the schedule or the EIP-2124 fork-id
//! would be stale and current peers would reject the handshake.

use super::{op_stack, ChainTypes};
use alloy_primitives::B256;
use op_alloy_consensus::{OpPooledTransaction, OpReceipt, OpTxEnvelope};
use reth_chainspec::{EthChainSpec, ForkCondition, Hardfork};
use reth_eth_wire_types::BasicNetworkPrimitives;
use reth_network::config::NetworkConfigBuilder;
use reth_optimism_chainspec::{OpChainSpec, OP_MAINNET};
use reth_optimism_primitives::OpPrimitives;
use reth_primitives_traits::Header;
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};

/// Karst activation timestamp on OP mainnet (2026-07-08 16:00:01 UTC).
const KARST_TIMESTAMP: u64 = 1_783_526_401;

/// OP mainnet hardforks not present in the pinned reth's fork list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum OpMainnetHardfork {
    /// 2026-07-08: Osaka EIP subset on L2, L2 contract manager (Upgrade 19).
    Karst,
}

impl Hardfork for OpMainnetHardfork {
    fn name(&self) -> &'static str {
        match self {
            Self::Karst => "Karst",
        }
    }
}

/// `OP_MAINNET` extended with the post-Jovian Karst hardfork.
static OP_MAINNET_CURRENT: LazyLock<Arc<OpChainSpec>> = LazyLock::new(|| {
    let mut spec: OpChainSpec = (**OP_MAINNET).clone();
    spec.inner.hardforks.insert(
        OpMainnetHardfork::Karst,
        ForkCondition::Timestamp(KARST_TIMESTAMP),
    );
    Arc::new(spec)
});

/// OP mainnet.
#[derive(Debug, Clone, Copy)]
pub struct OptimismChain;

impl ChainTypes for OptimismChain {
    type SignedTx = OpTxEnvelope;
    type Receipt = OpReceipt;
    type Primitives = OpPrimitives;
    type Net = BasicNetworkPrimitives<OpPrimitives, OpPooledTransaction>;
    type Spec = OpChainSpec;

    const NAME: &'static str = "optimism";

    fn chain_spec() -> Arc<OpChainSpec> {
        Arc::clone(&OP_MAINNET_CURRENT)
    }

    fn receipts_root(receipts: &[OpReceipt], header: &Header) -> B256 {
        op_stack::receipts_root(Self::chain_spec().as_ref(), receipts, header)
    }

    fn withdrawals_valid(header: &Header, body: &alloy_consensus::BlockBody<OpTxEnvelope>) -> bool {
        op_stack::withdrawals_valid(Self::chain_spec().as_ref(), header, body)
    }

    /// OP mainnet discovery: stock discv4 (kept — the legacy shared
    /// OP-stack DHT) PLUS standard discv5 with the default protocol id.
    /// Modern op-reth execution peers discover over discv5 with the
    /// `opel` ENR fork entry (reth sets it automatically from the OP
    /// chain spec); discv4 alone finds mostly other chains' nodes in the
    /// shared DHT.
    fn configure_network(
        builder: NetworkConfigBuilder<Self::Net>,
        listen_addr: SocketAddr,
    ) -> NetworkConfigBuilder<Self::Net> {
        // discv4 stays on the RLPx port's UDP; discv5 binds one port up
        // (both protocols cannot share a socket).
        let discv5_port = listen_addr.port().saturating_add(1);
        let discv5_addr = SocketAddr::new(listen_addr.ip(), discv5_port);
        let boot_nodes = Self::chain_spec().bootnodes().unwrap_or_default();
        let discv5_builder = reth_discv5::Config::builder(discv5_addr)
            .discv5_config(
                discv5::ConfigBuilder::new(discv5::ListenConfig::from_ip(
                    std::net::Ipv4Addr::UNSPECIFIED.into(),
                    discv5_port,
                ))
                .build(),
            )
            .add_unsigned_boot_nodes(boot_nodes);
        builder.discovery_v5(discv5_builder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::b256;
    use reth_chainspec::EthChainSpec;

    #[test]
    fn genesis_hash_matches_op_mainnet() {
        assert_eq!(
            OptimismChain::chain_spec().genesis_hash(),
            b256!("7ca38a1916c42007829c55e69d3e9a73265554b586a499015373241b8a3fa48b")
        );
    }

    #[test]
    fn karst_is_scheduled_after_jovian() {
        let spec = OptimismChain::chain_spec();
        assert!(spec
            .inner
            .hardforks
            .fork(OpMainnetHardfork::Karst)
            .active_at_timestamp(KARST_TIMESTAMP));
        assert!(!spec
            .inner
            .hardforks
            .fork(OpMainnetHardfork::Karst)
            .active_at_timestamp(KARST_TIMESTAMP - 1));
        // Karst must come after the pin's last known fork (Jovian).
        assert!(spec
            .inner
            .hardforks
            .fork(reth_optimism_forks::OpHardfork::Jovian)
            .active_at_timestamp(KARST_TIMESTAMP));
    }

    #[test]
    fn latest_fork_id_matches_karst() {
        // Expected head fork-id after Karst (EIP-2124 CRC over the genesis
        // hash and every fork block/timestamp), verified live against OP
        // mainnet peers via `sieve peers --chain optimism`.
        let fork_id = OptimismChain::chain_spec().latest_fork_id();
        assert_eq!(fork_id.next, 0);
        assert_eq!(fork_id.hash.0, [0xc2, 0x92, 0x39, 0xaf]);
    }

    #[test]
    fn bootnodes_resolve_to_op_stack_set() {
        let nodes = OptimismChain::chain_spec().bootnodes().unwrap_or_default();
        assert!(!nodes.is_empty());
    }
}
