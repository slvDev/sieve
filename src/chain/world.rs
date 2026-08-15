//! World Chain mainnet wiring (OP-stack L2, chain id 480).
//!
//! World Chain (by Tools for Humanity) is a member of the OP superchain: its
//! execution peers run standard devp2p discovery (stock discv4 + discv5, no
//! custom packet protocol id) and share the OP-stack bootnode set, so the
//! network configuration matches OP mainnet exactly. The pinned reth ships
//! World Chain's chain spec in-tree via the embedded superchain registry
//! (`WORLDCHAIN_MAINNET`).
//!
//! World Chain's PBH ("Priority Blockspace for Humans", giving World ID
//! verified users priority inclusion) is a block-building/mempool policy, not
//! a wire change: PBH affects transaction validation and block ordering, but
//! transactions included in blocks retain standard OP envelopes, and the node
//! uses stock OP primitives (`OpTxEnvelope`/`OpReceipt`). No new transaction
//! envelope type, so the pinned OP types decode every World Chain block.
//!
//! Unlike OP mainnet and Unichain, World Chain does NOT follow the shared
//! superchain fork timestamps — it runs a deliberately delayed schedule
//! (e.g. Jovian on 2026-05-01, months after the shared 2025-12-02), and has
//! no Karst entry in the registry as of this writing. Its post-pin schedule
//! is applied below; the head fork-id is locked by a unit test and verified
//! live against World Chain peers via `sieve peers --chain world`.

use super::{op_stack, ChainTypes};
use alloy_primitives::B256;
use op_alloy_consensus::{OpPooledTransaction, OpReceipt, OpTxEnvelope};
use reth_chainspec::{EthChainSpec, ForkCondition};
use reth_eth_wire_types::BasicNetworkPrimitives;
use reth_network::config::NetworkConfigBuilder;
use reth_optimism_chainspec::{OpChainSpec, WORLDCHAIN_MAINNET};
use reth_optimism_forks::OpHardfork;
use reth_optimism_primitives::OpPrimitives;
use reth_primitives_traits::Header;
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};

/// Jovian activation timestamp on World Chain mainnet (2026-05-01 00:00:00
/// UTC). World Chain runs a delayed schedule: this is months after the shared
/// superchain Jovian (2025-12-02), and it was scheduled after the pinned
/// reth's 2026-01-16 registry snapshot, so it is absent there and added here.
const JOVIAN_TIMESTAMP: u64 = 1_777_593_600;

/// `WORLDCHAIN_MAINNET` extended with the post-snapshot Jovian hardfork.
static WORLDCHAIN_MAINNET_CURRENT: LazyLock<Arc<OpChainSpec>> = LazyLock::new(|| {
    let mut spec: OpChainSpec = (**WORLDCHAIN_MAINNET).clone();
    spec.inner.hardforks.insert(
        OpHardfork::Jovian,
        ForkCondition::Timestamp(JOVIAN_TIMESTAMP),
    );
    Arc::new(spec)
});

/// World Chain mainnet.
#[derive(Debug, Clone, Copy)]
pub struct WorldChain;

impl ChainTypes for WorldChain {
    type SignedTx = OpTxEnvelope;
    type Receipt = OpReceipt;
    type Primitives = OpPrimitives;
    type Net = BasicNetworkPrimitives<OpPrimitives, OpPooledTransaction>;
    type Spec = OpChainSpec;

    const NAME: &'static str = "world";

    fn chain_spec() -> Arc<OpChainSpec> {
        Arc::clone(&WORLDCHAIN_MAINNET_CURRENT)
    }

    fn receipts_root(receipts: &[OpReceipt], header: &Header) -> B256 {
        op_stack::receipts_root(Self::chain_spec().as_ref(), receipts, header)
    }

    fn withdrawals_valid(header: &Header, body: &alloy_consensus::BlockBody<OpTxEnvelope>) -> bool {
        op_stack::withdrawals_valid(Self::chain_spec().as_ref(), header, body)
    }

    /// World Chain discovery: identical to OP mainnet — stock discv4 (the
    /// legacy shared OP-stack DHT) plus standard discv5 with the default
    /// protocol id. Modern op-reth peers discover over discv5 with the `opel`
    /// ENR fork entry (reth sets it automatically from the OP chain spec);
    /// discv4 alone finds mostly other chains' nodes in the shared DHT.
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
    fn genesis_hash_matches_world_mainnet() {
        assert_eq!(
            WorldChain::chain_spec().genesis_hash(),
            b256!("70d316d2e0973b62332ba2e9768dd7854298d7ffe77f0409ffdb8d859f2d3fa3")
        );
    }

    #[test]
    fn jovian_is_scheduled() {
        let spec = WorldChain::chain_spec();
        assert!(spec
            .inner
            .hardforks
            .fork(OpHardfork::Jovian)
            .active_at_timestamp(JOVIAN_TIMESTAMP));
        assert!(!spec
            .inner
            .hardforks
            .fork(OpHardfork::Jovian)
            .active_at_timestamp(JOVIAN_TIMESTAMP - 1));
    }

    #[test]
    fn latest_fork_id_matches_jovian() {
        // Expected head fork-id after World Chain's Jovian (EIP-2124 CRC over
        // the genesis hash and every fork block/timestamp), verified live
        // against World Chain peers via `sieve peers --chain world`. World
        // Chain has no Karst entry in the registry, so this is the tip.
        let fork_id = WorldChain::chain_spec().latest_fork_id();
        assert_eq!(fork_id.next, 0);
        assert_eq!(fork_id.hash.0, [0x8f, 0x0e, 0x4e, 0xe7]);
    }

    #[test]
    fn bootnodes_resolve_to_op_stack_set() {
        let nodes = WorldChain::chain_spec().bootnodes().unwrap_or_default();
        assert!(!nodes.is_empty());
    }
}
