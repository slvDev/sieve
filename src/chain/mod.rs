//! Chain abstraction.
//!
//! Each supported chain provides a compile-time bundle of wire types and
//! chain-specific behavior via [`ChainTypes`]. The sync pipeline is generic
//! over this trait and monomorphized per chain; `main` selects the concrete
//! chain at runtime by dispatching on [`ChainKind`] parsed from config.

mod base;
mod ethereum;

pub use base::BaseChain;
pub use ethereum::EthereumChain;

use alloy_consensus::{BlockBody, TxReceipt};
use alloy_primitives::B256;
use reth_chainspec::{EthChainSpec, Hardforks};
use reth_eth_wire_types::{NetPrimitivesFor, NetworkPrimitives};
use reth_network::config::NetworkConfigBuilder;
use reth_primitives_traits::{Header, NodePrimitives, SignedTransaction};
use std::net::SocketAddr;
use std::sync::Arc;

/// Which chain to index, parsed from the TOML `chain = "..."` key.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ChainKind {
    /// Ethereum mainnet (default).
    #[default]
    Mainnet,
    /// Base mainnet (OP-stack L2).
    Base,
}

impl ChainKind {
    /// Canonical chain name (matches the corresponding [`ChainTypes::NAME`]).
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Mainnet => EthereumChain::NAME,
            Self::Base => BaseChain::NAME,
        }
    }

    /// Genesis hash of the selected chain.
    #[must_use]
    pub fn genesis_hash(self) -> B256 {
        match self {
            Self::Mainnet => EthereumChain::chain_spec().genesis_hash(),
            Self::Base => BaseChain::chain_spec().genesis_hash(),
        }
    }

    /// Parse a chain name from config.
    ///
    /// # Errors
    ///
    /// Returns an error for unrecognized chain names.
    pub fn parse(name: &str) -> eyre::Result<Self> {
        match name.to_ascii_lowercase().as_str() {
            "mainnet" | "ethereum" => Ok(Self::Mainnet),
            "base" => Ok(Self::Base),
            other => Err(eyre::eyre!(
                "unknown chain \"{other}\" (supported: mainnet, base)"
            )),
        }
    }
}

/// Compile-time bundle of chain-specific types and behavior.
///
/// Both supported chains share the Ethereum block header and the alloy
/// [`BlockBody`] layout; they differ in transaction envelope (Base carries
/// type-0x7E deposit transactions), receipt type, wire primitives, chain
/// spec (genesis, forkid, bootnodes), and receipts-root computation.
pub trait ChainTypes: Send + Sync + Sized + 'static {
    /// Signed transaction type carried in block bodies.
    type SignedTx: SignedTransaction;
    /// Receipt type served over the wire.
    type Receipt: TxReceipt<Log = alloy_primitives::Log> + Unpin + 'static;
    /// Node-level primitive bundle (used for the noop storage provider
    /// backing the request handler).
    type Primitives: NodePrimitives<
        BlockHeader = Header,
        BlockBody = BlockBody<Self::SignedTx>,
        SignedTx = Self::SignedTx,
        Receipt = Self::Receipt,
    >;
    /// Wire-protocol type bundle for `reth-network`.
    type Net: NetworkPrimitives<
            BlockHeader = Header,
            BlockBody = BlockBody<Self::SignedTx>,
            Receipt = Self::Receipt,
        > + NetPrimitivesFor<Self::Primitives>;
    /// Chain spec used for the handshake (genesis, forkid) and bootnodes.
    type Spec: EthChainSpec + Hardforks + Send + Sync + 'static;

    /// Chain name for logging.
    const NAME: &'static str;

    /// The chain spec singleton.
    fn chain_spec() -> Arc<Self::Spec>;

    /// Compute the receipts root committed to in `header` from wire
    /// receipts, recomputing blooms from logs (never trusting the peer).
    ///
    /// Takes the header because OP chains hash deposit receipts
    /// differently depending on the block timestamp (Canyon activation).
    fn receipts_root(receipts: &[Self::Receipt], header: &Header) -> B256;

    /// Apply chain-specific network configuration (discovery transports,
    /// boot nodes) to the builder. `listen_addr` is the RLPx listen socket.
    ///
    /// Default: no changes (mainnet uses reth's stock discv4 + discv5).
    fn configure_network(
        builder: NetworkConfigBuilder<Self::Net>,
        listen_addr: SocketAddr,
    ) -> NetworkConfigBuilder<Self::Net> {
        let _ = listen_addr;
        builder
    }
}

#[cfg(test)]
mod tests {
    use super::ChainKind;

    #[test]
    fn parse_known_chains() {
        assert!(matches!(
            ChainKind::parse("mainnet"),
            Ok(ChainKind::Mainnet)
        ));
        assert!(matches!(
            ChainKind::parse("Ethereum"),
            Ok(ChainKind::Mainnet)
        ));
        assert!(matches!(ChainKind::parse("base"), Ok(ChainKind::Base)));
    }

    #[test]
    fn parse_unknown_chain_errors() {
        let result = ChainKind::parse("dogecoin");
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("unknown chain"));
    }
}
