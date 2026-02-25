//! Index configuration: which contracts, events, and handlers to run.
//!
//! Loaded from a config file (TOML or Rust-defined).
//! Example:
//! ```ignore
//! IndexConfig {
//!     contracts: vec![ContractIndex {
//!         name: "UniswapV3Pool",
//!         address: address!("..."),
//!         abi: include_str!("../abis/UniswapV3Pool.json"),
//!         events: vec!["Swap", "Mint", "Burn"],
//!         start_block: 12_369_621,
//!     }],
//! }
//! ```
