//! Shared type-safe newtypes used across the crate.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Type-safe wrapper for Ethereum block numbers.
///
/// Used at public API boundaries throughout the indexer. Scheduler internals
/// (`BinaryHeap`, `HashSet`) stay `u64` — conversion happens at the boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockNumber(u64);

impl BlockNumber {
    /// Create a new `BlockNumber`.
    #[must_use]
    pub const fn new(n: u64) -> Self {
        Self(n)
    }

    /// Return the inner `u64` value.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for BlockNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for BlockNumber {
    fn from(n: u64) -> Self {
        Self(n)
    }
}

impl From<BlockNumber> for u64 {
    fn from(bn: BlockNumber) -> Self {
        bn.0
    }
}

/// Type-safe wrapper for transaction indices within a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TxIndex(u32);

impl TxIndex {
    /// Create a new `TxIndex`.
    #[must_use]
    pub const fn new(n: u32) -> Self {
        Self(n)
    }

    /// Return the inner `u32` value.
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    /// Cast to `i32` for Postgres INTEGER bind.
    ///
    /// Safe for Ethereum — blocks never exceed `u32::MAX` transactions.
    #[must_use]
    pub const fn as_i32(self) -> i32 {
        self.0 as i32
    }

    /// Convert from `usize` (e.g. `.enumerate()`).
    #[must_use]
    pub const fn from_usize(n: usize) -> Self {
        Self(n as u32)
    }
}

impl fmt::Display for TxIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for TxIndex {
    fn from(n: u32) -> Self {
        Self(n)
    }
}

impl From<TxIndex> for u32 {
    fn from(ti: TxIndex) -> Self {
        ti.0
    }
}

/// Type-safe wrapper for log indices within a transaction receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogIndex(u32);

impl LogIndex {
    /// Create a new `LogIndex`.
    #[must_use]
    pub const fn new(n: u32) -> Self {
        Self(n)
    }

    /// Return the inner `u32` value.
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    /// Cast to `i32` for Postgres INTEGER bind.
    ///
    /// Safe for Ethereum — receipts never exceed `u32::MAX` logs.
    #[must_use]
    pub const fn as_i32(self) -> i32 {
        self.0 as i32
    }

    /// Convert from `usize` (e.g. `.enumerate()`).
    #[must_use]
    pub const fn from_usize(n: usize) -> Self {
        Self(n as u32)
    }
}

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for LogIndex {
    fn from(n: u32) -> Self {
        Self(n)
    }
}

impl From<LogIndex> for u32 {
    fn from(li: LogIndex) -> Self {
        li.0
    }
}

#[cfg(test)]
#[expect(clippy::panic_in_result_fn, reason = "assertions in tests are idiomatic")]
mod tests {
    use super::*;

    #[test]
    fn block_number_roundtrip() {
        let bn = BlockNumber::new(21_000_042);
        assert_eq!(bn.as_u64(), 21_000_042);
        assert_eq!(u64::from(bn), 21_000_042);
        assert_eq!(BlockNumber::from(42u64), BlockNumber::new(42));
    }

    #[test]
    fn block_number_display() {
        let bn = BlockNumber::new(12345);
        assert_eq!(format!("{bn}"), "12345");
    }

    #[test]
    fn block_number_ordering() {
        let a = BlockNumber::new(1);
        let b = BlockNumber::new(2);
        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, BlockNumber::new(1));
    }

    #[test]
    fn tx_index_roundtrip() {
        let ti = TxIndex::new(42);
        assert_eq!(ti.as_u32(), 42);
        assert_eq!(u32::from(ti), 42);
        assert_eq!(TxIndex::from(7u32), TxIndex::new(7));
    }

    #[test]
    fn tx_index_display() {
        let ti = TxIndex::new(99);
        assert_eq!(format!("{ti}"), "99");
    }

    #[test]
    fn tx_index_ordering() {
        let a = TxIndex::new(1);
        let b = TxIndex::new(2);
        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, TxIndex::new(1));
    }

    #[test]
    fn tx_index_from_usize() {
        let ti = TxIndex::from_usize(123);
        assert_eq!(ti.as_u32(), 123);
    }

    #[test]
    fn tx_index_as_i32() {
        let ti = TxIndex::new(42);
        assert_eq!(ti.as_i32(), 42);
    }

    #[test]
    fn log_index_roundtrip() {
        let li = LogIndex::new(42);
        assert_eq!(li.as_u32(), 42);
        assert_eq!(u32::from(li), 42);
        assert_eq!(LogIndex::from(7u32), LogIndex::new(7));
    }

    #[test]
    fn log_index_display() {
        let li = LogIndex::new(99);
        assert_eq!(format!("{li}"), "99");
    }

    #[test]
    fn log_index_ordering() {
        let a = LogIndex::new(1);
        let b = LogIndex::new(2);
        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, LogIndex::new(1));
    }

    #[test]
    fn log_index_from_usize() {
        let li = LogIndex::from_usize(456);
        assert_eq!(li.as_u32(), 456);
    }

    #[test]
    fn log_index_as_i32() {
        let li = LogIndex::new(42);
        assert_eq!(li.as_i32(), 42);
    }

    #[test]
    fn tx_index_serde_transparent() -> eyre::Result<()> {
        let ti = TxIndex::new(5);
        let json = serde_json::to_string(&ti)?;
        assert_eq!(json, "5");
        let back: TxIndex = serde_json::from_str(&json)?;
        assert_eq!(back, ti);
        Ok(())
    }

    #[test]
    fn log_index_serde_transparent() -> eyre::Result<()> {
        let li = LogIndex::new(12);
        let json = serde_json::to_string(&li)?;
        assert_eq!(json, "12");
        let back: LogIndex = serde_json::from_str(&json)?;
        assert_eq!(back, li);
        Ok(())
    }
}
