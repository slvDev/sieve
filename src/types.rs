//! Shared type-safe newtypes used across the crate.

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

#[cfg(test)]
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
}
