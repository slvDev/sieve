# TODO

Deferred code quality improvements.

## Documentation
- Field-level doc comments on all public struct fields
- `# Errors` and `# Examples` sections on public functions
- Module-level architecture overview docs

## Newtypes
- `derive_more` crate for Display/From derives on newtypes
- `TxIndex(usize)` newtype for transaction indices
- `LogIndex(usize)` newtype for log indices

## Observability
- Prometheus metrics (blocks/sec, events/sec, peer counts, fetch latencies)
- Structured error types instead of string-based eyre errors

## Testing
- E2E integration tests with a local Ethereum devnet
- Property-based tests for scheduler and filter logic
- Benchmark suite for filter/decode hot path
