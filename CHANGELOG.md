# Changelog

## [Unreleased]

## [0.1.1] - 2026-03-11

### Fixed

- Follow-mode stall when all peers have stale head_cap (pending=1, inflight=0 indefinitely)
- Background head tracker continuously probes chain tip via P2P, overrides per-peer head_cap in follow mode
- Peer heads now updated after successful fetch (monotonic, never regresses)

## [0.1.0] - 2026-03-10

Initial release.

- P2P sync engine — connect directly to Ethereum devp2p network, no RPC needed
- TOML configuration — define contracts, events, calls, transfers in a single config file
- ABI decoding — automatic event log and calldata decoding via alloy
- PostgreSQL storage — auto-generated tables from config, atomic per-block transactions
- Indexed parameter filtering — topic-level filters to reduce noise before decoding
- Call trace indexing — decode function calldata for successful transactions
- Native ETH transfer indexing — track value transfers with address filters
- Factory contract support — dynamic child contract discovery via creation events
- Auto-generated GraphQL API — filtering, sorting, cursor/offset pagination, AND/OR composition
- Follow mode — real-time indexing after historical sync catches up
- Reorg detection — automatic rollback on chain reorganizations (64-block window)
- Webhook streaming — HTTP POST notifications per block
- RabbitMQ streaming — per-event JSON messages with routing key templates
- Prometheus metrics — blocks, events, transfers, calls counters
- Health endpoints — `/health` (liveness), `/ready` (503 during backfill, 200 when caught up)
- CLI subcommands — `init`, `schema`, `reset`, `add-contract`, `inspect`, `peers`
- Etherscan integration — `add-contract` fetches verified ABIs with proxy detection
- Docker support — multi-stage Dockerfile with dependency caching
- Checkpoint/resume — automatic progress tracking, idempotent re-processing
- Environment variable fallbacks — `DATABASE_URL`, `WEBHOOK_URL`, `RABBITMQ_URL` for production deployments
- Bloom filter pre-screening — skip receipt fetching for ~98% of blocks with no matching events
- Batched DB transactions — 64 blocks per COMMIT for faster writes
- Parallel block processing — N CPU workers for decode/filter pipeline
- AIMD batch growth — faster warmup from 32 to 128 blocks per request
- `sieveup` installer — `curl | bash` install with automatic updates
