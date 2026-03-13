# Changelog

## [Unreleased]

### Fixed

- Follow-mode peer eviction: `mark_peer_success` was dead code, causing all peers to be evicted after 120s idle at the tip (peers=0 loop)
- Head probe responses now refresh peer liveness, preventing eviction during idle periods

## [0.1.4] - 2026-03-11

### Changed

- Docker images built from pre-compiled binaries instead of compiling in CI (~30s vs 40+ min)

## [0.1.3] - 2026-03-11

### Added

- `--version` / `-V` flag to CLI
- `sieve init` now creates a working USDC Transfer config with ERC20 ABI (plug and play)
- `sieve init --docker` generates a docker-compose.yml with PostgreSQL and healthcheck
- Multi-platform Docker images (linux/amd64 + linux/arm64)

### Changed

- `sieve init` no longer creates docker-compose.yml by default (use `--docker`)

### Fixed

- Release workflow: filter artifacts to skip Docker buildx cache metadata

## [0.1.2] - 2026-03-11

### Fixed

- Use default P2P port 30303 instead of random ephemeral ports (enables inbound peer connections)

### Changed

- Docker builds use cargo-chef for cached dependency compilation (~2-5 min rebuilds vs 20-25 min)
- docker-compose.yml uses pre-built GHCR image instead of building from source
- Added health check and restart policy to docker-compose.yml

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
