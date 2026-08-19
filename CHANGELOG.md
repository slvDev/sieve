# Changelog

## [0.2.0] - 2026-08-19

Multichain release. Sieve now indexes four OP-Stack chains alongside Ethereum
mainnet, and every block is verified against a multi-peer canonical quorum
before it is committed.

### Breaking

- Databases created by 0.1.x are refused. 0.2.0 pins each database to its chain
  and requires a verified-frontier marker that older databases do not have. Use
  a fresh database (or `sieve reset`) when upgrading. Schema changes on a 0.2.0
  database migrate automatically from then on.

### Added

- **Multichain support** via a `chain` config key: Base (8453), OP Mainnet (10),
  Unichain (130), and World Chain (480), alongside Ethereum mainnet (default).
  Each syncs over its own devp2p network. Base uses `basev0` discovery; OP,
  Unichain, and World use discv5.
- **Canonical-header verification** before any commit: a segment's tip hash is
  confirmed by an absolute quorum of distinct peers (default 3), the full header
  chain to that tip is validated link by link, and transaction, receipt, ommer,
  and chain-specific withdrawals roots are recomputed from each block body and
  checked against the header. No quorum, no commit.
- **Contiguous commits and a verified frontier**: blocks are committed strictly
  in order, the checkpoint always equals the highest committed block, and the
  verified-frontier marker advances atomically with it. Startup re-verifies the
  frontier and recovers from a reorg that happened while Sieve was stopped.
- **Chain-bound database identity**: a database is pinned to its chain on first
  run; reusing it for a different chain is refused.
- **Factory coverage tracking**: Sieve records the block range each factory was
  active for and refuses to start when adding a factory to a database already
  indexed past its start block, lowering a start block, or changing a factory's
  creation event or parameter, all of which would silently miss children.
  `--assume-factory-coverage` adopts factories with no coverage record when
  upgrading. Factory discovery is sequenced ahead of the parallel workers so
  same-block child events are never lost.
- **Multi-file config**: split protocols into `*.sieve.toml` fragments beside the
  root config; Sieve discovers and merges them (globals stay in the root).
- **Configurable worker count**: `--workers` CLI flag / `[sync] workers` to cap
  block-processing workers when co-locating several instances on one host.
- Chain-aware `add-contract` (per-chain Etherscan) and `sieve peers --chain`.
- `[p2p] trusted_peers` config to pin always-connected archive/serving nodes.

### Changed

- The whole sync pipeline is generic over chain types, dispatched once at startup
  from the `chain` key.
- Reorg detection is now quorum-authorized (an absolute peer threshold), replacing
  the single-peer probe.
- `sieve init --docker` exposes the discv5 UDP port (30304) that OP Mainnet,
  Unichain, and World Chain need for peer discovery.
- README reworked around the multichain scope and the integrity model.

### Fixed

- Factory child events emitted in or shortly after the child's creation block are
  no longer dropped under parallel processing; discovery is sequenced ahead of
  the workers.
- Withdrawals fields are validated per chain and fork for OP-Stack headers.

## [0.1.5] - 2026-03-15

### Added

- Pretty terminal UI: startup banner, animated spinners, progress bar with ETA, follow status with block age (suppressed with `--verbose`)
- `--verbose` / `-v` flag to use tracing logs instead of pretty UI
- `[api].port` TOML config for GraphQL API (omit to disable, `--api-port` overrides)
- `.env` file support via `dotenvy` — `DATABASE_URL` loaded automatically
- `sieve init` now creates `.env` with default database URL
- `add-contract` auto-fetches deploy block from Etherscan as `start_block` (override with `--start-block`)
- `[p2p].port` TOML config and `--p2p-port` CLI flag to override default 30303

### Changed

- **Breaking:** all sensitive URLs removed from TOML — use `.env` file instead (`DATABASE_URL`, `WEBHOOK_URL`, `RABBITMQ_URL`)
- `--fresh` log downgraded from warn to info (hidden in pretty mode, visible with `--verbose`)
- Prettified `sieve init` and `add-contract` CLI output
- `docker-compose.yml` uses `${VAR}` interpolation from `.env` (works with Coolify and other platforms)

### Fixed

- GraphQL API now serves on `/graphql` path (was only `/`)
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
