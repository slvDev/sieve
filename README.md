<h1 align="center">Sieve</h1>

<p align="center">
  <strong>Self-hosted indexer for Ethereum and the OP-Stack. Connects straight to the P2P network, no RPC provider.</strong><br>
  ~1000 blocks/sec on mainnet. No RPC keys. No rate limits. No bills.<br>
  Just Sieve, Postgres, and an internet connection.
</p>

<p align="center">
  Ethereum · Base · OP Mainnet · Unichain · World Chain
</p>

---

```bash
sieve add-contract 0xA0b8...eB48
sieve
```

Sieve fetches the ABI, creates your tables, syncs the chain over P2P, and serves a GraphQL API. Zero code.

## Why

|                 | Other indexers                     | Sieve                     |
| --------------- | ---------------------------------- | ------------------------- |
| **Data source** | RPC provider ($225-$900/mo)        | P2P network ($0)          |
| **Speed**       | Provider's rate limit              | ~1000 blocks/sec (mainnet) |
| **Setup**       | API keys, accounts, billing        | One-line install          |
| **Config**      | TypeScript / YAML / AssemblyScript | One TOML file             |
| **Chains**      | Per-provider                       | Ethereum + OP-Stack (5)   |

*Benchmarked on a Hetzner dedicated server, Ethereum mainnet, 11 contracts, 29 events, RabbitMQ streaming active.*

## Quick Start

### 1. Install

```bash
curl -fsSL https://raw.githubusercontent.com/slvDev/sieve/main/sieveup/install | bash
```

Run `sieveup` anytime to update to the latest version.

### 2. Configure

```bash
sieve init                        # creates sieve.toml, .env, abis/erc20.json (USDC Transfer, ready to run)
sieve add-contract 0xA0b8...      # or fetch any contract ABI from Etherscan
```

`sieve init` creates a working USDC Transfer config out of the box, plug and play. Add `--docker` to also generate a `docker-compose.yml` with PostgreSQL.

Set `ETHERSCAN_API_KEY` in `.env` for `add-contract` to work.

Or write the TOML yourself:

```toml
[[contracts]]
name = "USDC"
address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
abi = "abis/erc20.json"
start_block = 21_000_000

[[contracts.events]]
name = "Transfer"
table = "usdc_transfers"
context = ["block_timestamp", "tx_from"]
```

Sensitive URLs go in `.env` (auto-loaded at startup):

```env
DATABASE_URL=postgres://postgres:sieve@localhost:5432/sieve
```

Columns are auto-generated from the ABI. Solidity camelCase is converted to snake_case automatically (`_troveId` -> `trove_id`, `amount0Out` -> `amount0_out`). SQL reserved words are handled. You can [override columns](#contracts-and-events) if you want.

### 3. Run

```bash
sieve
```

That's it. Sieve backfills from each contract's `start_block`, catches up to the chain head, then follows new blocks in real-time. One command, no separate "historical sync" and "follow mode" steps.

### 4. Query

GraphQL API with built-in GraphiQL explorer:

```graphql
{
  usdc_transfers(
    where: {
      OR: [{ from_address: "0xAbc..." }, { to_address: "0xAbc..." }]
      value_gte: "1000000000"
    }
    orderBy: block_number
    orderDirection: desc
    first: 50
  ) {
    block_number
    tx_hash
    from_address
    to_address
    value
    block_timestamp
  }
}
```

Open `http://localhost:4000/graphql` for the GraphiQL IDE.

## What Sieve Can Index

**Event logs** -- filter by contract address, event signature, and indexed parameter values. The bread and butter.

**Function calls** -- decode top-level transaction calldata for specific function selectors. Only successful (non-reverted) calls.

**Native ETH transfers** -- track top-level transaction value with optional sender/receiver address filters. No ABI needed.

**Factory contracts** -- dynamically discover and index child contracts as they're deployed.

All of it configured in one TOML file. All of it stored in PostgreSQL. All of it queryable via GraphQL.

## Integrity

Sieve fetches blocks from anonymous P2P peers, so it never trusts any single one. Before a block is committed:

- **Multi-peer header quorum.** The canonical hash of each segment is confirmed by an absolute number of distinct peers (default 3). Empty or timed-out replies never count toward the quorum.
- **Canonical-chain validation.** The full header chain up to the agreed tip is fetched and checked link by link (every parent hash, ending on the quorum hash).
- **Payload verification.** Transaction, receipt, ommer, and chain-specific withdrawals roots are recomputed from the block body and matched against the header. A forged body is rejected and re-fetched from another peer.
- **Contiguous commits.** Blocks are written in strict order and the checkpoint always equals the highest committed block, so a crash or kill never leaves holes or half-written ranges.
- **Chain-bound database.** A database is pinned to its chain on first run; reusing it for a different chain is refused.
- **Quorum-authorized reorgs.** Reorgs (up to 64 blocks) roll back only when a peer quorum agrees on the new canonical tip.

No quorum, no commit. Sieve stops rather than write unverified data.

## Configuration Reference

### Chains

Sieve indexes Ethereum mainnet by default, plus four OP-Stack chains:

| Chain      | ID   | `chain =`               | Discovery            |
| ---------- | ---- | ----------------------- | -------------------- |
| Ethereum   | 1    | `"mainnet"` (default)   | discv4 / discv5      |
| Base       | 8453 | `"base"`                | `basev0` discv5      |
| OP Mainnet | 10   | `"optimism"` / `"op"`   | discv4 + discv5      |
| Unichain   | 130  | `"unichain"` / `"uni"`  | discv4 + discv5      |
| World Chain| 480  | `"world"`               | discv4 + discv5      |

```toml
chain = "base"   # default: "mainnet"
```

A database is bound to its chain on first run. Reusing a mainnet database
with `chain = "base"` (or any other mismatch) is refused, so chain state can
never mix. Use a separate database per chain, or wipe with `sieve reset` /
`--fresh`.

OP-stack notes (Base, OP mainnet, Unichain, and World Chain):

- Blocks arrive over the chain's devp2p network directly (no RPC, same as
  mainnet). Sieve follows the sequencer's unsafe head, typically within a
  block or two (~1-2s blocks). Base runs its own partitioned `basev0` peer
  discovery, which Sieve speaks; OP mainnet, Unichain, and World Chain use
  standard superchain discovery.
- OP-stack deposit transactions (type `0x7E`) are indexed like any other
  transaction: events decode normally, and user deposits with value show
  up in native transfer tables (the sender is the deposit's `from`
  address). The per-block L1-attributes deposit carries no value and is
  skipped by transfer indexing.
- Peer sets on OP-Stack chains are thinner than mainnet, and many public nodes
  prune receipts (on Base, to roughly a month). For historical backfills, pin
  archive/serving nodes via `trusted_peers` (below). Sieve automatically avoids
  asking peers for history they advertise as pruned.
- Test connectivity without a config: `sieve peers --chain base` /
  `sieve peers --chain optimism` / `sieve peers --chain unichain` /
  `sieve peers --chain world`.
- Known limitation: Base's unscheduled "Cobalt" hardfork will introduce a
  new transaction type (`0x79`) and can be activated via L1 signalling.
  When it is scheduled, Sieve will need an update to keep following Base.
  Similarly, future OP-stack hardforks that change the fork-id require a
  Sieve update to keep the handshake current.

```toml
[p2p]
# Always-connected peers (e.g. an archive node for deep backfills):
trusted_peers = ["enode://<pubkey>@<ip>:<port>"]
```

### Running Several Instances on One Host

Sieve indexes exactly one chain per process (the pipeline is compiled for the
chain in `chain =`). To index multiple chains, run one instance per chain,
each with all the protocols you want on that chain (see *Splitting Config
Across Files* below). Two things need distinct values so co-located instances
don't collide:

- **Ports.** Each instance binds RLPx (TCP) and discovery (UDP) on its P2P port,
  plus its API port. OP Mainnet, Unichain, and World Chain also bind discv5 on
  UDP `port + 1`; Base runs `basev0` discv5 on the same UDP port. Give each
  instance a distinct `--p2p-port` (space them by 2) and `--api-port`, or run one
  container per instance so each gets its own network namespace. The port number
  itself has no effect on sync speed; discovery advertises whatever port you pick.
- **Workers.** Each instance spawns one block-processing worker per CPU core by
  default. Packing several instances on one box oversubscribes the cores, so
  cap the count per instance:

  ```toml
  [sync]
  workers = 4      # default: CPU count. Also settable via --workers.
  ```

  Rule of thumb: keep the sum of `workers` across co-located instances at or
  below the host's core count. The remaining shared resource is egress
  bandwidth. Sync speed still depends on peer count and how many peers serve
  receipts, not on the port.

### Contracts and Events

```toml
[[contracts]]
name = "MyContract"
address = "0x..."
abi = "abis/my_contract.json"
start_block = 21_000_000
include_receipts = true     # adds gas_used, nonce, cumulative_gas_used, status columns

[[contracts.events]]
name = "Transfer"
table = "my_transfers"
context = [                 # optional block/tx metadata columns
  "block_timestamp",
  "block_hash",
  "tx_from",
  "tx_to",
  "tx_value",
  "tx_gas_price",
  # auto-added by include_receipts = true:
  # "tx_gas_used", "tx_nonce", "cumulative_gas_used", "tx_status"
]
columns = [                 # optional -- auto-generated from ABI if omitted
  { param = "from",  name = "sender",   type = "text" },
  { param = "to",    name = "receiver", type = "text" },
  { param = "value", name = "amount",   type = "numeric" },
]

# filter by indexed parameters (only index specific values)
[contracts.events.filter]
spender = ["0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD"]
```

### Function Call Indexing

```toml
[[contracts.calls]]
name = "transfer"
table = "usdc_transfer_calls"
context = ["block_timestamp", "tx_from"]
columns = [
  { param = "to",    name = "to_address", type = "text" },
  { param = "value", name = "value",      type = "numeric" },
]
```

### Native ETH Transfers

```toml
[[transfers]]
name = "eth_transfers"
table = "eth_transfers"
start_block = 21_000_000
context = ["block_timestamp", "tx_gas_price"]
include_receipts = true

[transfers.filter]
from = ["0x28C6c06298d514Db089934071355E5743bf21d60"]  # Binance hot wallet
```

### Factory Contracts

```toml
[[contracts]]
name = "UniswapV3Pool"
abi = "abis/uniswap_v3_pool.json"

# Discover child pools from the factory's creation event. start_block
# belongs here (children inherit it), not on [[contracts]].
[contracts.factory]
address = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
event = "PoolCreated"
param = "pool"
start_block = 12_369_621

[[contracts.events]]
name = "Swap"
table = "uniswap_v3_swaps"
```

Sieve tracks which block ranges each factory was active for. Adding a factory to a database already indexed past its `start_block`, lowering its `start_block`, changing its creation event or `param`, starting sync past its uncovered range, or re-adding a factory that was removed while indexing continued would all silently miss children or their events -- Sieve refuses to start instead and tells you to use a fresh database or `sieve reset`. When upgrading a database created before coverage tracking, run once with `--assume-factory-coverage` to record the current state as covered; it only applies to factories with no coverage record (asserting they were configured continuously) -- every other refusal stands.

### Receipt Context Fields

`include_receipts = true` on a contract or transfer auto-adds these columns:

| Context Field         | SQL Type  | Description                                                        |
| --------------------- | --------- | ------------------------------------------------------------------ |
| `tx_gas_used`         | `BIGINT`  | Per-transaction gas used                                           |
| `tx_nonce`            | `BIGINT`  | Transaction nonce                                                  |
| `cumulative_gas_used` | `BIGINT`  | Cumulative gas used in block up to this tx                         |
| `tx_status`           | `BOOLEAN` | Receipt success (always true -- Sieve only indexes successful txs) |

Also enriches streaming payloads with `tx_value`, `tx_gas_price`, `gas_used`, `nonce`, `cumulative_gas_used`, and `status`. Lets downstream consumers compute transaction costs without an RPC node.

Individual receipt fields can be added without the flag: `context = ["tx_gas_used", "tx_nonce"]`.

### Splitting Config Across Files

Indexing many protocols in one `sieve.toml` gets unwieldy. You can split each
protocol into its own `*.sieve.toml` file next to the root config. Sieve
automatically finds every `*.sieve.toml` file in the **same directory** as the
config passed to `--config` (default `sieve.toml`) and merges them.

```
project/
  sieve.toml          # globals + optional contracts
  aave.sieve.toml     # [[contracts]] / [[transfers]]
  uniswap.sieve.toml  # [[contracts]] / [[transfers]]
  abis/
    erc20.json
    pool.json
```

- **Globals live in the root only.** `chain`, `[api]`, `[p2p]`, `[sync]`, and
  `[[streams]]` may appear only in the root config. A protocol fragment may
  contain only `[[contracts]]` and `[[transfers]]`. Any global key in a
  fragment is a startup error.
- **The root may still hold contracts.** A single-file `sieve.toml` keeps
  working exactly as before; fragments are purely additive. The root can be
  globals-only, with every contract in fragments.
- **Names must be unique across all files.** A contract name defined in two
  files is a startup error naming both files; duplicate table names are also
  rejected at startup.
- **ABI paths resolve relative to the config directory** (the shared `abis/`
  above), the same as in a single file.
- Merging applies to the indexer and the config-resolving commands: `sieve
  schema`, `sieve inspect`, and `sieve reset`. (`sieve peers` and `sieve
  add-contract` intentionally read only the root config.)

```toml
# aave.sieve.toml -- no chain/api/p2p/sync/streams here
[[contracts]]
name = "AavePool"
address = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
abi = "abis/aave_pool.json"
start_block = 16_291_127

[[contracts.events]]
name = "Supply"
table = "aave_supplies"
```

> `sieve add-contract` still appends to the root config file; move the entry
> into a fragment by hand if you want it grouped with a protocol.

## Streaming

### Webhooks

HTTP POST after each block is committed. Useful for triggering downstream pipelines or cache invalidation.

```toml
[[streams]]
name = "my_webhook"
type = "webhook"
backfill = false   # skip during historical sync (default: true)
```

Set `WEBHOOK_URL` in `.env`.

Payload:

```json
{
  "block_number": 22516100,
  "block_timestamp": 1700000000,
  "tables": [
    { "name": "usdc_transfers", "event": "Transfer", "count": 3 },
    { "name": "eth_transfers", "event": "transfer", "count": 1 }
  ]
}
```

### RabbitMQ

Per-event JSON messages to an AMQP exchange with configurable routing keys.

```toml
[[streams]]
name = "rabbitmq_events"
type = "rabbitmq"
exchange = "sieve_events"
routing_key = "{table}.{event}"   # optional, default
backfill = false
```

Set `RABBITMQ_URL` in `.env`.

Message payload (one per event):

```json
{
  "table": "usdc_transfers",
  "event": "Transfer",
  "contract_name": "USDC",
  "contract": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
  "block_number": 22516100,
  "block_timestamp": 1700000000,
  "tx_hash": "0xabc...",
  "log_index": 5,
  "tx_index": 42,
  "tx_from": "0x1234...5678",
  "data": {
    "from": "0xDead...beef",
    "to": "0xCafe...babe",
    "value": "1000000"
  }
}
```

**Note:** `data` keys use raw ABI parameter names, not your TOML column names. If the Solidity ABI defines `_troveId`, the stream sends `_troveId` -- even though PostgreSQL stores it as `trove_id`. ABI names are immutable; config names can change.

Receipt fields (`tx_value`, `tx_gas_price`, `gas_used`, `nonce`, `cumulative_gas_used`, `status`) are included when `include_receipts = true`.

Both webhook and RabbitMQ streams can run simultaneously. Both are best-effort -- failures are logged, never block indexing. Lazy connection for RabbitMQ (auto-reconnect on failure).

## GraphQL API

Auto-generated from your TOML config. Every table gets:

- **Filter operators** -- `_eq`, `_ne`, `_gt`, `_gte`, `_lt`, `_lte`, `_in`, `_not_in`, `_contains`, `_starts_with`
- **Composition** -- `AND` / `OR` for complex filter logic
- **Pagination** -- cursor-based (`first`/`after`) and offset-based (`first`/`skip`)
- **Sorting** -- `orderBy` + `orderDirection`

## CLI

```
sieve [OPTIONS]                Run the indexer
sieve init                     Scaffold a new project (sieve.toml, .env, abis/)
sieve init --docker            Same + docker-compose.yml with PostgreSQL
sieve schema                   Print generated SQL DDL
sieve reset                    Drop and recreate all tables
sieve inspect                  Dry-run: show tables, columns, and filters
sieve add-contract <ADDRESS>   Fetch ABI from Etherscan and add to config
sieve peers [--chain <NAME>]   Test P2P connectivity (no DB or config needed)

Options:
  --config <PATH>             Path to TOML config [default: sieve.toml]
  --start-block <NUM>         Override start block
  --end-block <NUM>           Stop at this block (omit for follow mode)
  --database-url <URL>        PostgreSQL URL (or DATABASE_URL in .env)
  --api-port <PORT>           Override GraphQL API port (configurable in TOML)
  --p2p-port <PORT>           Override P2P listen port [default: 30303]
  --workers <NUM>             Block-processing workers [default: CPU count]
  --fresh                     Drop and recreate all tables before indexing
  --assume-factory-coverage   Adopt factories with no coverage record (DB upgrade)
  -v, --verbose               Use tracing logs instead of pretty UI
  -V, --version               Print version
```

### API Endpoints

When API is enabled (`[api] port` in TOML or `--api-port`):

| Endpoint   | Description                     |
| ---------- | ------------------------------- |
| `/`        | GraphiQL IDE                    |
| `/graphql` | GraphQL endpoint                |
| `/health`  | Liveness probe                  |
| `/ready`   | Readiness (503 during backfill) |
| `/metrics` | Prometheus metrics              |

### Environment Variables

All sensitive URLs live in `.env` (auto-loaded at startup via `dotenvy`). Never in TOML.

| Variable             | Purpose                   | Override          |
| -------------------- | ------------------------- | ----------------- |
| `DATABASE_URL`       | PostgreSQL connection URL | `--database-url`  |
| `WEBHOOK_URL`        | Webhook endpoint URL      | n/a               |
| `RABBITMQ_URL`       | RabbitMQ connection URL   | n/a               |
| `ETHERSCAN_API_KEY`  | Etherscan API key         | `--etherscan-api-key` |

### `sieve add-contract`

Fetches a verified ABI from Etherscan, saves it to `abis/`, and appends a `[[contracts]]` block to your config. Auto-detects proxy contracts. Automatically sets `start_block` to the contract's deploy block.

```bash
sieve add-contract 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
sieve add-contract 0xA0b8... --name USDC              # override name
sieve add-contract 0xA0b8... --start-block 21000000   # override start block
```

### `sieve inspect`

Validates your config without a database connection. Shows what tables, columns, context fields, and filters would be created.

```bash
sieve inspect --config sieve.toml
```

### `sieve peers`

Tests P2P connectivity without a database or config. Reports peer count and chain head every 5 seconds. Useful for diagnosing Docker/NAT issues.

```bash
sieve peers
# peers=5 best_head=22525078
# peers=8 best_head=22525090
```

## Docker

### Docker Compose

```bash
sieve init --docker               # creates sieve.toml, .env, abis/, and docker-compose.yml
docker compose up -d              # starts PostgreSQL + Sieve
```

Starts PostgreSQL and Sieve with GraphQL API on port 4000. Credentials come from `.env` (auto-loaded by Docker Compose). Edit `sieve.toml` for your contracts, or use the default USDC Transfer config.

### Manual build

```bash
docker build -t sieve .

docker run \
  -v ./sieve.toml:/app/sieve.toml:ro \
  -v ./abis:/app/abis:ro \
  -p 4000:4000 -p 30303:30303 -p 30303:30303/udp \
  -p 30304:30304/udp \
  sieve --database-url postgres://... --api-port 4000
```

Config and ABIs are mounted as volumes, not baked into the image. One image works for dev, staging, and production.

> **Ports:** 30303 (TCP + UDP) carries RLPx and discovery. OP Mainnet, Unichain,
> and World Chain also use UDP 30304 for discv5; Base uses 30303/UDP for its
> `basev0` discovery. Expose the ports your chain needs for peer discovery.
>
> **Public API:** the GraphQL server has open CORS and no built-in auth. Put it
> behind a reverse proxy (TLS, access control, rate limiting) before exposing it
> to the internet.

## How It Works

```
Ethereum P2P Network
       |
       v
  Sync Engine (parallel workers, bloom filter pre-screening)
       |
       |-----------------+-----------------+
       v                 v                 v
  Event Filter     Call Scanner     Transfer Scanner
       |                 |                 |
       v                 v                 |
  ABI Decoder      ABI Decoder            |
       |                 |                 |
       +-----------------+-----------------+
                         |
                         v
                   PostgreSQL -------> Webhooks / RabbitMQ
                         |
                         v
                   GraphQL API
```

Sieve syncs block headers and receipts over the chain's devp2p protocol, filters logs against your TOML config at sync time, decodes matched events, and writes to PostgreSQL. Unmatched log and payload data is discarded. You store the events you asked for, plus the block hashes and checkpoints Sieve keeps to verify the chain and resume cleanly.

- **One command:** backfill, catch-up, and live head-following, no separate modes
- **Checkpoint/resume:** restarts exactly where it left off (see [Integrity](#integrity))
- **Follow mode:** after historical sync, follows the chain head in real-time
- **Graceful shutdown:** Ctrl+C stops cleanly, progress is saved

## FAQ

**Can I filter by non-indexed event parameters (e.g., an address in the log data)?**

Not currently. Sieve filters at sync time using Ethereum log topics (topic0–topic3), which only contain indexed parameters. Non-indexed parameters are decoded and stored in Postgres, but can't be filtered before insertion. You can filter them after the fact using SQL or the GraphQL API. A post-decode value filter is on the roadmap.

**What happens if two contracts emit events with the same name but different parameters?**

No collision. Topic0 is the keccak256 hash of the full event signature including parameter types, so `Transfer(address,address,uint256)` and `Transfer(address,address,uint256,uint256)` produce different topic0 hashes. Sieve also filters by contract address first, so even identical events on different contracts are fully isolated. If you see decode warnings, it's likely a mismatched ABI (e.g., a proxy contract forwarding events with a different signature than the ABI specifies).

**Can I run multiple Sieve instances on the same machine?**

Yes. Each instance needs its own P2P port, database, and config. Use `--p2p-port` or `[p2p] port` in TOML to avoid port conflicts. Speed is not affected; Sieve discovers peers outbound.

## Acknowledgments

Sieve's P2P sync engine is built on [SHiNode](https://github.com/vicnaum/shinode), a high-performance Ethereum node that proved 1000+ blocks/sec sync over devp2p. The networking layer uses [Reth](https://github.com/paradigmxyz/reth) crates for Ethereum P2P protocol support.

## License

MIT OR Apache-2.0
