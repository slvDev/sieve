<h1 align="center">Sieve</h1>

<p align="center">
  <em>"I'm not like the rest of you. I'm stronger. I'm smarter. I'm better." — The Boys</em>
</p>

<p align="center">
  <strong>Ethereum indexer that connects directly to P2P. No RPC provider needed.</strong><br>
  100-200 blocks/sec. No API keys. No rate limits. No bills.<br>
  Just Sieve, Postgres, and an internet connection.
</p>

---

```bash
sieve add-contract 0xA0b8...eB48 --etherscan-api-key $KEY
sieve --api-port 4000
```

Sieve fetches the ABI, creates your tables, syncs the chain over P2P, and serves a GraphQL API. Zero code.

## Why

|                 | Other indexers                     | Sieve                     |
| --------------- | ---------------------------------- | ------------------------- |
| **Data source** | RPC provider ($225-$900/mo)        | Ethereum P2P network ($0) |
| **Speed**       | Provider's rate limit              | 100-200 blocks/sec        |
| **Setup**       | API keys, accounts, billing        | One-line install          |
| **Config**      | TypeScript / YAML / AssemblyScript | One TOML file             |

## Quick Start

### 1. Install

```bash
curl -fsSL https://raw.githubusercontent.com/slvDev/sieve/main/sieveup/install | bash
```

Run `sieveup` anytime to update to the latest version.

### 2. Configure

```bash
sieve init                        # creates sieve.toml and abis/
sieve add-contract 0xA0b8... \    # fetch ABI from Etherscan, add to config
  --etherscan-api-key $KEY
```

Or write the TOML yourself:

```toml
[database]
url = "postgres://postgres:sieve@localhost:5432/sieve"

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

Columns are auto-generated from the ABI. Solidity camelCase is converted to snake_case automatically (`_troveId` -> `trove_id`, `amount0Out` -> `amount0_out`). SQL reserved words are handled. You can [override columns](#contracts-and-events) if you want.

### 3. Run

```bash
sieve --api-port 4000
```

That's it. Sieve backfills from each contract's `start_block`, catches up to the chain head, then follows new blocks in real-time. One command — no separate "historical sync" and "follow mode" steps.

### 4. Query

GraphQL API with built-in GraphiQL explorer:

```graphql
{
  usdc_transfers(
    where: {
      OR: [{ from_address: "0xAbc..." }, { to_address: "0xAbc..." }]
      value_gte: "1000000000"
    }
    order_by: block_number
    order_direction: desc
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

Open `http://localhost:4000` for the GraphiQL IDE.

## What Sieve Can Index

**Event logs** -- filter by contract address, event signature, and indexed parameter values. The bread and butter.

**Function calls** -- decode transaction calldata for specific function selectors. Only successful (non-reverted) calls.

**Native ETH transfers** -- track value transfers with optional sender/receiver address filters. No ABI needed.

**Factory contracts** -- dynamically discover and index child contracts as they're deployed.

All of it configured in one TOML file. All of it stored in PostgreSQL. All of it queryable via GraphQL.

## Configuration Reference

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
start_block = 12_369_621

[contracts.factory]
address = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
event = "PoolCreated"
parameter = "pool"
```

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

## Streaming

### Webhooks

HTTP POST after each block is committed. Useful for triggering downstream pipelines or cache invalidation.

```toml
[[streams]]
name = "my_webhook"
type = "webhook"
url = "http://localhost:8080/sieve-events"  # or omit and set WEBHOOK_URL env var
backfill = false   # skip during historical sync (default: true)
```

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
url = "amqp://guest:guest@rabbitmq:5672/%2f"  # or omit and set RABBITMQ_URL env var
exchange = "sieve_events"
routing_key = "{table}.{event}"   # optional, default
backfill = false
```

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

- **Filter operators** -- `_eq`, `_ne`, `_gt`, `_gte`, `_lt`, `_lte`, `_in`, `_not_in`, `_contains`, `_starts_with`, `_ends_with`
- **Composition** -- `AND` / `OR` for complex filter logic
- **Pagination** -- cursor-based (`first`/`after`) and offset-based (`first`/`skip`)
- **Sorting** -- `order_by` + `order_direction`

## CLI

```
sieve [OPTIONS]                Run the indexer
sieve init                     Scaffold a new project (sieve.toml + abis/)
sieve schema                   Print generated SQL DDL
sieve reset                    Drop and recreate all tables
sieve inspect                  Dry-run: show tables, columns, and filters
sieve add-contract <ADDRESS>   Fetch ABI from Etherscan and add to config
sieve peers                    Test P2P connectivity (no DB or config needed)

Options:
  --config <PATH>         Path to TOML config [default: sieve.toml]
  --start-block <NUM>     Override start block
  --end-block <NUM>       Stop at this block (omit for follow mode)
  --database-url <URL>    PostgreSQL URL (or DATABASE_URL env var)
  --api-port <PORT>       Enable GraphQL API on this port
  --fresh                 Drop and recreate all tables before indexing
```

### API Endpoints

When running with `--api-port`:

| Endpoint   | Description                     |
| ---------- | ------------------------------- |
| `/`        | GraphiQL IDE                    |
| `/graphql` | GraphQL endpoint                |
| `/health`  | Liveness probe                  |
| `/ready`   | Readiness (503 during backfill) |
| `/metrics` | Prometheus metrics              |

### Environment Variables

| Variable       | Purpose                   | Overridden by                                     |
| -------------- | ------------------------- | ------------------------------------------------- |
| `DATABASE_URL` | PostgreSQL connection URL | `--database-url` flag or `[database] url` in TOML |
| `WEBHOOK_URL`  | Webhook endpoint URL      | `url` field in `[[streams]]` TOML section         |
| `RABBITMQ_URL` | RabbitMQ connection URL   | `url` field in `[[streams]]` TOML section         |

For production deployments, set secrets as environment variables instead of baking them into config files.

### `sieve add-contract`

Fetches a verified ABI from Etherscan, saves it to `abis/`, and appends a `[[contracts]]` block to your config. Auto-detects proxy contracts.

```bash
sieve add-contract 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \
  --etherscan-api-key $ETHERSCAN_API_KEY \
  --name USDC \
  --start-block 21000000
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
cp sieve.example.toml sieve.toml   # edit for your contracts
docker compose up -d
```

Starts PostgreSQL and Sieve with GraphQL API on port 4000.

### Manual build

```bash
docker build -t sieve .

docker run \
  -v ./sieve.toml:/app/sieve.toml:ro \
  -v ./abis:/app/abis:ro \
  -p 4000:4000 -p 30303:30303 -p 30303:30303/udp \
  sieve --config /app/sieve.toml --database-url postgres://... --api-port 4000
```

Config and ABIs are mounted as volumes, not baked into the image. One image works for dev, staging, and production.

> **Note:** Port 30303 (TCP + UDP) must be reachable from the internet for Ethereum P2P peer discovery.

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

Sieve syncs block headers and receipts over Ethereum's devp2p protocol, filters logs against your TOML config at sync time, decodes matched events, and writes to PostgreSQL. Everything else is discarded — you only store what you asked for.

- **Checkpoint/resume** — restarts from where it left off
- **Reorg handling** — detects reorganizations (up to 64 blocks) and rolls back affected data
- **Follow mode** — after historical sync, follows the chain head in real-time
- **Graceful shutdown** — Ctrl+C stops cleanly, progress is saved

## Acknowledgments

Sieve's P2P sync engine is built on [SHiNode](https://github.com/vicnaum/shinode), a high-performance Ethereum node that proved 1000+ blocks/sec sync over devp2p. The networking layer uses [Reth](https://github.com/paradigmxyz/reth) crates for Ethereum P2P protocol support.

## License

MIT OR Apache-2.0
