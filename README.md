# Sieve

An Ethereum event indexer that connects directly to the P2P network. No RPC provider needed.

Sieve syncs block headers and receipts over Ethereum's devp2p protocol at 1000+ blocks/sec, filters for the events you care about, decodes them, and writes to PostgreSQL. Then it serves an auto-generated GraphQL API on top.

**Zero infrastructure cost.** No Alchemy. No Infura. No full node. Just Sieve, Postgres, and an internet connection.

## Why Sieve

Every existing indexer — Ponder, rindexer, The Graph — requires an RPC endpoint. That means either running a full node (hundreds of GBs, ongoing maintenance) or paying a provider (rate limits, costs that scale with usage).

Sieve skips all of that. It speaks Ethereum's native P2P protocol directly, the same way nodes talk to each other. You get:

- **Fast sync** — 1000+ blocks/sec from the P2P network, proven by the [SHiNode](https://github.com/AantonC/SHiNode) engine it's built on
- **Minimal storage** — only the events you define are stored. Full mainnet history in MBs, not hundreds of GBs
- **No RPC bills** — connect to Ethereum peers directly, no API keys or rate limits
- **Simple config** — one TOML file defines what to index. No code to write
- **Production-ready** — auto-generated GraphQL API, Prometheus metrics, health probes, cursor pagination, reorg handling, webhook streaming

## Quick Start

### Prerequisites

- Rust 1.91+ (stable)
- PostgreSQL 16+
- Port 30303 open (TCP + UDP) for Ethereum P2P

### 1. Build

```bash
git clone https://github.com/example/sieve.git
cd sieve
cargo build --release
```

### 2. Create your config

Copy the example and edit it for your contracts:

```bash
cp sieve.example.toml sieve.toml
```

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
columns = [
  { param = "from",  name = "from_address", type = "text" },
  { param = "to",    name = "to_address",   type = "text" },
  { param = "value", name = "value",        type = "numeric" },
]
```

Place your ABI JSON files in the `abis/` directory. If columns are omitted, Sieve auto-generates them from the ABI with automatic snake_case conversion (e.g. `_troveId` → `trove_id`, `amount0Out` → `amount0_out`).

### 3. Run

```bash
# Historical sync (specific range)
./target/release/sieve --config sieve.toml --start-block 21000000 --end-block 21001000

# Follow mode (sync history then follow new blocks)
./target/release/sieve --config sieve.toml --api-port 4000
```

### 4. Query

With `--api-port`, Sieve serves a GraphQL API with a built-in GraphiQL explorer:

```
http://localhost:4000/          # GraphiQL IDE
http://localhost:4000/graphql   # GraphQL endpoint
http://localhost:4000/health    # Liveness probe
http://localhost:4000/ready     # Readiness probe (503 during backfill)
http://localhost:4000/metrics   # Prometheus metrics
```

```graphql
{
  usdc_transfers(
    where: { from_address: "0xDead...beef" }
    order_by: block_number
    order_direction: desc
    first: 10
  ) {
    block_number
    from_address
    to_address
    value
  }
}
```

## Configuration Reference

### Contracts and Events

```toml
[[contracts]]
name = "MyContract"
address = "0x..."
abi = "abis/my_contract.json"
start_block = 21_000_000

# Index event logs
[[contracts.events]]
name = "Transfer"           # Event name from ABI
table = "my_transfers"      # PostgreSQL table name
context = [                 # Optional block/tx metadata columns
  "block_timestamp",
  "block_hash",
  "tx_from",
  "tx_to",
  "tx_value",
  "tx_gas_price",
]
columns = [                 # Optional column mapping (auto-generated if omitted)
  { param = "from",  name = "sender",   type = "text" },
  { param = "to",    name = "receiver", type = "text" },
  { param = "value", name = "amount",   type = "numeric" },
]
# Column names are auto-converted to snake_case from Solidity camelCase.
# SQL reserved words (from, to, value) and names that collide with
# built-in columns (id, block_number, tx_hash, etc.) are handled
# automatically — reserved words are quoted, collisions are prefixed.

# Optional: filter by indexed parameters (only index specific values)
[contracts.events.filter]
spender = ["0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD"]
```

### Function Call Indexing

Index top-level function calls by decoding transaction calldata. Only successful (non-reverted) calls are stored.

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

Track ETH transfers (non-zero value transactions) without any contract ABI:

```toml
[[transfers]]
name = "eth_transfers"
table = "eth_transfers"
start_block = 21_000_000
context = ["block_timestamp", "tx_gas_price"]

# Optional: filter by sender/receiver
[transfers.filter]
from = ["0x28C6c06298d514Db089934071355E5743bf21d60"]  # Binance hot wallet
```

### Factory Contracts

Automatically discover and index child contracts deployed by a factory:

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

### Webhook Streaming

Get notified via HTTP POST after each block is committed to the database. Useful for triggering materialized view refreshes, downstream pipelines, or cache invalidation.

```toml
[[streams]]
name = "my_webhook"
type = "webhook"
url = "http://localhost:8080/sieve-events"
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

- Best-effort delivery (failures are logged, never block indexing)
- `backfill = false` skips notifications during historical sync
- Multiple webhooks supported — each gets every block notification

### RabbitMQ Streaming

Publish each decoded event, function call, or transfer as an individual JSON message to a RabbitMQ exchange. Each message includes the full decoded data, routed by a configurable routing key.

```toml
[[streams]]
name = "rabbitmq_events"
type = "rabbitmq"
url = "amqp://guest:guest@rabbitmq:5672/%2f"
exchange = "sieve_events"
routing_key = "{table}.{event}"   # optional, this is the default
backfill = false
```

Message payload (one per event):

```json
{
  "table": "usdc_transfers",
  "event": "Transfer",
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

- Routing key supports `{table}` and `{event}` placeholders (e.g. `usdc_transfers.Transfer`)
- Exchange is declared as `topic` type, durable
- Messages are persistent (delivery mode 2) with `application/json` content type
- Lazy connection — connects on first event, reconnects automatically on failure
- Webhook and RabbitMQ streams can be used together — both fire after each block commit

## GraphQL API

Sieve auto-generates a full GraphQL schema from your TOML config. Every table gets:

- **Queries** — singular and plural with filtering, sorting, pagination
- **Filter operators** — `_eq`, `_ne`, `_gt`, `_gte`, `_lt`, `_lte`, `_in`, `_not_in`, `_contains`, `_starts_with`, `_ends_with`
- **Composition** — `AND` / `OR` for complex filter logic
- **Pagination** — cursor-based (`first`/`after`) and offset-based (`first`/`skip`)
- **Sorting** — `order_by` + `order_direction`

```graphql
{
  usdc_transfers(
    where: {
      OR: [
        { from_address: "0xAbc..." }
        { to_address: "0xAbc..." }
      ]
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

## CLI Reference

```
sieve [OPTIONS]

Options:
  --config <PATH>         Path to TOML config file [default: sieve.toml]
  --start-block <NUM>     Override start block for all contracts
  --end-block <NUM>       Stop at this block (omit for follow mode)
  --database-url <URL>    PostgreSQL URL (or set DATABASE_URL env var)
  --api-port <PORT>       Enable GraphQL API on this port
  --fresh                 Drop and recreate all tables before indexing
```

## Docker Compose

```yaml
services:
  db:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD: sieve
      POSTGRES_DB: sieve
    volumes:
      - pgdata:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  sieve:
    build: .
    ports:
      - "4000:4000"      # GraphQL API
      - "30303:30303"     # Ethereum P2P (TCP)
      - "30303:30303/udp" # Ethereum P2P (UDP)
    depends_on:
      - db
    environment:
      DATABASE_URL: postgres://postgres:sieve@db:5432/sieve
    command: ["sieve", "--api-port", "4000"]
    volumes:
      - ./sieve.toml:/app/sieve.toml
      - ./abis:/app/abis

volumes:
  pgdata:
```

> **Note:** Port 30303 (TCP + UDP) must be reachable from the internet for Ethereum P2P peer discovery.

## How It Works

```
Ethereum P2P Network
       │
       ▼
  P2P Layer (devp2p, eth/68-70, peer pool)
       │
       ▼
  Sync Engine (AIMD batch sizing, 1000+ blocks/sec)
       │
       ├────────────────┬──────────────────┐
       ▼                ▼                  ▼
  Event Filter    Call Scanner      Transfer Scanner
  (address+topic) (tx.input[0..4])  (tx.value > 0)
       │                │                  │
       ▼                ▼                  │
  ABI Decoder     ABI Decoder             │
  (log data)      (calldata)              │
       │                │                  │
       └────────────────┴──────────────────┘
                        │
                        ▼
                  PostgreSQL ──────► Webhooks / RabbitMQ
                        │
                        ▼
                  GraphQL API
```

- **Checkpoint/resume** — restarts from where it left off
- **Reorg handling** — detects chain reorganizations (up to 64 blocks) and rolls back affected data
- **Follow mode** — after historical sync, follows the chain head in real-time
- **Graceful shutdown** — Ctrl+C stops cleanly, progress is saved

## Building Without jemalloc

By default, Sieve uses jemalloc for better memory allocation performance. To build without it:

```bash
cargo build --release --no-default-features
```

## License

MIT OR Apache-2.0
