<p align="center">
  <img src="assets/sieve-hero.webp" alt="Sieve: blocks stream through a filter and only your events are kept" width="100%">
</p>

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
| **Data source** | RPC provider ($225-$900/mo)        | P2P network, Base archives ($0) |
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

Columns are auto-generated from the ABI. Solidity camelCase is converted to snake_case automatically (`_troveId` -> `trove_id`, `amount0Out` -> `amount0_out`). SQL reserved words are handled. Unnamed ABI parameters are named by position (`param_4`), and arrays and structs are stored as JSONB. You can [override columns](#contracts-and-events) if you want.

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
- **Archive-sourced blocks (Base).** Blocks imported from snapshot archives are authenticated by the pinned manifest digest, file checksums, and a header chain that links to the trusted `checkpoint_hash` you supply. Each committed block records whether it came from a peer quorum or an archive.

No quorum, no commit. Sieve stops rather than write unverified data.

## Configuration Reference

### Chains

Sieve indexes Ethereum mainnet by default, plus four OP-Stack chains:

| Chain      | ID   | `chain =`               | Discovery            |
| ---------- | ---- | ----------------------- | -------------------- |
| Ethereum   | 1    | `"mainnet"` / `"ethereum"` (default) | discv4 / discv5 |
| Base       | 8453 | `"base"`                | `basev0` discv5      |
| OP Mainnet | 10   | `"optimism"` / `"op"`   | discv4 + discv5      |
| Unichain   | 130  | `"unichain"` / `"uni"`  | discv4 + discv5      |
| World Chain| 480  | `"world"` / `"worldchain"` | discv4 + discv5   |

```toml
chain = "base"   # default: "mainnet"
```

A database is bound to its chain on first run. Reusing a mainnet database
with `chain = "base"` (or any other mismatch) is refused, so chain state can
never mix. Use a separate database per chain, or wipe with `sieve reset` /
`--fresh`.

OP-stack notes (Base, OP mainnet, Unichain, and World Chain):

- Blocks arrive over the chain's devp2p network directly (no RPC, same as
  mainnet). Sieve polls peers for the sequencer's unsafe head and indexes
  new blocks in small batches. Base runs its own partitioned `basev0` peer
  discovery, which Sieve speaks; OP mainnet, Unichain, and World Chain use
  standard superchain discovery.
- OP-stack deposit transactions (type `0x7E`) are indexed like any other
  transaction: events decode normally, and user deposits with value show
  up in native transfer tables (the sender is the deposit's `from`
  address). The per-block L1-attributes deposit carries no value and is
  skipped by transfer indexing.
- Peer sets on OP-Stack chains are thinner than mainnet, and many public nodes
  prune receipts (on Base, to roughly a month). For older Base history, use
  [Base Archive Bootstrap](#base-archive-bootstrap). On the other chains, pin
  archive/serving nodes via `trusted_peers` (below). Sieve automatically avoids
  asking peers for history they advertise as pruned.
- Test connectivity without a config: `sieve peers --chain base` /
  `sieve peers --chain optimism` / `sieve peers --chain unichain` /
  `sieve peers --chain world`.
- Base fork schedule: Cobalt (2026-09-30 18:00 UTC) and Denim
  (2026-11-05 18:00 UTC) are built in. After Denim, Base produces 200ms
  blocks; header timestamps stay in whole seconds, so up to five
  consecutive blocks share the same `block_timestamp`.
- Known limitation: Base's unscheduled Everest hardfork adds the EIP-8130
  transaction type `0x79`, which Sieve does not decode. Everest, like any
  future OP-stack hardfork that changes the fork-id, requires a Sieve
  update to keep following the chain.

```toml
[p2p]
# Always-connected peers (e.g. an archive node for deep backfills):
trusted_peers = ["enode://<pubkey>@<ip>:<port>"]
```

### Base Archive Bootstrap

Inspect a pinned Base V2 snapshot before planning a historical archive import:

```bash
sieve archive-plan --manifest base-manifest.json \
  --manifest-sha256 <64-hex-digit-digest> \
  --start-block 500123 --end-block 1000010
```

This offline command prints the selected archives, decode dependencies, disk
estimates, and trust requirements without connecting to a database or peers.

To import the selected Base V2 archives, add an `[archive]` section to your Base
configuration. Paths are relative to the root configuration file:

```toml
[archive]
manifest = "base-manifest.json"
manifest_sha256 = "<64-hex-digit-digest>"
end_block = 500999
checkpoint_hash = "0x<trusted-end-block-hash>"
staging_dir = "archive-stage"
max_staging_bytes = 8000000000
max_download_bytes = 300000000
handoff = false
```

Obtain the manifest digest and endpoint hash independently. Run `archive-plan`
on your exact range before choosing budgets; the example values cover only small
early groups. The reader accepts Base V2 aligned static files from producers
`2.5.2-dev (76a8261)`, `2.5.2-dev (5877708)` and `2.5.2-dev (7134be3)`.
Checksums and header links do not prove OP derivation or L1 finality.

```bash
sieve --config sieve.toml --start-block 499000 --end-block 500999
```

Imports resume from committed progress and release each group's transaction and
receipt files after indexing, while retaining authenticated headers. With
`handoff = true`, omit `--end-block` to continue from a recent archive endpoint
through ordinary Base peer history into live follow. An explicit end above the
archive endpoint bounds the peer tail; an equal end performs only the import.
Missing peer history is retried without skipping blocks.

Recent archive groups are large (the group covering blocks 51.5M to 52M is
about 25 GB compressed), and staging is released after each group. `archive-plan`
reports the exact download and staging needs for your range.

Measured on Base mainnet with a Uniswap v3 WETH/AAVE pool: 7 days imported from
the archive (302,400 blocks), 31,336 blocks caught up from peers, then live
follow. That is 333,736 consecutive blocks with every parent hash linked across
the archive/peer seam (block 51,968,527 to 51,968,528) and 9,588 pool events
collected. The archive anchor, the first peer block, and the decoded pool events
around the seam were checked against an independent RPC.

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

#### Overloaded events: `signature`

Solidity lets one contract declare several events with the same name, for
example Aerodrome gauges emitting both `Deposit(address,uint256,uint128)` and
`Deposit(address,address,uint256)`. Each overload has its own topic0, so pick
one per entry with `signature` and give each its own table:

```toml
[[contracts.events]]
name = "Deposit"
signature = "Deposit(address,uint256,uint128)"   # user, tokenId, liquidityToStake
table = "gauge_cl_deposits"

[[contracts.events]]
name = "Deposit"
signature = "Deposit(address,address,uint256)"   # from, to, amount
table = "gauge_classic_deposits"
```

- The signature is canonical: the event name, then the ABI types in order,
  comma-separated, with no spaces, parameter names, or `indexed`. Tuples are
  written out as `(type,type)`. Sieve compares it exactly against the ABI;
  anything else is a config error that lists the valid signatures.
- An overloaded name without `signature` fails at startup (and in `sieve
  inspect`, `schema`, and `reset`):

  ```text
  event 'Deposit' in contract 'Gauge' is overloaded; add `signature` with one of: Deposit(address,address,uint256), Deposit(address,uint256,uint128) (e.g. signature = "Deposit(address,address,uint256)")
  ```

- Rows are routed by contract and topic0, so each overload lands only in its
  own table. One selector may still feed several tables; each table is
  counted and streamed once per log. Stream payloads keep the bare event name
  (`"event": "Deposit"`).
- `sieve inspect` prints each entry's `signature` and full `topic0`.
- Configs for events with a unique name need no change: omitting `signature`
  keeps the same tables, SQL, and archive resume fingerprint, so upgrading
  needs no reset. Adding `signature` to an existing entry changes the archive
  fingerprint. Renaming an event or changing its parameter types in the ABI
  changes its topic0.
- `sieve add-contract` writes one entry per overload, with its `signature`
  and a numbered table (`<contract>_deposit_1`, `<contract>_deposit_2`, in ABI
  order).
- Factory creation events take no `signature`; to use an overloaded one,
  point `factory.abi` at an ABI containing only the intended variant.

#### Column types, arrays and structs

| Solidity type                                   | Default column                       |
| ----------------------------------------------- | ------------------------------------ |
| `bool`                                          | `boolean`                            |
| `address`, `string`                             | `text` (addresses checksummed)       |
| `uint8`..`uint64`, `int8`..`int64`              | `bigint`                             |
| larger integers                                 | `numeric`                            |
| `bytes`, `bytesN`                               | `bytea`                              |
| any array (`uint128[]`, `bytes32[2]`), `tuple`  | `jsonb`                              |

Arrays become JSON arrays and structs become JSON objects keyed by their ABI
component names, at every nesting level. Integers are decimal strings, so
`uint256` values keep full precision; bytes are lowercase `0x` hex (`"0x"`
when empty); booleans stay booleans. An Aerodrome `GaugesAllocated` row
stores:

```json
[{"gauge": "0x1111111111111111111111111111111111111111", "allocated": "1000", "data": "0x"}]
```

and can be queried with `allocations->0->>'allocated'`. An indexed array or
struct is only a hash in the log, so it is stored as that `0x` hex string.
`type = "text"` on such a column stores the same JSON as text.

#### Unnamed parameters

An ABI input with an empty name is named `param_<i>`, where `i` is its
0-based position among all of the event's or function's inputs (struct
components count among their siblings). Aerodrome's
`PoolCreated(address indexed token0, address indexed token1, bool indexed stable, address pool, uint256)`
gets a `param_4 numeric` column, and `param = "param_4"` selects it in
`columns`, `filter` keys, and `factory.param`. The older `param = ""` still
works while exactly one input is unnamed. An ABI whose real name equals a
generated one at the same level (`param_1` plus an unnamed input 1) is
rejected at startup.

#### Upgrading from 0.3.4 or earlier

Earlier versions stored arrays and structs in `text` columns as Rust debug
strings (`Array([Tuple([...])])`). Sieve never alters existing tables: an
existing `text` column keeps its old rows unchanged and receives JSON text
for new rows, and the GraphQL API keeps serving it as a String. For uniformly
JSON history, reindex into a fresh database. `ALTER TABLE ... TYPE jsonb USING
col::jsonb` fails while any debug-string row remains.

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

- **Globals live in the root only.** `chain`, `[api]`, `[p2p]`, `[sync]`,
  `[archive]`, and `[[streams]]` may appear only in the root config. A protocol fragment may
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

**Note:** `data` keys use raw ABI parameter names, not your TOML column names. If the Solidity ABI defines `_troveId`, the stream sends `_troveId` -- even though PostgreSQL stores it as `trove_id`. ABI names are immutable; config names can change. Unnamed parameters use their generated `param_<i>` name. Values have the same JSON form as the [JSONB columns](#column-types-arrays-and-structs): arrays and struct objects are nested JSON, integers decimal strings.

Receipt fields (`tx_value`, `tx_gas_price`, `gas_used`, `nonce`, `cumulative_gas_used`, `status`) are included when `include_receipts = true`.

Both webhook and RabbitMQ streams can run simultaneously. Both are best-effort -- failures are logged, never block indexing. Lazy connection for RabbitMQ (auto-reconnect on failure).

## GraphQL API

Auto-generated from your TOML config. Every table gets:

- **Filter operators** -- exact match (`field: value`), `_ne`, `_gt`, `_gte`, `_lt`, `_lte`, `_in`, `_not_in`, plus `_contains` and `_starts_with` on text columns
- **Composition** -- `AND` / `OR` for complex filter logic
- **Pagination** -- cursor-based (`first`/`after`) and offset-based (`first`/`skip`)
- **Sorting** -- `orderBy` + `orderDirection`

Numeric columns (`bigint`, `numeric`, `integer`) take filter values as strings (`block_number: "21000000"`, `value_in: ["1", "2"]`) and compare them as numbers.

JSON columns (`jsonb`, `json`) are returned as `String` fields holding the JSON text, and their filters (`field`, `_in`, `_not_in`), `orderBy`, and cursors compare that text (sorting follows the database collation). PostgreSQL normalizes `jsonb` text (key order, spacing), so match against values read from the API.

## CLI

```
sieve [OPTIONS]                Run the indexer
sieve archive-plan [OPTIONS]   Plan a Base archive import offline (no DB or peers)
sieve init                     Scaffold a new project (sieve.toml, .env, abis/)
sieve init --docker            Same + docker-compose.yml with PostgreSQL
sieve schema                   Print generated SQL DDL
sieve reset                    Drop and recreate all tables
sieve inspect                  Dry-run: show tables, event signatures/topic0, columns, and filters
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

Fetches a verified ABI from Etherscan, saves it to `abis/`, and appends a `[[contracts]]` block to your config. Auto-detects proxy contracts. Automatically sets `start_block` to the contract's deploy block. Overloaded events get one entry per `signature`, with numbered tables.

```bash
sieve add-contract 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
sieve add-contract 0xA0b8... --name USDC              # override name
sieve add-contract 0xA0b8... --start-block 21000000   # override start block
```

### `sieve inspect`

Validates your config without a database connection. Shows what tables, columns, context fields, and filters would be created, and each event's canonical `signature` and `topic0`.

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

### Prebuilt image

Release builds are published to `ghcr.io/slvdev/sieve:latest` (used by the
generated `docker-compose.yml`).

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
devp2p network (+ Base snapshot archives)
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

Sieve syncs block headers and receipts over the chain's devp2p protocol (on Base it can first import history from snapshot archives), filters logs against your TOML config at sync time, decodes matched events, and writes to PostgreSQL. Unmatched log and payload data is discarded. You store the events you asked for, plus the block hashes and checkpoints Sieve keeps to verify the chain and resume cleanly.

- **One command:** backfill, catch-up, and live head-following, no separate modes
- **Checkpoint/resume:** restarts exactly where it left off (see [Integrity](#integrity))
- **Follow mode:** after historical sync, follows the chain head in real-time
- **Graceful shutdown:** Ctrl+C or SIGTERM (including `docker stop`) drains pending work and saves progress. A second signal forces exit. Allow enough Docker stop time for the drain to finish.

## FAQ

**Can I filter by non-indexed event parameters (e.g., an address in the log data)?**

Not currently. Sieve filters at sync time using Ethereum log topics (topic0–topic3), which only contain indexed parameters. Non-indexed parameters are decoded and stored in Postgres, but can't be filtered before insertion. You can filter them after the fact using SQL or the GraphQL API. A post-decode value filter is on the roadmap.

**What happens if two contracts emit events with the same name but different parameters?**

No collision. Topic0 is the keccak256 hash of the full event signature including parameter types, so `Transfer(address,address,uint256)` and `Transfer(address,address,uint256,uint256)` produce different topic0 hashes. Sieve also filters by contract address first, so even identical events on different contracts are fully isolated. If you see decode warnings, it's likely a mismatched ABI (e.g., a proxy contract forwarding events with a different signature than the ABI specifies).

**Can I run multiple Sieve instances on the same machine?**

Yes. Each instance needs its own P2P port, database, and config. Use `--p2p-port` or `[p2p] port` in TOML to avoid port conflicts. Speed is not affected; Sieve discovers peers outbound. Only one Sieve process can write to a database; a second one pointed at the same database exits at startup.

## Acknowledgments

Sieve's P2P sync engine is built on [SHiNode](https://github.com/vicnaum/shinode), a high-performance Ethereum node that proved 1000+ blocks/sec sync over devp2p. The networking layer uses [Reth](https://github.com/paradigmxyz/reth) crates for Ethereum P2P protocol support.

## License

MIT OR Apache-2.0
