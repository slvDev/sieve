//! Sieve — Ethereum event indexer over P2P.
//!
//! Entry point: loads TOML config, connects to PostgreSQL, optionally spawns
//! the GraphQL API server, then runs the P2P sync engine. Supports historical
//! backfill (`--end-block`) and live head-following modes. Graceful shutdown
//! on first Ctrl+C, hard exit on second.

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod api;
mod cli;
mod config;
mod db;
mod decode;
mod etherscan;
mod filter;
mod handler;
mod metrics;
mod p2p;
mod stream;
mod sync;
#[cfg(test)]
mod test_utils;
mod toml_config;
mod types;

use types::BlockNumber;

use clap::Parser;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = cli::Cli::parse();

    // Route subcommands
    if let Some(ref command) = cli.command {
        return match command {
            cli::Command::Init { docker } => cmd_init(&cli, *docker),
            cli::Command::Schema => cmd_schema(&cli),
            cli::Command::Reset => cmd_reset(&cli).await,
            cli::Command::AddContract {
                address,
                name,
                start_block,
                etherscan_api_key,
            } => {
                cmd_add_contract(
                    &cli,
                    address,
                    name.as_deref(),
                    *start_block,
                    etherscan_api_key.as_deref(),
                )
                .await
            }
            cli::Command::Inspect => cmd_inspect(&cli),
            cli::Command::Peers => cmd_peers().await,
        };
    }

    run_default(&cli).await
}

/// Run the default indexer path (no subcommand).
///
/// # Errors
///
/// Returns an error on config, database, P2P, or sync failures.
async fn run_default(cli: &cli::Cli) -> eyre::Result<()> {
    let startup = load_toml_config(cli)?;

    // Validate --end-block if provided
    if let Some(end_block) = cli.end_block {
        if end_block < startup.start_block.as_u64() {
            return Err(eyre::eyre!(
                "--end-block ({end_block}) must be >= start_block ({})",
                startup.start_block
            ));
        }
    }

    info!(
        start_block = startup.start_block.as_u64(),
        end_block = cli.end_block,
        mode = if cli.end_block.is_some() {
            "historical"
        } else {
            "follow"
        },
        "sieve starting"
    );

    // Graceful shutdown signal
    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(shutdown_handler(stop_tx));

    let db = Arc::new(setup_database(cli, &startup).await?);

    // Metrics
    let metrics = Arc::new(metrics::SieveMetrics::new());

    // GraphQL API (must be built before resolved_transfers/calls are consumed)
    maybe_spawn_api(cli, &startup, &db, &metrics, &stop_rx)?;

    let start_block = startup.start_block;
    let ctx = build_sync_context(cli, startup, &db, &metrics, stop_rx).await?;

    run_indexer(cli, start_block, ctx).await
}

/// Build handler registries and related data from resolved config.
fn build_registries(
    startup: &StartupConfig,
) -> (
    Arc<handler::HandlerRegistry>,
    Arc<handler::TransferRegistry>,
    Arc<handler::CallRegistry>,
    bool,
    bool,
) {
    let handlers: Vec<Box<dyn handler::EventHandler>> = startup
        .resolved_events
        .iter()
        .map(|re| -> Box<dyn handler::EventHandler> {
            Box::new(handler::ConfigDrivenHandler::new(re.clone()))
        })
        .collect();
    let handlers = Arc::new(handler::HandlerRegistry::new(handlers));
    info!(handlers = handlers.len(), "registered event handlers");

    let transfer_handler_vec: Vec<handler::TransferHandler> = startup
        .resolved_transfers
        .iter()
        .cloned()
        .map(handler::TransferHandler::new)
        .collect();
    let has_transfers = !transfer_handler_vec.is_empty();
    let transfer_handlers = Arc::new(handler::TransferRegistry::new(transfer_handler_vec));

    let call_handler_vec: Vec<handler::CallHandler> = startup
        .resolved_calls
        .iter()
        .cloned()
        .map(handler::CallHandler::new)
        .collect();
    let has_calls = !call_handler_vec.is_empty();
    let call_handlers = Arc::new(handler::CallRegistry::new(call_handler_vec));

    (
        handlers,
        transfer_handlers,
        call_handlers,
        has_transfers,
        has_calls,
    )
}

/// Build handler registries, connect P2P, and assemble the sync context.
///
/// # Errors
///
/// Returns an error on P2P connection or factory child loading failures.
async fn build_sync_context(
    cli: &cli::Cli,
    startup: StartupConfig,
    db: &Arc<db::Database>,
    metrics: &Arc<metrics::SieveMetrics>,
    stop_rx: watch::Receiver<bool>,
) -> eyre::Result<sync::SyncContext> {
    let event_table_map = build_event_table_map(&startup.resolved_events);
    let receipt_tables = Arc::new(build_receipt_tables(
        &startup.resolved_events,
        &startup.resolved_transfers,
        &startup.resolved_calls,
    ));

    let (handlers, transfer_handlers, call_handlers, has_transfers, has_calls) =
        build_registries(&startup);

    let index_config = Arc::new(startup.index_config);
    info!(
        contracts = index_config.contracts.len(),
        "loaded index config"
    );

    if !startup.factories.is_empty() {
        db::load_factory_children(db, &index_config).await?;
    }
    let factories = Arc::new(startup.factories);

    let bloom_filter = build_bloom_filter(&index_config, has_transfers, has_calls, &factories);

    let stream_dispatcher = build_stream_dispatcher(&startup.resolved_streams);

    let session = p2p::connect_mainnet_peers().await?;
    info!(
        peers = session.pool.len(),
        "connected to ethereum p2p network"
    );

    Ok(sync::SyncContext {
        pool: Arc::clone(&session.pool),
        config: index_config,
        db: Arc::clone(db),
        handlers,
        metrics: Arc::clone(metrics),
        stop_rx,
        factories,
        transfer_handlers,
        call_handlers,
        stream_dispatcher,
        event_table_map: Arc::new(event_table_map),
        is_backfill: cli.end_block.is_some(),
        receipt_tables,
        bloom_filter,
        head_seen_rx: None,
    })
}

/// Resolved startup parameters from TOML config + CLI.
#[derive(Debug)]
struct StartupConfig {
    database_url: String,
    index_config: config::IndexConfig,
    resolved_events: Vec<toml_config::ResolvedEvent>,
    factories: Vec<toml_config::ResolvedFactory>,
    resolved_transfers: Vec<toml_config::ResolvedTransfer>,
    resolved_calls: Vec<toml_config::ResolvedCall>,
    resolved_streams: Vec<toml_config::ResolvedStream>,
    start_block: BlockNumber,
}

/// Parsed + resolved config (no DB URL needed).
struct ResolvedStartup {
    sieve_config: toml_config::SieveConfig,
    resolved: toml_config::ResolvedConfig,
}

/// Load and resolve TOML config without requiring a database URL.
///
/// # Errors
///
/// Returns an error if the config file cannot be read, parsed, or resolved.
fn load_resolved_config(cli: &cli::Cli) -> eyre::Result<ResolvedStartup> {
    let config_path = Path::new(&cli.config);
    let sieve_config = toml_config::load_config(config_path)?;
    let config_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
    let resolved = toml_config::resolve_config(&sieve_config, config_dir)?;
    Ok(ResolvedStartup {
        sieve_config,
        resolved,
    })
}

/// Resolve database URL: CLI > env > TOML. Error if none provided.
///
/// # Errors
///
/// Returns an error if no database URL is available from any source.
fn resolve_database_url(cli: &cli::Cli, config: &toml_config::SieveConfig) -> eyre::Result<String> {
    cli.database_url
        .clone()
        .or_else(|| config.database.as_ref().and_then(|d| d.url.clone()))
        .ok_or_else(|| {
            eyre::eyre!(
                "no database URL provided. Use --database-url, DATABASE_URL env var, or [database].url in config"
            )
        })
}

/// Load TOML config, resolve ABI files, and compute startup parameters.
///
/// # Errors
///
/// Returns an error if the config file cannot be read, parsed, or resolved.
fn load_toml_config(cli: &cli::Cli) -> eyre::Result<StartupConfig> {
    let startup = load_resolved_config(cli)?;
    let database_url = resolve_database_url(cli, &startup.sieve_config)?;

    // Compute effective start_block: CLI override or minimum across contracts, factories, and transfers
    let start_block = BlockNumber::new(cli.start_block.unwrap_or_else(|| {
        let contract_min = startup
            .sieve_config
            .contracts
            .iter()
            .filter_map(|c| c.start_block)
            .min()
            .unwrap_or(u64::MAX);
        let factory_min = startup
            .resolved
            .factories
            .iter()
            .map(|f| f.start_block)
            .min()
            .unwrap_or(u64::MAX);
        let transfer_min = startup
            .sieve_config
            .transfers
            .iter()
            .filter_map(|t| t.start_block)
            .min()
            .unwrap_or(u64::MAX);
        contract_min.min(factory_min).min(transfer_min)
    }));

    Ok(StartupConfig {
        database_url,
        index_config: startup.resolved.index_config,
        resolved_events: startup.resolved.resolved_events,
        factories: startup.resolved.factories,
        resolved_transfers: startup.resolved.transfers,
        resolved_calls: startup.resolved.calls,
        resolved_streams: startup.resolved.streams,
        start_block,
    })
}

// ── Subcommand handlers ──────────────────────────────────────────────

/// Scaffold a new Sieve project.
///
/// # Errors
///
/// Returns an error if the config file already exists or file I/O fails.
#[expect(clippy::print_stdout, reason = "CLI output for init command")]
fn cmd_init(cli: &cli::Cli, docker: bool) -> eyre::Result<()> {
    let config_path = Path::new(&cli.config);
    if config_path.exists() {
        return Err(eyre::eyre!("{} already exists", cli.config));
    }

    std::fs::create_dir_all("abis")
        .map_err(|e| eyre::eyre!("failed to create abis/ directory: {e}"))?;

    let template = r#"[database]
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
"#;

    std::fs::write(config_path, template)
        .map_err(|e| eyre::eyre!("failed to write {}: {e}", cli.config))?;

    // Write minimal ERC20 ABI (Transfer + Approval events)
    let abi_path = Path::new("abis/erc20.json");
    if !abi_path.exists() {
        let erc20_abi = r#"[
  {
    "anonymous": false,
    "inputs": [
      { "indexed": true, "name": "from", "type": "address" },
      { "indexed": true, "name": "to", "type": "address" },
      { "indexed": false, "name": "value", "type": "uint256" }
    ],
    "name": "Transfer",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      { "indexed": true, "name": "owner", "type": "address" },
      { "indexed": true, "name": "spender", "type": "address" },
      { "indexed": false, "name": "value", "type": "uint256" }
    ],
    "name": "Approval",
    "type": "event"
  }
]
"#;
        std::fs::write(abi_path, erc20_abi)
            .map_err(|e| eyre::eyre!("failed to write abis/erc20.json: {e}"))?;
    }

    if docker {
        write_docker_compose()?;
        println!(
            "created {}, abis/erc20.json, and docker-compose.yml",
            cli.config
        );
    } else {
        println!("created {} and abis/erc20.json", cli.config);
    }
    Ok(())
}

fn write_docker_compose() -> eyre::Result<()> {
    let compose_path = Path::new("docker-compose.yml");
    if compose_path.exists() {
        return Ok(());
    }
    let compose = r#"services:
  db:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD: sieve
      POSTGRES_DB: sieve
    volumes:
      - pgdata:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d sieve"]
      interval: 10s
      timeout: 5s
      retries: 5

  sieve:
    image: ghcr.io/slvdev/sieve:latest
    ports:
      - "4000:4000"
      - "30303:30303"
      - "30303:30303/udp"
    depends_on:
      db:
        condition: service_healthy
    environment:
      DATABASE_URL: postgres://postgres:sieve@db:5432/sieve
    command: ["--api-port", "4000"]
    volumes:
      - ./sieve.toml:/app/sieve.toml:ro
      - ./abis:/app/abis:ro
    restart: unless-stopped

volumes:
  pgdata:
"#;
    std::fs::write(compose_path, compose)
        .map_err(|e| eyre::eyre!("failed to write docker-compose.yml: {e}"))
}

/// Print the SQL DDL that Sieve would generate from the config.
///
/// # Errors
///
/// Returns an error if the config file cannot be read or resolved.
#[expect(clippy::print_stdout, reason = "CLI output for schema command")]
fn cmd_schema(cli: &cli::Cli) -> eyre::Result<()> {
    let startup = load_resolved_config(cli)?;

    println!("-- Internal tables\n");
    println!("{};", db::CHECKPOINTS_DDL);
    println!();
    println!("{};", db::BLOCK_HASHES_DDL);
    println!();
    println!("{};", db::FACTORY_CHILDREN_DDL);

    for event in &startup.resolved.resolved_events {
        println!(
            "\n-- Table: {} ({} / {})\n",
            event.table_name, event.contract_name, event.event_name
        );
        println!("{}", event.create_table_sql);
        for idx in &event.create_indexes_sql {
            println!("{idx}");
        }
    }

    for transfer in &startup.resolved.transfers {
        println!("\n-- Table: {} (transfer)\n", transfer.table_name);
        println!("{}", transfer.create_table_sql);
        for idx in &transfer.create_indexes_sql {
            println!("{idx}");
        }
    }

    for call in &startup.resolved.calls {
        println!(
            "\n-- Table: {} ({} / {})\n",
            call.table_name, call.contract_name, call.function_name
        );
        println!("{}", call.create_table_sql);
        for idx in &call.create_indexes_sql {
            println!("{idx}");
        }
    }

    Ok(())
}

/// Drop all tables and recreate them.
///
/// # Errors
///
/// Returns an error if the database connection or DDL fails.
async fn cmd_reset(cli: &cli::Cli) -> eyre::Result<()> {
    let startup = load_resolved_config(cli)?;
    let database_url = resolve_database_url(cli, &startup.sieve_config)?;

    let db = db::Database::connect(&database_url).await?;
    db::drop_all_tables(
        &db,
        &startup.resolved.resolved_events,
        &startup.resolved.transfers,
        &startup.resolved.calls,
    )
    .await?;
    db::create_internal_tables(&db).await?;
    db::create_user_tables(&db, &startup.resolved.resolved_events).await?;
    db::create_transfer_tables(&db, &startup.resolved.transfers).await?;
    db::create_call_tables(&db, &startup.resolved.calls).await?;

    info!("reset complete — all tables dropped and recreated");
    Ok(())
}

/// Fetch a contract ABI from Etherscan and append it to the config.
///
/// # Errors
///
/// Returns an error if the address is invalid, the API key is missing,
/// the Etherscan request fails, or the config file cannot be written.
#[expect(clippy::print_stdout, reason = "CLI output for add-contract command")]
async fn cmd_add_contract(
    cli: &cli::Cli,
    address: &str,
    name_override: Option<&str>,
    start_block: Option<u64>,
    api_key: Option<&str>,
) -> eyre::Result<()> {
    use std::io::Write as _;

    let parsed: alloy_primitives::Address = address
        .parse()
        .map_err(|_| eyre::eyre!("invalid address: {address}"))?;
    let checksummed = alloy_primitives::Address::to_checksum(&parsed, None);

    let api_key = api_key.ok_or_else(|| {
        eyre::eyre!(
            "Etherscan API key required. Use --etherscan-api-key or set ETHERSCAN_API_KEY env var"
        )
    })?;

    let config_path = Path::new(&cli.config);
    if !config_path.exists() {
        return Err(eyre::eyre!(
            "{} not found — run `sieve init` first",
            cli.config
        ));
    }

    let info = etherscan::fetch_contract_info(&checksummed, api_key).await?;

    let contract_name = name_override.map_or_else(
        || {
            if info.name.is_empty() {
                checksummed.chars().take(10).collect()
            } else {
                info.name.clone()
            }
        },
        String::from,
    );

    let snake_name = toml_config::camel_to_snake_case(&contract_name);
    let abi_path = format!("abis/{snake_name}.json");

    save_abi_file(&info.abi_json, Path::new(&abi_path))?;

    let abi: alloy_json_abi::JsonAbi = serde_json::from_str(&info.abi_json)
        .map_err(|e| eyre::eyre!("failed to parse ABI JSON: {e}"))?;

    let mut seen = HashSet::new();
    let events: Vec<(String, String)> = abi
        .events()
        .filter(|ev| seen.insert(ev.name.clone()))
        .map(|ev| {
            let event_snake = toml_config::camel_to_snake_case(&ev.name);
            let table = format!("{snake_name}_{event_snake}");
            (ev.name.clone(), table)
        })
        .collect();

    let toml_block = generate_contract_toml(
        &contract_name,
        &checksummed,
        &abi_path,
        start_block,
        &events,
    );

    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(config_path)
        .map_err(|e| eyre::eyre!("failed to open {}: {e}", cli.config))?;
    file.write_all(toml_block.as_bytes())
        .map_err(|e| eyre::eyre!("failed to write to {}: {e}", cli.config))?;

    println!(
        "added {contract_name} ({} events) to {}",
        events.len(),
        cli.config
    );
    println!("  abi: {abi_path}");
    if info.is_proxy {
        println!("  note: proxy detected — using implementation ABI");
    }
    Ok(())
}

/// Save pretty-printed ABI JSON to a file, creating parent directories.
fn save_abi_file(abi_json: &str, path: &Path) -> eyre::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| eyre::eyre!("failed to create {}: {e}", parent.display()))?;
    }
    // Pretty-print the ABI JSON
    let parsed: serde_json::Value = serde_json::from_str(abi_json)
        .map_err(|e| eyre::eyre!("failed to parse ABI JSON for formatting: {e}"))?;
    let pretty = serde_json::to_string_pretty(&parsed)
        .map_err(|e| eyre::eyre!("failed to format ABI JSON: {e}"))?;
    std::fs::write(path, pretty)
        .map_err(|e| eyre::eyre!("failed to write {}: {e}", path.display()))?;
    Ok(())
}

/// Generate a TOML `[[contracts]]` block with event definitions.
#[must_use]
fn generate_contract_toml(
    name: &str,
    address: &str,
    abi_path: &str,
    start_block: Option<u64>,
    events: &[(String, String)],
) -> String {
    use std::fmt::Write;

    let mut out = String::with_capacity(256);
    out.push_str("\n[[contracts]]\n");
    let _ = writeln!(out, "name = \"{name}\"");
    let _ = writeln!(out, "address = \"{address}\"");
    let _ = writeln!(out, "abi = \"{abi_path}\"");
    match start_block {
        Some(block) => {
            let _ = writeln!(out, "start_block = {block}");
        }
        None => {
            let _ = writeln!(out, "# start_block = 0");
        }
    }

    for (event_name, table_name) in events {
        out.push('\n');
        out.push_str("[[contracts.events]]\n");
        let _ = writeln!(out, "name = \"{event_name}\"");
        let _ = writeln!(out, "table = \"{table_name}\"");
    }

    out
}

/// Dry-run: show tables, columns, and filters from the config.
///
/// # Errors
///
/// Returns an error if the config file cannot be read or resolved.
#[expect(clippy::print_stdout, reason = "CLI output for inspect command")]
fn cmd_inspect(cli: &cli::Cli) -> eyre::Result<()> {
    let startup = load_resolved_config(cli)?;
    let resolved = &startup.resolved;

    // Group events by contract_name
    let mut events_by_contract: HashMap<&str, Vec<&toml_config::ResolvedEvent>> = HashMap::new();
    for re in &resolved.resolved_events {
        events_by_contract
            .entry(&re.contract_name)
            .or_default()
            .push(re);
    }

    // Group calls by contract_name
    let mut calls_by_contract: HashMap<&str, Vec<&toml_config::ResolvedCall>> = HashMap::new();
    for rc in &resolved.calls {
        calls_by_contract
            .entry(&rc.contract_name)
            .or_default()
            .push(rc);
    }

    // Contracts from TOML (for address/abi/start_block metadata)
    let contracts = &startup.sieve_config.contracts;

    println!("Contracts: {}", contracts.len());
    for contract in contracts {
        let addr = contract.address.as_deref().unwrap_or("(factory-child)");
        println!("\n  {} ({addr})", contract.name);
        println!("    abi: {}", contract.abi);
        if let Some(sb) = contract.start_block {
            println!("    start_block: {sb}");
        }

        if let Some(events) = events_by_contract.get(contract.name.as_str()) {
            println!("\n    Events:");
            for ev in events {
                print_event_detail(ev);
            }
        }

        if let Some(calls) = calls_by_contract.get(contract.name.as_str()) {
            println!("\n    Calls:");
            for call in calls {
                print_call_detail(call);
            }
        }
    }

    // Transfers
    if resolved.transfers.is_empty() {
        println!("\nTransfers: (none)");
    } else {
        println!("\nTransfers: {}", resolved.transfers.len());
        for t in &resolved.transfers {
            print_transfer_detail(t);
        }
    }

    // Streams
    if resolved.streams.is_empty() {
        println!("\nStreams: (none)");
    } else {
        println!("\nStreams: {}", resolved.streams.len());
        for s in &resolved.streams {
            println!("  {} ({})", s.name, s.stream_type);
            println!("    url: {}", s.url);
            println!("    backfill: {}", s.backfill);
        }
    }

    Ok(())
}

/// Print a single event's columns, context fields, and topic filters.
#[expect(clippy::print_stdout, reason = "CLI output helper")]
fn print_event_detail(event: &toml_config::ResolvedEvent) {
    println!("      {} -> {}", event.event_name, event.table_name);
    if !event.columns.is_empty() {
        let cols: Vec<String> = event
            .columns
            .iter()
            .map(|c| format!("{} ({})", c.column_name, c.pg_type))
            .collect();
        println!("        columns: {}", cols.join(", "));
    }
    if !event.context_fields.is_empty() {
        let ctx: Vec<&str> = event
            .context_fields
            .iter()
            .map(|f| f.pg_column_name())
            .collect();
        println!("        context: {}", ctx.join(", "));
    }
    if !event.topic_filters.is_empty() {
        println!(
            "        topic_filters: {} filter(s)",
            event.topic_filters.len()
        );
    }
}

/// Print a single call's columns and context fields.
#[expect(clippy::print_stdout, reason = "CLI output helper")]
fn print_call_detail(call: &toml_config::ResolvedCall) {
    println!("      {} -> {}", call.function_name, call.table_name);
    if !call.columns.is_empty() {
        let cols: Vec<String> = call
            .columns
            .iter()
            .map(|c| format!("{} ({})", c.column_name, c.pg_type))
            .collect();
        println!("        columns: {}", cols.join(", "));
    }
    if !call.context_fields.is_empty() {
        let ctx: Vec<&str> = call
            .context_fields
            .iter()
            .map(|f| f.pg_column_name())
            .collect();
        println!("        context: {}", ctx.join(", "));
    }
}

/// Print a single transfer's context fields and address filters.
#[expect(clippy::print_stdout, reason = "CLI output helper")]
fn print_transfer_detail(transfer: &toml_config::ResolvedTransfer) {
    println!("  {} -> {}", transfer.name, transfer.table_name);
    if !transfer.context_fields.is_empty() {
        let ctx: Vec<&str> = transfer
            .context_fields
            .iter()
            .map(|f| f.pg_column_name())
            .collect();
        println!("    context: {}", ctx.join(", "));
    }
    if !transfer.filter_from.is_empty() {
        let addrs: Vec<String> = transfer
            .filter_from
            .iter()
            .map(|a| alloy_primitives::Address::to_checksum(a, None))
            .collect();
        println!("    filter_from: {}", addrs.join(", "));
    }
    if !transfer.filter_to.is_empty() {
        let addrs: Vec<String> = transfer
            .filter_to
            .iter()
            .map(|a| alloy_primitives::Address::to_checksum(a, None))
            .collect();
        println!("    filter_to: {}", addrs.join(", "));
    }
}

/// Connect to P2P network and report peer count until interrupted.
///
/// # Errors
///
/// Returns an error if the P2P network fails to start.
#[expect(clippy::print_stdout, reason = "CLI output for peers command")]
async fn cmd_peers() -> eyre::Result<()> {
    println!("Connecting to Ethereum P2P network...");
    let session = p2p::connect_mainnet_peers().await?;
    println!("Startup complete: {} peers connected", session.pool.len());

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let count = session.pool.len();
                let best = session.pool.best_peer_head().unwrap_or(0);
                println!("peers={count} best_head={best}");
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Shutting down.");
                break;
            }
        }
    }
    Ok(())
}

/// Run the indexer in either historical or follow mode.
///
/// # Errors
///
/// Returns an error on sync or database failures.
async fn run_indexer(
    cli: &cli::Cli,
    start_block: BlockNumber,
    ctx: sync::SyncContext,
) -> eyre::Result<()> {
    if let Some(end_block_raw) = cli.end_block {
        let end_block = BlockNumber::new(end_block_raw);
        let effective_start = resolve_effective_start(&ctx.db, start_block, end_block).await?;

        if effective_start > end_block {
            info!("nothing to index");
            ctx.metrics
                .is_ready
                .store(true, std::sync::atomic::Ordering::Relaxed);
            return Ok(());
        }

        let metrics = Arc::clone(&ctx.metrics);
        let outcome = sync::run_sync(effective_start, end_block, ctx).await?;

        // Historical sync complete — mark as ready
        metrics
            .is_ready
            .store(true, std::sync::atomic::Ordering::Relaxed);

        info!(
            blocks = outcome.blocks_fetched,
            receipts = outcome.total_receipts,
            events_matched = outcome.events_matched,
            events_decoded = outcome.events_decoded,
            events_stored = outcome.events_stored,
            transfers_stored = outcome.transfers_stored,
            calls_stored = outcome.calls_stored,
            elapsed_ms = outcome.elapsed.as_millis() as u64,
            "sync complete"
        );
    } else {
        sync::run_follow_loop(start_block, ctx).await?;
    }

    Ok(())
}

/// Connect to PostgreSQL, optionally drop tables, and create schema.
///
/// # Errors
///
/// Returns an error if the database connection or DDL fails.
async fn setup_database(cli: &cli::Cli, startup: &StartupConfig) -> eyre::Result<db::Database> {
    let db = db::Database::connect(&startup.database_url).await?;
    if cli.fresh {
        warn!("--fresh: dropping all tables");
        db::drop_all_tables(
            &db,
            &startup.resolved_events,
            &startup.resolved_transfers,
            &startup.resolved_calls,
        )
        .await?;
    }
    db::create_internal_tables(&db).await?;
    db::create_user_tables(&db, &startup.resolved_events).await?;
    db::create_transfer_tables(&db, &startup.resolved_transfers).await?;
    db::create_call_tables(&db, &startup.resolved_calls).await?;
    Ok(db)
}

/// Determine the effective start block, accounting for checkpoint resume.
///
/// # Errors
///
/// Returns an error if the checkpoint read fails.
async fn resolve_effective_start(
    db: &db::Database,
    start_block: BlockNumber,
    end_block: BlockNumber,
) -> eyre::Result<BlockNumber> {
    if let Some(checkpoint) = db.last_checkpoint().await? {
        if checkpoint >= end_block {
            info!(
                checkpoint = checkpoint.as_u64(),
                end_block = end_block.as_u64(),
                "range already indexed, nothing to do"
            );
            return Ok(BlockNumber::new(end_block.as_u64() + 1)); // signals "nothing to index"
        }
        if checkpoint >= start_block {
            let resume_from = BlockNumber::new(checkpoint.as_u64() + 1);
            info!(
                checkpoint = checkpoint.as_u64(),
                resume_from = resume_from.as_u64(),
                "resuming from checkpoint"
            );
            return Ok(resume_from);
        }
    }
    Ok(start_block)
}

/// Handle Ctrl+C for graceful shutdown.
///
/// First signal: set the stop flag so all loops drain gracefully.
/// Second signal: hard exit (for impatient users).
#[expect(clippy::exit, reason = "second Ctrl+C requires immediate hard exit")]
async fn shutdown_handler(stop_tx: watch::Sender<bool>) {
    // First Ctrl+C → graceful shutdown
    tokio::signal::ctrl_c().await.ok();
    warn!("shutdown signal received; stopping after draining");
    let _ = stop_tx.send(true);

    // Second Ctrl+C → hard exit
    tokio::signal::ctrl_c().await.ok();
    warn!("second shutdown signal received; forcing exit");
    std::process::exit(130);
}

/// Build the event table map: `"contract:event"` → `(table_name, event_name)`.
///
/// Spawn the GraphQL API server if `--api-port` is set.
fn maybe_spawn_api(
    cli: &cli::Cli,
    startup: &StartupConfig,
    db: &Arc<db::Database>,
    metrics: &Arc<metrics::SieveMetrics>,
    stop_rx: &watch::Receiver<bool>,
) -> eyre::Result<()> {
    let Some(api_port) = cli.api_port else {
        return Ok(());
    };
    let schema = api::build_schema(
        &startup.resolved_events,
        &startup.resolved_transfers,
        &startup.resolved_calls,
        db.pool().clone(),
    )?;
    let api_stop = stop_rx.clone();
    let api_metrics = Arc::clone(metrics);
    tokio::spawn(async move {
        if let Err(e) = api::run_api_server(api_port, schema, api_metrics, api_stop).await {
            tracing::error!(error = %e, "API server error");
        }
    });
    Ok(())
}

/// Build a bloom filter for skipping blocks with no matching contract addresses.
///
/// Enabled only when the config has no transfer handlers, no call handlers,
/// and no factory contracts (those need full block data for every block).
fn build_bloom_filter(
    index_config: &config::IndexConfig,
    has_transfers: bool,
    has_calls: bool,
    factories: &[toml_config::ResolvedFactory],
) -> Option<Arc<filter::BloomFilter>> {
    if has_transfers || has_calls || !factories.is_empty() {
        info!("bloom filter disabled (transfers, calls, or factories configured)");
        return None;
    }
    let addresses: Vec<alloy_primitives::Address> = index_config
        .contracts
        .iter()
        .filter(|c| c.address != alloy_primitives::Address::ZERO)
        .map(|c| c.address)
        .collect();
    if addresses.is_empty() {
        return None;
    }
    info!(
        addresses = addresses.len(),
        "bloom filter enabled — skipping blocks with no matching contracts"
    );
    Some(Arc::new(filter::BloomFilter::new(addresses)))
}

/// Used by the sync engine to map decoded events to table names for
/// stream notification payloads.
fn build_event_table_map(
    events: &[toml_config::ResolvedEvent],
) -> HashMap<String, (String, String)> {
    let mut map = HashMap::with_capacity(events.len());
    for re in events {
        let key = format!("{}:{}", re.contract_name, re.event_name);
        map.insert(key, (re.table_name.clone(), re.event_name.clone()));
    }
    map
}

/// Build the set of table names that have `include_receipts = true`.
///
/// Used by the sync engine to decide whether to enrich streaming payloads
/// with receipt/tx metadata for a given table.
fn build_receipt_tables(
    events: &[toml_config::ResolvedEvent],
    transfers: &[toml_config::ResolvedTransfer],
    calls: &[toml_config::ResolvedCall],
) -> HashSet<String> {
    let mut set = HashSet::new();
    for e in events {
        if e.include_receipts {
            set.insert(e.table_name.clone());
        }
    }
    for t in transfers {
        if t.include_receipts {
            set.insert(t.table_name.clone());
        }
    }
    for c in calls {
        if c.include_receipts {
            set.insert(c.table_name.clone());
        }
    }
    set
}

/// Build stream sinks from resolved stream definitions.
///
/// Build the stream dispatcher if any streams are configured.
fn build_stream_dispatcher(
    streams: &[toml_config::ResolvedStream],
) -> Option<Arc<stream::StreamDispatcher>> {
    if streams.is_empty() {
        return None;
    }
    let sinks = build_stream_sinks(streams);
    info!(streams = sinks.len(), "configured stream sinks");
    Some(Arc::new(stream::StreamDispatcher::new(sinks, 256)))
}

/// Returns `Vec<(sink, backfill)>` for the `StreamDispatcher`.
fn build_stream_sinks(
    streams: &[toml_config::ResolvedStream],
) -> Vec<(Box<dyn stream::StreamSink>, bool)> {
    streams
        .iter()
        .filter_map(|s| {
            let sink: Box<dyn stream::StreamSink> = match s.stream_type.as_str() {
                "webhook" => Box::new(stream::webhook::WebhookSink::new(
                    s.name.clone(),
                    s.url.clone(),
                )),
                "rabbitmq" => {
                    let exchange = s.exchange.clone().unwrap_or_default();
                    let default_routing_key = ["{table}", ".", "{event}"].concat();
                    let routing_key = s.routing_key.clone().unwrap_or(default_routing_key);
                    Box::new(stream::rabbitmq::RabbitMqSink::new(
                        s.name.clone(),
                        s.url.clone(),
                        exchange,
                        routing_key,
                    ))
                }
                other => {
                    warn!(stream = %s.name, stream_type = %other, "unknown stream type, skipping");
                    return None;
                }
            };
            Some((sink, s.backfill))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_contract_toml_basic() {
        let events = vec![
            ("Transfer".to_owned(), "usdc_transfer".to_owned()),
            ("Approval".to_owned(), "usdc_approval".to_owned()),
        ];
        let toml = generate_contract_toml("USDC", "0xA0b8", "abis/usdc.json", None, &events);
        assert!(toml.contains("name = \"USDC\""));
        assert!(toml.contains("address = \"0xA0b8\""));
        assert!(toml.contains("abi = \"abis/usdc.json\""));
        assert!(toml.contains("# start_block = 0"));
        assert!(toml.contains("name = \"Transfer\""));
        assert!(toml.contains("table = \"usdc_transfer\""));
        assert!(toml.contains("name = \"Approval\""));
        assert!(toml.contains("table = \"usdc_approval\""));
    }

    #[test]
    fn generate_contract_toml_with_start_block() {
        let events = vec![("Transfer".to_owned(), "usdc_transfer".to_owned())];
        let toml = generate_contract_toml(
            "USDC",
            "0xA0b8",
            "abis/usdc.json",
            Some(21_000_000),
            &events,
        );
        assert!(toml.contains("start_block = 21000000"));
        assert!(!toml.contains("# start_block"));
    }

    #[test]
    fn generate_contract_toml_no_events() {
        let toml = generate_contract_toml("Empty", "0x1234", "abis/empty.json", None, &[]);
        assert!(toml.contains("name = \"Empty\""));
        assert!(!toml.contains("[[contracts.events]]"));
    }
}
