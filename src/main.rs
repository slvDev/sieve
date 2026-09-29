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
mod archive;
mod chain;
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
mod ui;

use types::BlockNumber;

use clap::Parser;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // Load .env before clap so DATABASE_URL is available via #[arg(env)]
    dotenvy::dotenv().ok();

    let cli = cli::Cli::parse();

    if cli.explain {
        return print_explain();
    }

    let default_level = if cli.verbose { "info" } else { "warn" };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                // `tar_no_std` warns once per startup while reth reads the
                // embedded OP superchain-registry archive (unichain chain
                // spec); the skipped directory entries are harmless.
                let filter = tracing_subscriber::EnvFilter::new(default_level);
                match "tar_no_std=off".parse() {
                    Ok(directive) => filter.add_directive(directive),
                    Err(_) => filter,
                }
            }),
        )
        .init();

    // Route subcommands
    if let Some(ref command) = cli.command {
        return match command {
            cli::Command::ArchivePlan(args) => archive::print_plan(args),
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
            cli::Command::Peers { chain } => match resolve_config_chain(&cli, chain.as_deref())? {
                chain::ChainKind::Mainnet => cmd_peers::<chain::EthereumChain>().await,
                chain::ChainKind::Base => cmd_peers::<chain::BaseChain>().await,
                chain::ChainKind::Optimism => cmd_peers::<chain::OptimismChain>().await,
                chain::ChainKind::Unichain => cmd_peers::<chain::UnichainChain>().await,
                chain::ChainKind::World => cmd_peers::<chain::WorldChain>().await,
            },
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

    if !cli.verbose {
        let table_names: Vec<&str> = startup
            .resolved_events
            .iter()
            .map(|e| e.table_name.as_str())
            .chain(
                startup
                    .resolved_transfers
                    .iter()
                    .map(|t| t.table_name.as_str()),
            )
            .chain(startup.resolved_calls.iter().map(|c| c.table_name.as_str()))
            .collect();
        ui::print_banner(
            env!("CARGO_PKG_VERSION"),
            &cli.config,
            &startup.database_url,
            &table_names,
            startup.api_port,
        );
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
    tokio::spawn(shutdown_handler(stop_tx, cli.verbose));

    let db = Arc::new(setup_database(cli, &startup).await?);

    // Prune orphaned rows beyond the checkpoint BEFORE the API can serve
    // them (an interrupted pre-contiguity run may have left some behind).
    prune_beyond_checkpoint(&db, &startup).await?;

    // Refuse factory configurations whose history this database has not
    // actually covered — their creation events would silently never be
    // scanned. --assume-factory-coverage adopts factories with no
    // coverage record (the upgrade path for databases from before
    // coverage tracking); every other refusal stands.
    db::ensure_factory_coverage(
        &db,
        &startup.factories,
        startup.start_block.as_u64(),
        cli.assume_factory_coverage,
    )
    .await?;

    // Metrics
    let metrics = Arc::new(metrics::SieveMetrics::new());

    match startup.chain {
        chain::ChainKind::Mainnet => {
            prepare_and_run::<chain::EthereumChain>(cli, startup, &db, &metrics, stop_rx).await
        }
        chain::ChainKind::Base => {
            prepare_and_run::<chain::BaseChain>(cli, startup, &db, &metrics, stop_rx).await
        }
        chain::ChainKind::Optimism => {
            prepare_and_run::<chain::OptimismChain>(cli, startup, &db, &metrics, stop_rx).await
        }
        chain::ChainKind::Unichain => {
            prepare_and_run::<chain::UnichainChain>(cli, startup, &db, &metrics, stop_rx).await
        }
        chain::ChainKind::World => {
            prepare_and_run::<chain::WorldChain>(cli, startup, &db, &metrics, stop_rx).await
        }
    }
}

/// Connect peers, verify the committed frontier by quorum (recovering from
/// a reorg-while-stopped), and only THEN bind the API and run the indexer.
///
/// Ordering is a security boundary: the GraphQL API is spawned strictly
/// after [`sync::verify_or_recover_frontier`], so it can never serve
/// poisoned or reorged rows during the (possibly minutes-long) quorum
/// discovery.
async fn prepare_and_run<C: chain::ChainTypes>(
    cli: &cli::Cli,
    startup: StartupConfig,
    db: &Arc<db::Database>,
    metrics: &Arc<metrics::SieveMetrics>,
    stop_rx: watch::Receiver<bool>,
) -> eyre::Result<()> {
    // Build the API schema before `startup` is consumed, but do NOT bind
    // the server yet.
    let api = build_api_schema(&startup, db)?;
    let start_block = startup.start_block;

    let ctx = build_sync_context::<C>(cli, startup, db, metrics, stop_rx).await?;

    // Verify committed state (and recover from a reorg while stopped)
    // BEFORE anything can read it.
    let policy = sync::canonical::QuorumPolicy::default();
    sync::verify_or_recover_frontier(&ctx, &policy).await?;

    // An existing database must resume at or below checkpoint + 1; a
    // configured start ABOVE it would leave an unindexed gap the scalar
    // checkpoint would then silently advance past.
    enforce_no_start_gap(&ctx.db, start_block).await?;

    // Frontier verified — now the API may serve.
    if let Some((port, schema)) = api {
        spawn_api_server(port, schema, metrics, &ctx.stop_rx);
    }

    run_indexer(cli, start_block, ctx).await
}

/// Parse and validate trusted peer URLs from config.
///
/// Validated at config-resolution time — before any database or network
/// side effects — and required to use the `enode://` scheme explicitly.
fn parse_trusted_peers(raw: &[String]) -> eyre::Result<Vec<reth_network_peers::TrustedPeer>> {
    raw.iter()
        .map(|url| {
            if !url.starts_with("enode://") {
                return Err(eyre::eyre!(
                    "invalid trusted peer \"{url}\": must be an enode:// URL"
                ));
            }
            url.parse()
                .map_err(|err| eyre::eyre!("invalid trusted peer \"{url}\": {err}"))
        })
        .collect()
}

/// Resolve the configured chain from a flag or the config file.
///
/// Precedence: explicit flag, then the config file's `chain` key (when the
/// file exists — `peers` must keep working without one), then mainnet.
fn resolve_config_chain(cli: &cli::Cli, flag: Option<&str>) -> eyre::Result<chain::ChainKind> {
    if let Some(name) = flag {
        return chain::ChainKind::parse(name);
    }
    if Path::new(&cli.config).exists() {
        let sieve_config = toml_config::load_config(Path::new(&cli.config))?;
        if let Some(name) = sieve_config.chain.as_deref() {
            return chain::ChainKind::parse(name);
        }
    }
    Ok(chain::ChainKind::default())
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
async fn build_sync_context<C: chain::ChainTypes>(
    cli: &cli::Cli,
    startup: StartupConfig,
    db: &Arc<db::Database>,
    metrics: &Arc<metrics::SieveMetrics>,
    stop_rx: watch::Receiver<bool>,
) -> eyre::Result<sync::SyncContext<C>> {
    let event_table_map = build_event_table_map(&startup.resolved_events);
    let receipt_tables = Arc::new(build_receipt_tables(
        &startup.resolved_events,
        &startup.resolved_transfers,
        &startup.resolved_calls,
    ));

    let (handlers, transfer_handlers, call_handlers, has_transfers, has_calls) =
        build_registries(&startup);

    let worker_count = startup.worker_count;
    let index_config = Arc::new(startup.index_config);
    info!(
        contracts = index_config.contracts.len(),
        "loaded index config"
    );

    if !startup.factories.is_empty() {
        db::load_factory_children(db, &index_config, &startup.factories).await?;
    }
    let factories = Arc::new(startup.factories);

    let bloom_filter = build_bloom_filter(&index_config, has_transfers, has_calls, &factories);

    let stream_dispatcher = build_stream_dispatcher(&startup.resolved_streams);

    let session = if cli.verbose {
        p2p::connect_peers::<C>(startup.p2p_port, &startup.trusted_peers).await?
    } else {
        let (done_tx, mut done_rx) = watch::channel(false);
        let spinner_task = tokio::spawn(async move {
            let mut spinner = ui::Spinner::new();
            loop {
                ui::print_connecting(spinner.frame());
                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_millis(80)) => {}
                    _ = done_rx.changed() => break,
                }
            }
        });
        let session = p2p::connect_peers::<C>(startup.p2p_port, &startup.trusted_peers).await?;
        let _ = done_tx.send(true);
        spinner_task.await.ok();
        ui::clear_line();
        session
    };
    info!(
        chain = C::NAME,
        peers = session.pool.len(),
        "connected to p2p network"
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
        verbose: cli.verbose,
        worker_count,
    })
}

/// Resolved startup parameters from TOML config + CLI.
#[derive(Debug)]
struct StartupConfig {
    chain: chain::ChainKind,
    database_url: String,
    api_port: Option<u16>,
    p2p_port: Option<u16>,
    trusted_peers: Vec<reth_network_peers::TrustedPeer>,
    index_config: config::IndexConfig,
    resolved_events: Vec<toml_config::ResolvedEvent>,
    factories: Vec<toml_config::ResolvedFactory>,
    resolved_transfers: Vec<toml_config::ResolvedTransfer>,
    resolved_calls: Vec<toml_config::ResolvedCall>,
    resolved_streams: Vec<toml_config::ResolvedStream>,
    start_block: BlockNumber,
    worker_count: usize,
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
    let sieve_config = toml_config::load_merged_config(config_path)?;
    let config_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
    let resolved = toml_config::resolve_config(&sieve_config, config_dir)?;
    Ok(ResolvedStartup {
        sieve_config,
        resolved,
    })
}

#[expect(clippy::print_stdout, reason = "CLI output for --explain")]
fn print_explain() -> eyre::Result<()> {
    println!("Ethereum event indexer. Connects directly to P2P.");
    println!("No RPC provider. No API keys. No rate limits. No bills.");
    Ok(())
}

/// Resolve database URL from CLI flag or `DATABASE_URL` env var (via `.env`).
///
/// # Errors
///
/// Returns an error if no database URL is available.
fn resolve_database_url(cli: &cli::Cli) -> eyre::Result<String> {
    cli.database_url.clone().ok_or_else(|| {
        eyre::eyre!("no database URL provided. Set DATABASE_URL in .env or use --database-url")
    })
}

/// Resolve the block-processing worker count.
///
/// Precedence: `--workers` (CLI) overrides `[sync] workers` (TOML), which
/// falls back to `cpu_default` (the host CPU count).
///
/// # Errors
///
/// Returns an error if an explicit count of zero is configured.
fn resolve_worker_count(
    cli_workers: Option<usize>,
    toml_workers: Option<usize>,
    cpu_default: usize,
) -> eyre::Result<usize> {
    match cli_workers.or(toml_workers) {
        Some(0) => Err(eyre::eyre!(
            "workers must be at least 1 (set --workers or [sync] workers to a positive value)"
        )),
        Some(n) => Ok(n),
        None => Ok(cpu_default),
    }
}

/// Load TOML config, resolve ABI files, and compute startup parameters.
///
/// # Errors
///
/// Returns an error if the config file cannot be read, parsed, or resolved.
fn load_toml_config(cli: &cli::Cli) -> eyre::Result<StartupConfig> {
    let startup = load_resolved_config(cli)?;
    let database_url = resolve_database_url(cli)?;

    // Resolve API port: CLI > TOML. None = API disabled.
    let api_port = cli
        .api_port
        .or_else(|| startup.sieve_config.api.as_ref().and_then(|a| a.port));

    // Resolve P2P port: CLI > TOML. None = reth default (30303).
    let p2p_port = cli
        .p2p_port
        .or_else(|| startup.sieve_config.p2p.as_ref().and_then(|p| p.port));

    let trusted_peers = parse_trusted_peers(
        startup
            .sieve_config
            .p2p
            .as_ref()
            .map_or(&[][..], |p| &p.trusted_peers),
    )?;

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

    let chain_kind = startup
        .sieve_config
        .chain
        .as_deref()
        .map_or(Ok(chain::ChainKind::default()), chain::ChainKind::parse)?;

    // Resolve worker count: CLI > TOML > CPU count. Zero is rejected.
    let cpu_default = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
    let worker_count = resolve_worker_count(
        cli.workers,
        startup.sieve_config.sync.as_ref().and_then(|s| s.workers),
        cpu_default,
    )?;

    Ok(StartupConfig {
        chain: chain_kind,
        database_url,
        api_port,
        p2p_port,
        trusted_peers,
        index_config: startup.resolved.index_config,
        resolved_events: startup.resolved.resolved_events,
        factories: startup.resolved.factories,
        resolved_transfers: startup.resolved.transfers,
        resolved_calls: startup.resolved.calls,
        resolved_streams: startup.resolved.streams,
        start_block,
        worker_count,
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
    use owo_colors::OwoColorize;

    let config_path = Path::new(&cli.config);
    if config_path.exists() {
        return Err(eyre::eyre!("{} already exists", cli.config));
    }

    std::fs::create_dir_all("abis")
        .map_err(|e| eyre::eyre!("failed to create abis/ directory: {e}"))?;

    let template = r#"[api]
port = 4000  # omit [api] section to disable GraphQL

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

    let check = "\u{2713}".green();
    println!("\n  {check} {}", cli.config);

    // Write .env with DATABASE_URL (and POSTGRES_PASSWORD for docker mode)
    let env_path = Path::new(".env");
    if !env_path.exists() {
        let env_content = if docker {
            "DATABASE_URL=postgres://postgres:sieve@db:5432/sieve\nPOSTGRES_PASSWORD=sieve\n"
        } else {
            "DATABASE_URL=postgres://postgres:sieve@localhost:5432/sieve\n"
        };
        std::fs::write(env_path, env_content)
            .map_err(|e| eyre::eyre!("failed to write .env: {e}"))?;
        println!("  {check} .env");
    }

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
        println!("  {check} abis/erc20.json");
    }

    if docker {
        write_docker_compose()?;
        println!("  {check} docker-compose.yml");
        println!("\n  Run {} to start", "docker compose up".green());
    } else {
        println!(
            "\n  Run {} to start indexing USDC transfers",
            "sieve".green()
        );
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
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
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
      - "30304:30304/udp"  # discv5 on OP Mainnet / Unichain / World Chain
    depends_on:
      db:
        condition: service_healthy
    environment:
      DATABASE_URL: ${DATABASE_URL}
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
    let database_url = resolve_database_url(cli)?;
    let chain_kind = resolve_config_chain(cli, None)?;

    let db = db::Database::connect(&database_url).await?;
    db::drop_all_tables(
        &db,
        chain_kind.name(),
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
/// Spawn the "Fetching ABI" spinner; send `true` on the returned channel
/// to stop it.
fn spawn_etherscan_spinner() -> (watch::Sender<bool>, tokio::task::JoinHandle<()>) {
    use std::io::Write as _;

    let (done_tx, mut done_rx) = watch::channel(false);
    let spinner_task = tokio::spawn(async move {
        let mut spinner = ui::Spinner::new();
        loop {
            let mut stderr = std::io::stderr();
            let _ = write!(
                stderr,
                "\x1b[2K\r  {} Fetching ABI from Etherscan...",
                spinner.frame()
            );
            stderr.flush().ok();
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_millis(80)) => {}
                _ = done_rx.changed() => break,
            }
        }
    });
    (done_tx, spinner_task)
}

#[expect(clippy::print_stdout, reason = "CLI output for add-contract command")]
async fn cmd_add_contract(
    cli: &cli::Cli,
    address: &str,
    name_override: Option<&str>,
    start_block: Option<u64>,
    api_key: Option<&str>,
) -> eyre::Result<()> {
    use owo_colors::OwoColorize;
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

    // Query the block explorer for the configured chain, not mainnet.
    let chain_id = resolve_config_chain(cli, None)?.etherscan_chain_id();

    // Spinner while fetching from Etherscan
    let (done_tx, spinner_task) = spawn_etherscan_spinner();

    let info = etherscan::fetch_contract_info(chain_id, &checksummed, api_key).await?;

    // Fetch creation block if not provided via --start-block
    let start_block = match start_block {
        Some(b) => Some(b),
        None => etherscan::fetch_creation_block(chain_id, &checksummed, api_key)
            .await
            .unwrap_or(None),
    };

    let _ = done_tx.send(true);
    spinner_task.await.ok();
    ui::clear_line();
    println!("  {} Fetching ABI from Etherscan...", "\u{2713}".green());

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

    let check = "\u{2713}".green();
    if info.is_proxy {
        println!(
            "  {check} Proxy detected \u{2192} implementation {}",
            info.name
        );
    }
    let block_info = start_block.map_or_else(String::new, |b| format!(", start_block={b}"));
    println!(
        "  {check} Added {contract_name} to {}\n    {} events{block_info}",
        cli.config,
        events.len(),
    );
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
async fn cmd_peers<C: chain::ChainTypes>() -> eyre::Result<()> {
    println!("Connecting to {} P2P network...", C::NAME);
    let session = p2p::connect_peers::<C>(None, &[]).await?;
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
async fn run_indexer<C: chain::ChainTypes>(
    cli: &cli::Cli,
    start_block: BlockNumber,
    ctx: sync::SyncContext<C>,
) -> eyre::Result<()> {
    let verbose = ctx.verbose;

    // The committed frontier was already quorum-verified (and any
    // reorg-while-stopped recovered) in `prepare_and_run`, before the API
    // was bound and before we reach here.
    if let Some(end_block_raw) = cli.end_block {
        let end_block = BlockNumber::new(end_block_raw);
        let effective_start = resolve_effective_start(&ctx.db, start_block, end_block).await?;

        if effective_start > end_block {
            if !verbose {
                ui::print_info("already indexed, nothing to do");
            }
            info!("nothing to index (committed frontier verified)");
            ctx.metrics
                .is_ready
                .store(true, std::sync::atomic::Ordering::Relaxed);
            return Ok(());
        }

        let metrics = Arc::clone(&ctx.metrics);
        let outcome = run_historical_windows(effective_start, end_block, ctx).await?;

        // Historical sync complete — mark as ready
        metrics
            .is_ready
            .store(true, std::sync::atomic::Ordering::Relaxed);

        if !verbose {
            ui::print_sync_complete(&outcome);
        }

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

/// Prune all indexed rows above the checkpoint (orphans from runs that
/// predate strictly-contiguous commits, or from a torn shutdown).
///
/// Runs before the API is spawned so unverified rows are never served.
/// A zero/absent checkpoint means nothing contiguous was ever committed,
/// so any existing rows in the sync range are orphans by definition.
async fn prune_beyond_checkpoint(db: &db::Database, startup: &StartupConfig) -> eyre::Result<()> {
    let (handlers, transfer_handlers, call_handlers, _, _) = build_registries(startup);
    let checkpoint = db.last_checkpoint().await?.map_or(0, BlockNumber::as_u64);
    let target = BlockNumber::new(checkpoint);
    let mut tx = db.begin().await?;
    handlers.rollback_all(target, &mut tx).await?;
    transfer_handlers.rollback_all(target, &mut tx).await?;
    call_handlers.rollback_all(target, &mut tx).await?;
    db::rollback_factory_children(&mut tx, target, &startup.index_config).await?;
    db::rollback_to(&mut tx, target).await?;
    tx.commit().await?;
    Ok(())
}

/// Maximum blocks handed to one `run_sync` invocation during historical
/// backfill. Bounds the scheduler's materialized block range: a full Base
/// backfill is ~50M blocks, which must not become one giant allocation.
const HISTORICAL_WINDOW_BLOCKS: u64 = 250_000;

/// Run a historical sync in bounded windows, accumulating the outcome.
///
/// # Errors
///
/// Returns an error if any window fails.
async fn run_historical_windows<C: chain::ChainTypes>(
    start: BlockNumber,
    end: BlockNumber,
    ctx: sync::SyncContext<C>,
) -> eyre::Result<sync::engine::SyncOutcome> {
    let mut total = sync::engine::SyncOutcome::default();
    let mut window_start = start.as_u64();
    let end = end.as_u64();

    while window_start <= end {
        if *ctx.stop_rx.borrow() {
            break;
        }
        let window_end = end.min(window_start.saturating_add(HISTORICAL_WINDOW_BLOCKS - 1));
        let outcome = sync::run_canonical_segments(
            BlockNumber::new(window_start),
            BlockNumber::new(window_end),
            ctx.clone(),
        )
        .await?;
        total.accumulate(&outcome);
        if *ctx.stop_rx.borrow() {
            break;
        }
        // Defense in depth: never advance to the next window unless this
        // window's full range is committed behind the checkpoint.
        let checkpoint = ctx
            .db
            .last_checkpoint()
            .await?
            .map_or(0, BlockNumber::as_u64);
        if checkpoint < window_end {
            return Err(eyre::eyre!(
                "window {window_start}-{window_end} reported success but the checkpoint is at \
                 {checkpoint}; refusing to advance"
            ));
        }
        window_start = window_end.saturating_add(1);
    }

    Ok(total)
}

/// Connect to PostgreSQL, optionally drop tables, and create schema.
///
/// # Errors
///
/// Returns an error if the database connection or DDL fails.
async fn setup_database(cli: &cli::Cli, startup: &StartupConfig) -> eyre::Result<db::Database> {
    let db = db::Database::connect(&startup.database_url).await?;
    if cli.fresh {
        info!("--fresh: dropping all tables");
        db::drop_all_tables(
            &db,
            startup.chain.name(),
            &startup.resolved_events,
            &startup.resolved_transfers,
            &startup.resolved_calls,
        )
        .await?;
    }
    db::create_internal_tables(&db).await?;
    // Bind or verify the chain identity before creating user tables or
    // serving anything — a chain mismatch must not mutate schema.
    // (`--fresh` above is the explicit way to switch chains on a reused DB.)
    db::ensure_chain_identity(&db, startup.chain.name(), startup.chain.genesis_hash()).await?;
    db::create_user_tables(&db, &startup.resolved_events).await?;
    db::create_transfer_tables(&db, &startup.resolved_transfers).await?;
    db::create_call_tables(&db, &startup.resolved_calls).await?;
    Ok(db)
}

/// Refuse a configured start that would leave a gap above an existing
/// database's checkpoint.
///
/// An existing database's committed prefix ends at its checkpoint. Sync
/// must continue from `checkpoint + 1`; a configured `start_block` ABOVE
/// that would skip `checkpoint+1..start_block`, and the scalar checkpoint
/// would later advance past the hole. A fresh database (no indexed state)
/// may start anywhere.
///
/// # Errors
///
/// Returns an error on a gap, or if the DB read fails.
async fn enforce_no_start_gap(db: &db::Database, start_block: BlockNumber) -> eyre::Result<()> {
    if !db.has_indexed_state().await? {
        return Ok(());
    }
    let checkpoint = db.last_checkpoint().await?.map_or(0, BlockNumber::as_u64);
    if start_block.as_u64() > checkpoint + 1 {
        return Err(eyre::eyre!(
            "configured start block {} is above this database's checkpoint {checkpoint}; \
             resuming there would leave blocks {}..={} unindexed — lower the start block to \
             {} or below to resume, or use a fresh database",
            start_block.as_u64(),
            checkpoint + 1,
            start_block.as_u64() - 1,
            checkpoint + 1,
        ));
    }
    Ok(())
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
async fn shutdown_handler(stop_tx: watch::Sender<bool>, verbose: bool) {
    // First Ctrl+C → graceful shutdown
    tokio::signal::ctrl_c().await.ok();
    if !verbose {
        ui::clear_line();
    }
    warn!("shutdown signal received; stopping after draining");
    let _ = stop_tx.send(true);

    // Second Ctrl+C → hard exit
    tokio::signal::ctrl_c().await.ok();
    warn!("second shutdown signal received; forcing exit");
    std::process::exit(130);
}

/// Spawn the GraphQL API server if configured.
/// Build the GraphQL schema from resolved config (must run before
/// `startup` is consumed by `build_sync_context`). Returns `None` when no
/// API port is configured.
fn build_api_schema(
    startup: &StartupConfig,
    db: &Arc<db::Database>,
) -> eyre::Result<Option<(u16, async_graphql::dynamic::Schema)>> {
    let Some(port) = startup.api_port else {
        return Ok(None);
    };
    let schema = api::build_schema(
        &startup.resolved_events,
        &startup.resolved_transfers,
        &startup.resolved_calls,
        db.pool().clone(),
    )?;
    Ok(Some((port, schema)))
}

/// Spawn the API server. Called ONLY after the committed frontier is
/// quorum-verified, so the API never serves poisoned or reorged rows.
fn spawn_api_server(
    port: u16,
    schema: async_graphql::dynamic::Schema,
    metrics: &Arc<metrics::SieveMetrics>,
    stop_rx: &watch::Receiver<bool>,
) {
    let api_stop = stop_rx.clone();
    let api_metrics = Arc::clone(metrics);
    tokio::spawn(async move {
        if let Err(e) = api::run_api_server(port, schema, api_metrics, api_stop).await {
            tracing::error!(error = %e, "API server error");
        }
    });
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
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions in tests are idiomatic"
)]
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

    #[test]
    fn worker_count_cli_overrides_toml() -> eyre::Result<()> {
        assert_eq!(resolve_worker_count(Some(2), Some(8), 16)?, 2);
        Ok(())
    }

    #[test]
    fn worker_count_falls_back_to_toml() -> eyre::Result<()> {
        assert_eq!(resolve_worker_count(None, Some(8), 16)?, 8);
        Ok(())
    }

    #[test]
    fn worker_count_falls_back_to_cpu() -> eyre::Result<()> {
        assert_eq!(resolve_worker_count(None, None, 16)?, 16);
        Ok(())
    }

    #[test]
    fn worker_count_rejects_zero() {
        assert!(resolve_worker_count(Some(0), None, 16).is_err());
        // A zero from TOML is rejected even when CLI is absent.
        assert!(resolve_worker_count(None, Some(0), 16).is_err());
        // CLI override still wins: an explicit CLI zero rejects a valid TOML value.
        assert!(resolve_worker_count(Some(0), Some(8), 16).is_err());
    }

    async fn gap_test_db() -> eyre::Result<db::Database> {
        let url = std::env::var("DATABASE_URL").map_err(|_| eyre::eyre!("DATABASE_URL not set"))?;
        let db = db::Database::connect(&url).await?;
        db::create_internal_tables(&db).await?;
        Ok(db)
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn start_gap_above_checkpoint_is_refused() -> eyre::Result<()> {
        let db = gap_test_db().await?;
        // Simulate an existing database indexed to block 100.
        sqlx::query("DELETE FROM _sieve_block_hashes")
            .execute(db.pool())
            .await?;
        let mut tx = db.begin().await?;
        db::store_block_hashes_batch(
            &mut tx,
            &[100],
            &[[0x11u8; 32].to_vec()],
            &[[0x10u8; 32].to_vec()],
        )
        .await?;
        db::update_checkpoint(&mut tx, BlockNumber::new(100)).await?;
        tx.commit().await?;

        // Configured start 200 leaves blocks 101..=199 unindexed → refused.
        let result = enforce_no_start_gap(&db, BlockNumber::new(200)).await;
        assert!(result.is_err());
        assert!(format!("{result:?}").contains("unindexed"));

        // Resuming at or below checkpoint+1 is fine.
        enforce_no_start_gap(&db, BlockNumber::new(101)).await?;
        enforce_no_start_gap(&db, BlockNumber::new(50)).await?;

        sqlx::query("DELETE FROM _sieve_block_hashes")
            .execute(db.pool())
            .await?;
        sqlx::query("UPDATE _sieve_checkpoints SET block_number = 0 WHERE id = 1")
            .execute(db.pool())
            .await?;
        Ok(())
    }
}
