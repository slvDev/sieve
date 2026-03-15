//! CLI argument parsing.

use clap::{Parser, Subcommand};

/// Ethereum event indexer over P2P.
#[derive(Debug, Parser)]
#[command(name = "sieve", version, about = "Ethereum event indexer over P2P")]
pub struct Cli {
    /// Path to the TOML config file.
    #[arg(long, default_value = "sieve.toml", global = true)]
    pub config: String,

    /// PostgreSQL connection URL. Falls back to DATABASE_URL env var, then config file.
    #[arg(long, env = "DATABASE_URL", global = true)]
    pub database_url: Option<String>,

    /// Subcommand (init, schema, reset). Omit to run the indexer.
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Block number to start indexing from (overrides per-contract start_block in config).
    #[arg(long)]
    pub start_block: Option<u64>,

    /// Block number to stop at (inclusive). Omit for follow mode (continuous head-following).
    #[arg(long)]
    pub end_block: Option<u64>,

    /// Port for the GraphQL API server. Omit to disable.
    #[arg(long)]
    pub api_port: Option<u16>,

    /// Drop and recreate all tables before indexing. Use to start fresh.
    #[arg(long)]
    pub fresh: bool,

    /// Enable verbose tracing output (default: pretty UI).
    #[arg(short = 'v', long)]
    pub verbose: bool,
}

/// Utility subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Scaffold a new Sieve project (creates sieve.toml and abis/erc20.json).
    Init {
        /// Also generate a docker-compose.yml for running Sieve with PostgreSQL.
        #[arg(long)]
        docker: bool,
    },
    /// Print the SQL DDL that Sieve would generate from the config.
    Schema,
    /// Drop all tables and recreate them (like --fresh without starting the indexer).
    Reset,
    /// Fetch a contract ABI from Etherscan and add it to the config.
    AddContract {
        /// Contract address (hex with 0x prefix).
        address: String,
        /// Override the contract name (default: from Etherscan).
        #[arg(long)]
        name: Option<String>,
        /// Block number to start indexing from.
        #[arg(long)]
        start_block: Option<u64>,
        /// Etherscan API key (or set ETHERSCAN_API_KEY env var).
        #[arg(long, env = "ETHERSCAN_API_KEY")]
        etherscan_api_key: Option<String>,
    },
    /// Dry-run: show tables, columns, and filters from the config.
    Inspect,
    /// Connect to P2P network and report peer count (no DB or config needed).
    Peers,
}

#[cfg(test)]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions in tests are idiomatic"
)]
mod tests {
    use super::*;

    #[test]
    fn parse_all_args() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from([
            "sieve",
            "--config",
            "custom.toml",
            "--start-block",
            "21000000",
            "--end-block",
            "21000100",
            "--database-url",
            "postgres://localhost/sieve",
        ])?;
        assert_eq!(cli.config, "custom.toml");
        assert_eq!(cli.start_block, Some(21_000_000));
        assert_eq!(cli.end_block, Some(21_000_100));
        assert_eq!(
            cli.database_url.as_deref(),
            Some("postgres://localhost/sieve")
        );
        Ok(())
    }

    #[test]
    fn config_defaults_to_sieve_toml() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve"])?;
        assert_eq!(cli.config, "sieve.toml");
        Ok(())
    }

    #[test]
    fn start_block_is_optional() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve"])?;
        assert!(cli.start_block.is_none());
        Ok(())
    }

    #[test]
    fn database_url_is_optional() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve"])?;
        // database_url is None unless provided via flag or env
        // (env may be set in CI, so just check it parses)
        let _ = cli.database_url;
        Ok(())
    }

    #[test]
    fn end_block_is_optional() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve"])?;
        assert_eq!(cli.end_block, None);
        Ok(())
    }

    #[test]
    fn api_port_parsed() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve", "--api-port", "8080"])?;
        assert_eq!(cli.api_port, Some(8080));
        Ok(())
    }

    #[test]
    fn api_port_is_optional() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve"])?;
        assert!(cli.api_port.is_none());
        Ok(())
    }

    #[test]
    fn fresh_defaults_to_false() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve"])?;
        assert!(!cli.fresh);
        Ok(())
    }

    #[test]
    fn fresh_flag_parsed() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve", "--fresh"])?;
        assert!(cli.fresh);
        Ok(())
    }

    #[test]
    fn verbose_defaults_to_false() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve"])?;
        assert!(!cli.verbose);
        Ok(())
    }

    #[test]
    fn verbose_short_flag_parsed() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve", "-v"])?;
        assert!(cli.verbose);
        Ok(())
    }

    #[test]
    fn verbose_long_flag_parsed() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve", "--verbose"])?;
        assert!(cli.verbose);
        Ok(())
    }

    #[test]
    fn parse_init_subcommand() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve", "init"])?;
        assert!(matches!(cli.command, Some(Command::Init { docker: false })));
        Ok(())
    }

    #[test]
    fn parse_init_with_docker_flag() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve", "init", "--docker"])?;
        assert!(matches!(cli.command, Some(Command::Init { docker: true })));
        Ok(())
    }

    #[test]
    fn parse_schema_subcommand() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve", "schema"])?;
        assert!(matches!(cli.command, Some(Command::Schema)));
        Ok(())
    }

    #[test]
    fn parse_reset_subcommand() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve", "reset"])?;
        assert!(matches!(cli.command, Some(Command::Reset)));
        Ok(())
    }

    #[test]
    fn parse_schema_with_custom_config() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve", "schema", "--config", "custom.toml"])?;
        assert!(matches!(cli.command, Some(Command::Schema)));
        assert_eq!(cli.config, "custom.toml");
        Ok(())
    }

    #[test]
    fn no_subcommand_is_none() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve"])?;
        assert!(cli.command.is_none());
        Ok(())
    }

    #[test]
    fn parse_add_contract_subcommand() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from([
            "sieve",
            "add-contract",
            "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        ])?;
        assert!(matches!(cli.command, Some(Command::AddContract { .. })));
        if let Some(Command::AddContract {
            address,
            name,
            start_block,
            etherscan_api_key,
        }) = cli.command
        {
            assert_eq!(address, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
            assert!(name.is_none());
            assert!(start_block.is_none());
            assert!(etherscan_api_key.is_none());
        }
        Ok(())
    }

    #[test]
    fn parse_add_contract_with_all_flags() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from([
            "sieve",
            "add-contract",
            "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "--name",
            "USDC",
            "--start-block",
            "21000000",
            "--etherscan-api-key",
            "MYKEY123",
        ])?;
        if let Some(Command::AddContract {
            address,
            name,
            start_block,
            etherscan_api_key,
        }) = cli.command
        {
            assert_eq!(address, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48");
            assert_eq!(name.as_deref(), Some("USDC"));
            assert_eq!(start_block, Some(21_000_000));
            assert_eq!(etherscan_api_key.as_deref(), Some("MYKEY123"));
        }
        Ok(())
    }

    #[test]
    fn parse_inspect_subcommand() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve", "inspect"])?;
        assert!(matches!(cli.command, Some(Command::Inspect)));
        Ok(())
    }

    #[test]
    fn parse_inspect_with_custom_config() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from(["sieve", "inspect", "--config", "custom.toml"])?;
        assert!(matches!(cli.command, Some(Command::Inspect)));
        assert_eq!(cli.config, "custom.toml");
        Ok(())
    }
}
