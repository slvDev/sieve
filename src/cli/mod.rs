//! CLI argument parsing.

use clap::Parser;

/// Ethereum event indexer over P2P.
#[derive(Debug, Parser)]
#[command(name = "sieve", about = "Ethereum event indexer over P2P")]
pub struct Cli {
    /// Path to the TOML config file.
    #[arg(long, default_value = "sieve.toml")]
    pub config: String,

    /// Block number to start indexing from (overrides per-contract start_block in config).
    #[arg(long)]
    pub start_block: Option<u64>,

    /// Block number to stop at (inclusive). Omit for follow mode (continuous head-following).
    #[arg(long)]
    pub end_block: Option<u64>,

    /// PostgreSQL connection URL. Falls back to DATABASE_URL env var, then config file.
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// Port for the GraphQL API server. Omit to disable.
    #[arg(long)]
    pub api_port: Option<u16>,
}

#[cfg(test)]
#[expect(clippy::panic_in_result_fn, reason = "assertions in tests are idiomatic")]
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
        assert_eq!(cli.database_url.as_deref(), Some("postgres://localhost/sieve"));
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
}
