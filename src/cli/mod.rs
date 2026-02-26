//! CLI argument parsing.

use clap::Parser;

/// Ethereum event indexer over P2P.
#[derive(Debug, Parser)]
#[command(name = "sieve", about = "Ethereum event indexer over P2P")]
pub struct Cli {
    /// Block number to start indexing from.
    #[arg(long)]
    pub start_block: u64,

    /// Block number to stop at (inclusive). Omit for follow mode (continuous head-following).
    #[arg(long)]
    pub end_block: Option<u64>,

    /// PostgreSQL connection URL. Falls back to DATABASE_URL env var.
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,
}

#[cfg(test)]
#[expect(clippy::panic_in_result_fn, reason = "assertions in tests are idiomatic")]
mod tests {
    use super::*;

    #[test]
    fn parse_all_args() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from([
            "sieve",
            "--start-block",
            "21000000",
            "--end-block",
            "21000100",
            "--database-url",
            "postgres://localhost/sieve",
        ])?;
        assert_eq!(cli.start_block, 21_000_000);
        assert_eq!(cli.end_block, Some(21_000_100));
        assert_eq!(cli.database_url, "postgres://localhost/sieve");
        Ok(())
    }

    #[test]
    fn end_block_is_optional() -> Result<(), clap::Error> {
        let cli = Cli::try_parse_from([
            "sieve",
            "--start-block",
            "100",
            "--database-url",
            "postgres://localhost/sieve",
        ])?;
        assert_eq!(cli.end_block, None);
        Ok(())
    }

    #[test]
    fn missing_start_block_fails() {
        let cli = Cli::try_parse_from([
            "sieve",
            "--database-url",
            "postgres://localhost/sieve",
        ]);
        assert!(cli.is_err());
    }
}
