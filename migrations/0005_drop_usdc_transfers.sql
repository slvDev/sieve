-- Drop the hardcoded usdc_transfers table.
-- User-defined tables are now created at runtime from sieve.toml config.
DROP TABLE IF EXISTS usdc_transfers;
