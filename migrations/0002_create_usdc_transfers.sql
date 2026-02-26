CREATE TABLE IF NOT EXISTS usdc_transfers (
    id BIGSERIAL PRIMARY KEY,
    block_number BIGINT NOT NULL,
    tx_hash BYTEA NOT NULL,
    tx_index INTEGER NOT NULL,
    log_index INTEGER NOT NULL,
    from_address TEXT NOT NULL,
    to_address TEXT NOT NULL,
    value TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_usdc_transfers_block ON usdc_transfers (block_number);
