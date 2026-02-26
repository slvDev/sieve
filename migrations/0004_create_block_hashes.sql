CREATE TABLE IF NOT EXISTS _sieve_block_hashes (
    block_number BIGINT PRIMARY KEY,
    block_hash BYTEA NOT NULL
);
