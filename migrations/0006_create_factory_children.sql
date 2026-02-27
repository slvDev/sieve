CREATE TABLE IF NOT EXISTS _sieve_factory_children (
    id BIGSERIAL PRIMARY KEY,
    factory_name TEXT NOT NULL,
    child_address BYTEA NOT NULL,
    block_number BIGINT NOT NULL,
    UNIQUE (child_address)
);
