CREATE TABLE IF NOT EXISTS _sieve_checkpoints (
    id SMALLINT PRIMARY KEY DEFAULT 1,
    block_number BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO _sieve_checkpoints (id, block_number) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;
