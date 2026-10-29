//! Event selection: unique-name compatibility goldens captured on v0.3.3.
#![expect(clippy::panic_in_result_fn, reason = "test assertions")]
use super::*;
use sha2::{Digest, Sha256};

fn fixture_dir(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// Every table's CREATE, index, INSERT, and rollback SQL in resolution order.
fn ordered_sql(resolved: &ResolvedConfig) -> String {
    let mut sql = String::new();
    for event in &resolved.resolved_events {
        for statement in std::iter::once(&event.create_table_sql)
            .chain(&event.create_indexes_sql)
            .chain([&event.insert_sql, &event.rollback_sql])
        {
            sql.push_str(statement);
            sql.push('\n');
        }
    }
    sql
}

#[test]
fn unique_name_config_matches_v0_3_3_goldens() -> eyre::Result<()> {
    let dir = fixture_dir("uniswap-unique");
    let config = load_merged_config(&dir.join("sieve.toml"))?;
    assert_eq!(
        crate::archive::config_fingerprint(&config, &dir)?,
        "d33a45c4982a265808b6bb3d6c8ce209a49b310f6eebd1677db031862429eddc"
    );

    let resolved = resolve_config(&config, &dir)?;
    let tables: Vec<&str> = resolved
        .resolved_events
        .iter()
        .map(|e| e.table_name.as_str())
        .collect();
    assert_eq!(
        tables,
        [
            "v3_swaps",
            "v3_mints",
            "v3_burns",
            "v3_initializations",
            "v3_swaps_by_router",
            "v4_initializations",
            "v4_swaps",
            "v4_modify_liquidity",
        ]
    );
    let sql = ordered_sql(&resolved);
    assert_eq!(
        format!("{:x}", Sha256::digest(sql.as_bytes())),
        "59c7542bb7fe864e6b48c12e0f6f5559489d09b91ee9ffdef066c092ce539b27",
        "{sql}"
    );

    let mints = &resolved.resolved_events[1];
    assert_eq!(
        mints.create_table_sql,
        "CREATE TABLE IF NOT EXISTS v3_mints (\n  id BIGSERIAL PRIMARY KEY,\n  \
         block_number BIGINT NOT NULL,\n  tx_hash BYTEA NOT NULL,\n  tx_index INTEGER NOT NULL,\n  \
         log_index INTEGER NOT NULL,\n  contract_address TEXT NOT NULL,\n  \
         tx_gas_used BIGINT NOT NULL,\n  tx_nonce BIGINT NOT NULL,\n  \
         cumulative_gas_used BIGINT NOT NULL,\n  tx_status BOOLEAN NOT NULL,\n  \
         \"lp_owner\" text NOT NULL,\n  \"amount\" text NOT NULL,\n  \
         UNIQUE (contract_address, block_number, tx_index, log_index)\n)"
    );
    assert_eq!(
        mints.insert_sql,
        "INSERT INTO v3_mints (block_number, tx_hash, tx_index, log_index, contract_address, \
         tx_gas_used, tx_nonce, cumulative_gas_used, tx_status, \"lp_owner\", \"amount\") \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
         ON CONFLICT (contract_address, block_number, tx_index, log_index) DO NOTHING"
    );
    let v4_init = &resolved.resolved_events[5];
    assert_eq!(
        v4_init.insert_sql,
        "INSERT INTO v4_initializations (block_number, tx_hash, tx_index, log_index, block_hash, \
         tx_value, \"param_id\", \"currency0\", \"currency1\", \"fee\", \"tick_spacing\", \
         \"hooks\", \"sqrt_price_x96\", \"tick\") VALUES ($1, $2, $3, $4, $5, $6::numeric, $7, \
         $8, $9, $10, $11, $12, $13::numeric, $14) \
         ON CONFLICT (block_number, tx_index, log_index) DO NOTHING"
    );
    assert_eq!(
        v4_init.rollback_sql,
        "DELETE FROM v4_initializations WHERE block_number > $1"
    );

    let fingerprints: Vec<String> = resolved
        .factories
        .iter()
        .map(ResolvedFactory::extraction_fingerprint)
        .collect();
    assert_eq!(
        fingerprints,
        ["event PoolCreated(address indexed token0, address indexed token1, uint24 indexed fee, \
          int24 tickSpacing, address pool) => pool"]
    );
    Ok(())
}
