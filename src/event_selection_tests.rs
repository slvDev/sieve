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

pub const CL_DEPOSIT: &str = "Deposit(address,uint256,uint128)";
pub const CLASSIC_DEPOSIT: &str = "Deposit(address,address,uint256)";
pub const CL_WITHDRAW: &str = "Withdraw(address,uint256,uint128)";
pub const CLASSIC_WITHDRAW: &str = "Withdraw(address,uint256)";

/// The overload fixture's four selectors with their expected topic0 vectors.
const GAUGE_SELECTORS: [(&str, &str); 4] = [
    (
        CL_DEPOSIT,
        "0x1c8ab8c7f45390d58f58f1d655213a82cca5d12179761a87c16f098813b8f211",
    ),
    (
        CLASSIC_DEPOSIT,
        "0x5548c837ab068cf56a2c2479df0882a4922fd203edb7517321831d95078c5f62",
    ),
    (
        CL_WITHDRAW,
        "0x8903a5b5d08a841e7f68438387f1da20c84dea756379ed37e633ff3854b99b84",
    ),
    (
        CLASSIC_WITHDRAW,
        "0x884edad9ce6fa2440d8a54cc123490eb96d2768479d49ff9c7366125a9424364",
    ),
];

pub fn gauge_abi() -> eyre::Result<JsonAbi> {
    Ok(serde_json::from_str(&std::fs::read_to_string(
        fixture_dir("event-overloads").join("abis/gauge.json"),
    )?)?)
}

#[test]
fn gauge_overload_selectors_match_vectors() -> eyre::Result<()> {
    let abi = gauge_abi()?;
    for (signature, topic0) in GAUGE_SELECTORS {
        let topic0: B256 = topic0.parse()?;
        assert_eq!(
            alloy_primitives::keccak256(signature),
            topic0,
            "{signature}"
        );
        let selected: Vec<_> = abi
            .events()
            .filter(|e| e.signature() == signature)
            .map(alloy_json_abi::Event::selector)
            .collect();
        assert_eq!(selected, [topic0], "{signature}");
    }
    Ok(())
}

/// Unique, identical-duplicate, conflicting-layout, and tuple/array events.
const EXTRA_ABI: &str = r#"[
    {"type":"event","name":"Transfer","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]},
    {"type":"event","name":"Transfer","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]},
    {"type":"event","name":"Sync","anonymous":false,"inputs":[{"name":"reserve","type":"uint112","indexed":false}]},
    {"type":"event","name":"Sync","anonymous":false,"inputs":[{"name":"reserve","type":"uint112","indexed":true}]},
    {"type":"event","name":"Swap","anonymous":false,"inputs":[{"name":"key","type":"tuple","indexed":false,"components":[{"name":"token","type":"address"},{"name":"fee","type":"uint24"}]},{"name":"amounts","type":"int256[2]","indexed":false}]},
    {"type":"event","name":"Swap","anonymous":false,"inputs":[{"name":"amount","type":"uint256","indexed":false}]}
]"#;

const NONCANONICAL: &[&str] = &["no ABI variant", CL_DEPOSIT, CLASSIC_DEPOSIT];

/// (name, signature, Ok((selected signature, first input)) or Err(message substrings)).
type SelectCase = (
    &'static str,
    Option<&'static str>,
    Result<(&'static str, &'static str), &'static [&'static str]>,
);

const SELECT_CASES: [SelectCase; 15] = [
    (
        "Deposit",
        None,
        Err(&[
            "contract 'Gauge'",
            "'Deposit'",
            "is overloaded",
            "Deposit(address,address,uint256), Deposit(address,uint256,uint128)",
            "signature = \"Deposit(address,address,uint256)\"",
        ]),
    ),
    (
        "Withdraw",
        None,
        Err(&["Withdraw(address,uint256), Withdraw(address,uint256,uint128)"]),
    ),
    ("Deposit", Some(CL_DEPOSIT), Ok((CL_DEPOSIT, "user"))),
    (
        "Deposit",
        Some(CLASSIC_DEPOSIT),
        Ok((CLASSIC_DEPOSIT, "from")),
    ),
    ("Withdraw", Some(CL_WITHDRAW), Ok((CL_WITHDRAW, "user"))),
    (
        "Withdraw",
        Some(CLASSIC_WITHDRAW),
        Ok((CLASSIC_WITHDRAW, "from")),
    ),
    (
        "Deposit",
        Some("Deposit(address, uint256, uint128)"),
        Err(NONCANONICAL),
    ),
    (
        "Deposit",
        Some("Deposit(address user,uint256 tokenId,uint128 liquidityToStake)"),
        Err(NONCANONICAL),
    ),
    (
        "Deposit",
        Some("deposit(address,address,uint256)"),
        Err(NONCANONICAL),
    ),
    ("Deposit", Some(CLASSIC_WITHDRAW), Err(NONCANONICAL)),
    (
        "Deposit",
        Some("Deposit(address,uint256)"),
        Err(NONCANONICAL),
    ),
    (
        "Missing",
        None,
        Err(&["event 'Missing' not found in ABI for contract 'Gauge'"]),
    ),
    (
        "Transfer",
        None,
        Ok(("Transfer(address,address,uint256)", "from")),
    ),
    (
        "Sync",
        Some("Sync(uint112)"),
        Err(&["'Sync(uint112)'", "conflicting ABI layouts"]),
    ),
    (
        "Swap",
        Some("Swap((address,uint24),int256[2])"),
        Ok(("Swap((address,uint24),int256[2])", "key")),
    ),
];

#[test]
fn select_event_by_name_and_signature() -> eyre::Result<()> {
    let gauge = gauge_abi()?;
    let mut reversed = gauge_abi()?;
    for variants in reversed.events.values_mut() {
        variants.reverse();
    }
    let extra: JsonAbi = serde_json::from_str(EXTRA_ABI)?;
    for abi in [&gauge, &reversed] {
        for (name, signature, expected) in SELECT_CASES {
            let source = if extra.events.contains_key(name) {
                &extra
            } else {
                abi
            };
            let result = crate::config::select_event(source, "Gauge", name, signature);
            match (result, expected) {
                (Ok(event), Ok((selected, first_input))) => {
                    assert_eq!(event.signature(), selected);
                    assert_eq!(event.inputs[0].name, first_input, "{selected}");
                }
                (Err(err), Err(substrings)) => {
                    let message = err.to_string();
                    for substring in substrings {
                        assert!(message.contains(substring), "{message} lacks {substring}");
                    }
                }
                (result, expected) => {
                    eyre::bail!("{name} {signature:?}: got {result:?}, expected {expected:?}")
                }
            }
        }
    }
    Ok(())
}

/// One resolved table: table, signature, columns, filtered topic indexes.
type ResolvedRow<'a> = (&'a str, &'a str, Vec<&'a str>, Vec<usize>);

#[test]
fn overload_fixture_resolves_each_selector_once() -> eyre::Result<()> {
    let dir = fixture_dir("event-overloads");
    let mut config = load_merged_config(&dir.join("sieve.toml"))?;
    let resolved = resolve_config(&config, &dir)?;
    let rows: Vec<ResolvedRow<'_>> = resolved
        .resolved_events
        .iter()
        .map(|e| {
            (
                e.table_name.as_str(),
                e.signature.as_str(),
                e.columns.iter().map(|c| c.column_name.as_str()).collect(),
                e.topic_filters.iter().map(|f| f.topic_index).collect(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        [
            (
                "gauge_cl_deposit",
                CL_DEPOSIT,
                vec!["user", "token_id", "liquidity_to_stake"],
                vec![]
            ),
            (
                "gauge_classic_deposit",
                CLASSIC_DEPOSIT,
                vec!["from", "to", "amount"],
                vec![]
            ),
            (
                "gauge_cl_withdraw",
                CL_WITHDRAW,
                vec!["user", "token_id", "liquidity_to_stake"],
                vec![2]
            ),
            (
                "gauge_classic_withdraw",
                CLASSIC_WITHDRAW,
                vec!["from", "amount"],
                vec![1]
            ),
        ]
    );

    let contract = &resolved.index_config.contracts[0];
    assert_eq!(contract.events.len(), 4);
    for (event, (signature, topic0)) in resolved.resolved_events.iter().zip(GAUGE_SELECTORS) {
        let topic0: B256 = topic0.parse()?;
        assert_eq!(event.topic0, topic0);
        assert_eq!(contract.events[&topic0].signature(), signature);
    }
    let filtered: HashSet<B256> = contract.topic_filters.keys().copied().collect();
    assert_eq!(
        filtered,
        HashSet::from([
            resolved.resolved_events[2].topic0,
            resolved.resolved_events[3].topic0
        ])
    );

    config.contracts[0].events[1].signature = None;
    let err = resolve_config(&config, &dir).err().map(|e| e.to_string());
    assert!(
        err.as_deref()
            .is_some_and(|e| e
                .contains("Deposit(address,address,uint256), Deposit(address,uint256,uint128)")),
        "{err:?}"
    );
    Ok(())
}

#[test]
fn factory_fixture_resolves_overloaded_child_template() -> eyre::Result<()> {
    let dir = fixture_dir("event-overloads");
    let resolved = resolve_config(&load_config(&dir.join("factory.toml"))?, &dir)?;
    let tables: Vec<(&str, &str)> = resolved
        .resolved_events
        .iter()
        .map(|e| (e.table_name.as_str(), e.signature.as_str()))
        .collect();
    assert_eq!(
        tables,
        [
            ("gauge_cl_deposit", CL_DEPOSIT),
            ("gauge_classic_deposit", CLASSIC_DEPOSIT),
            ("gauge_classic_deposit_copy", CLASSIC_DEPOSIT),
            ("gauge_cl_withdraw", CL_WITHDRAW),
            ("gauge_classic_withdraw", CLASSIC_WITHDRAW),
        ]
    );
    assert_eq!(resolved.index_config.contracts[0].events.len(), 4);
    let factory = &resolved.factories[0];
    assert_eq!(
        factory.creation_selector,
        alloy_primitives::keccak256("GaugeCreated(address)")
    );
    assert_eq!(
        factory.extraction_fingerprint(),
        "event GaugeCreated(address indexed gauge) => gauge"
    );
    Ok(())
}

#[test]
fn explicit_signature_changes_archive_fingerprint() -> eyre::Result<()> {
    let dir = fixture_dir("uniswap-unique");
    let mut config = load_merged_config(&dir.join("sieve.toml"))?;
    let golden = crate::archive::config_fingerprint(&config, &dir)?;
    assert!(!serde_json::to_string(&config.contracts[1].events[1])?.contains("signature"));

    let sql = ordered_sql(&resolve_config(&config, &dir)?);
    config.contracts[1].events[1].signature =
        Some("Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)".to_owned());
    assert_ne!(crate::archive::config_fingerprint(&config, &dir)?, golden);
    assert_eq!(ordered_sql(&resolve_config(&config, &dir)?), sql);
    Ok(())
}
