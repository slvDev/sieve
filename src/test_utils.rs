//! Shared test utilities used across multiple test modules.

use alloy_primitives::{Address, Bytes, Log, LogData, B256};
use reth_ethereum_primitives::Receipt;

use crate::api::types::ColumnMeta;

/// Build a log with given address, topics, and data.
pub fn make_log(addr: Address, topics: Vec<B256>, data: Bytes) -> Log<LogData> {
    Log {
        address: addr,
        data: LogData::new_unchecked(topics, data),
    }
}

/// Build a minimal receipt with given logs.
pub fn make_receipt(logs: Vec<Log<LogData>>) -> Receipt {
    Receipt {
        tx_type: alloy_consensus::TxType::Legacy,
        success: true,
        cumulative_gas_used: 0,
        logs,
    }
}

/// Build a minimal legacy transaction for testing.
pub fn build_test_transaction() -> reth_ethereum_primitives::TransactionSigned {
    use alloy_consensus::{Signed, TxLegacy};
    use alloy_primitives::{Signature, TxKind, U256};

    let tx = TxLegacy {
        chain_id: Some(1),
        nonce: 0,
        gas_price: 0,
        gas_limit: 21000,
        to: TxKind::Call(Address::ZERO),
        value: U256::ZERO,
        input: Bytes::new(),
    };

    let sig = Signature::new(U256::from(1u64), U256::from(1u64), false);
    let signed = Signed::new_unchecked(tx, sig, B256::repeat_byte(0xAA));
    reth_ethereum_primitives::TransactionSigned::Legacy(signed)
}

/// Standard test columns for query builder tests.
pub fn test_columns() -> Vec<ColumnMeta> {
    vec![
        ColumnMeta {
            name: "id".into(),
            pg_type: "bigserial".into(),
            nullable: false,
        },
        ColumnMeta {
            name: "block_number".into(),
            pg_type: "bigint".into(),
            nullable: false,
        },
        ColumnMeta {
            name: "tx_hash".into(),
            pg_type: "bytea".into(),
            nullable: false,
        },
        ColumnMeta {
            name: "tx_index".into(),
            pg_type: "integer".into(),
            nullable: false,
        },
        ColumnMeta {
            name: "from_address".into(),
            pg_type: "text".into(),
            nullable: false,
        },
        ColumnMeta {
            name: "value".into(),
            pg_type: "numeric".into(),
            nullable: false,
        },
        ColumnMeta {
            name: "active".into(),
            pg_type: "boolean".into(),
            nullable: false,
        },
    ]
}
