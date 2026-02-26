ALTER TABLE usdc_transfers
ADD CONSTRAINT uq_usdc_transfers_block_tx_log
UNIQUE (block_number, tx_index, log_index);
