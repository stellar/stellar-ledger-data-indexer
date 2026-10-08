-- +migrate Up notransaction
-- The previous file built idx_contract_data_contract_id_key_hash, which
-- replaces this index. Returns ~100 GB on pubnet.
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id_key_hash_ledger_sequence_desc;

-- +migrate Down notransaction
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id_key_hash_ledger_sequence_desc;

CREATE INDEX CONCURRENTLY idx_contract_data_contract_id_key_hash_ledger_sequence_desc
ON public.contract_data (contract_id, key_hash, ledger_sequence DESC);
