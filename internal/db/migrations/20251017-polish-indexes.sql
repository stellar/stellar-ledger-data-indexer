-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied

-- contract_data indexes:
---- Rename indexes to use the table name as a prefix:
ALTER INDEX idx_contract_id RENAME TO idx_contract_data_contract_id;
ALTER INDEX idx_key_hash RENAME TO idx_contract_data_key_hash;
ALTER INDEX idx_key_symbol RENAME TO idx_contract_data_key_symbol;

---- Create new index (CONCURRENTLY not supported in migrations):
CREATE INDEX IF NOT EXISTS idx_contract_data_contract_id_key_hash_ledger_sequence_desc
ON public.contract_data (contract_id, key_hash, ledger_sequence DESC);

-- ttl index:
---- Create new index (CONCURRENTLY not supported in migrations):
CREATE INDEX IF NOT EXISTS idx_ttl_key_hash_ledger_sequence_desc
ON public.ttl (key_hash, ledger_sequence DESC);


-- +migrate Down
-- SQL section 'Down' is executed when this migration is rolled back
-- ttl index:
DROP INDEX IF EXISTS idx_ttl_key_hash_ledger_sequence_desc;

-- contract_data indexes:
DROP INDEX IF EXISTS idx_contract_data_contract_id_key_hash_ledger_sequence_desc;
ALTER INDEX idx_contract_data_key_symbol RENAME TO idx_key_symbol;
ALTER INDEX idx_contract_data_key_hash RENAME TO idx_key_hash;
ALTER INDEX idx_contract_data_contract_id RENAME TO idx_contract_id;
