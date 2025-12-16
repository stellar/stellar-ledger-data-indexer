-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied

-- contract_data indexes:
---- Rename indexes to use the table name as a prefix:
ALTER INDEX idx_contract_id RENAME TO idx_contract_data_id;
ALTER INDEX idx_key_decoded RENAME TO idx_contract_data_key_decoded;
ALTER INDEX idx_key_hash RENAME TO idx_contract_data_key_hash;

---- Create new index (CONCURRENTLY not supported in migrations):
CREATE INDEX IF NOT EXISTS idx_contract_data_id_keyhash_ledger_sequence_desc
ON public.contract_data (id, key_hash, ledger_sequence DESC);

-- transaction index:
---- Rename index to use the table name as a prefix:
ALTER INDEX idx_transaction_hash RENAME TO idx_transaction_transaction_hash;

-- ttl index:
---- Create new index (CONCURRENTLY not supported in migrations):
CREATE INDEX IF NOT EXISTS idx_ttl_keyhash_ledger_sequence_desc
ON public.ttl (key_hash, ledger_sequence DESC);


-- +migrate Down
-- SQL section 'Down' is executed when this migration is rolled back
-- ttl index:
DROP INDEX IF EXISTS idx_ttl_keyhash_ledger_sequence_desc;

-- transaction index:
ALTER INDEX idx_transaction_transaction_hash RENAME TO idx_transaction_hash;

-- contract_data indexes:
DROP INDEX IF EXISTS idx_contract_data_id_keyhash_ledger_sequence_desc;
ALTER INDEX idx_contract_data_key_hash RENAME TO idx_key_hash;
ALTER INDEX idx_contract_data_key_decoded RENAME TO idx_key_decoded;
ALTER INDEX idx_contract_data_id RENAME TO idx_contract_id;
