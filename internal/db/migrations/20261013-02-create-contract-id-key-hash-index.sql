-- +migrate Up notransaction
-- Replaces (contract_id, key_hash, ledger_sequence DESC): key_hash is the
-- primary key, so ledger_sequence orders nothing but changes on every upsert.
-- Dropped first because IF NOT EXISTS keeps an INVALID index from a failed build.
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id_key_hash;

CREATE INDEX CONCURRENTLY idx_contract_data_contract_id_key_hash
ON public.contract_data (contract_id, key_hash);

-- +migrate Down notransaction
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id_key_hash;
