-- +migrate Up notransaction
-- key_hash is the primary key, so the trailing ledger_sequence DESC in
-- idx_contract_data_contract_id_key_hash_ledger_sequence_desc never orders
-- anything, but it changes on every upsert and bloats the index (100 GB on
-- pubnet, ~67 fresh). (contract_id, key_hash) serves the same lookups and
-- key_hash sorts, and its key never changes after insert.
-- This file builds the replacement; the next one drops the old index, so a
-- retry of either file never leaves lab-backend queries without one.
-- Needs +70 GB free while it runs.
-- CREATE INDEX ... IF NOT EXISTS does not repair an INVALID index left by a
-- failed build, so the exact name is dropped first.
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id_key_hash;

CREATE INDEX CONCURRENTLY idx_contract_data_contract_id_key_hash
ON public.contract_data (contract_id, key_hash);

-- +migrate Down notransaction
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id_key_hash;
