-- +migrate Up notransaction
-- VACUUM never shrinks an index file: 274 GB on pubnet against ~85 GB fresh.
-- Drop leftovers of a failed rebuild first, so a pod restart can retry.
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id_closed_at_ccnew;
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id_closed_at_ccnew1;
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id_closed_at_ccold;

REINDEX INDEX CONCURRENTLY idx_contract_data_contract_id_closed_at;

-- +migrate Down notransaction
