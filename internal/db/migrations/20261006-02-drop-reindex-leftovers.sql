-- +migrate Up notransaction
-- A failed REINDEX CONCURRENTLY leaves an invalid _ccnew/_ccnew1 copy, or a
-- _ccold one if it failed at the final swap. They take disk, and a later
-- REINDEX of the same index errors while a _ccnew exists.
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id_closed_at_ccnew;
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id_closed_at_ccnew1;
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id_closed_at_ccold;
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id_live_until_ccnew;
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id_live_until_ccnew1;
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id_live_until_ccold;

-- +migrate Down notransaction
