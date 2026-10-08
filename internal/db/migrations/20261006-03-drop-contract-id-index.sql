-- +migrate Up notransaction
-- (contract_id) is the leading column of every other contract_data index, so
-- the planner already has a prefix for contract_id lookups. 7 GB on pubnet.
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id;

-- +migrate Down notransaction
-- A failed CREATE INDEX CONCURRENTLY leaves an INVALID index that IF NOT
-- EXISTS would keep, so the exact name is dropped first.
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id;

CREATE INDEX CONCURRENTLY idx_contract_data_contract_id
ON public.contract_data (contract_id);
