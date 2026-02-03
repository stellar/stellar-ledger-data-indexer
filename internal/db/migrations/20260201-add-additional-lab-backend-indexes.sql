-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied
-- Description: Optimize contract_data and ttl indexes for TB-scale performance

-- 1. Subquery optimization that improves all queries from lab backend
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ttl_ledger_sequence_desc
ON ttl (ledger_sequence DESC);

-- 2. Keys endpoint optimization that improves /keys endpoint
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contract_data_contract_key_symbol
ON contract_data (contract_id, key_symbol)
WHERE key_symbol IS NOT NULL;

-- 3. Sort index for closed_at with tiebreaker that improves /storage endpoint
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contract_data_contract_closed_at
ON contract_data (contract_id, closed_at DESC, key_hash DESC);

-- 4. Sort index for durability with tiebreaker that improves /storage endpoint
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contract_data_contract_durability
ON contract_data (contract_id, durability, key_hash);

-- 5. CLEANUP: Remove redundant single-column index already covered by composite index(es)
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_id_loadtest;

-- +migrate Down

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contract_data_contract_id_loadtest
ON contract_data (contract_id);

DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_durability;
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_closed_at;
DROP INDEX CONCURRENTLY IF EXISTS idx_contract_data_contract_key_symbol;
DROP INDEX CONCURRENTLY IF EXISTS idx_ttl_ledger_sequence_desc;
