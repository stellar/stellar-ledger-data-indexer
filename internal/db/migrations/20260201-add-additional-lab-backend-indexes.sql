-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied
-- Description: Optimize contract_data and ttl indexes for TB-scale performance

-- 1. Subquery optimization that improves all queries from lab backend
-- NOTE: CONCURRENTLY not supported in migrations
CREATE INDEX IF NOT EXISTS idx_ttl_ledger_sequence_desc
ON public.ttl (ledger_sequence DESC);

-- 2. Keys endpoint optimization that improves /keys endpoint
-- NOTE: CONCURRENTLY not supported in migrations
CREATE INDEX IF NOT EXISTS idx_contract_data_contract_id_key_symbol
ON public.contract_data (contract_id, key_symbol)
WHERE key_symbol IS NOT NULL;

-- 3. Sort index for closed_at with tiebreaker that improves /storage endpoint
-- NOTE: CONCURRENTLY not supported in migrations
CREATE INDEX IF NOT EXISTS idx_contract_data_contract_id_closed_at
ON public.contract_data (contract_id, closed_at DESC, key_hash DESC);

-- 4. Sort index for durability with tiebreaker that improves /storage endpoint
-- NOTE: CONCURRENTLY not supported in migrations
CREATE INDEX IF NOT EXISTS idx_contract_data_contract_id_durability
ON public.contract_data (contract_id, durability, key_hash);

-- 5. Cleanup redundant index
DROP INDEX IF EXISTS idx_contract_data_key_symbol;


-- +migrate Down

CREATE INDEX IF NOT EXISTS idx_contract_data_key_symbol ON public.contract_data (key_symbol);

DROP INDEX IF EXISTS idx_contract_data_contract_id_durability;
DROP INDEX IF EXISTS idx_contract_data_contract_id_closed_at;
DROP INDEX IF EXISTS idx_contract_data_contract_id_key_symbol;
DROP INDEX IF EXISTS idx_ttl_ledger_sequence_desc;
