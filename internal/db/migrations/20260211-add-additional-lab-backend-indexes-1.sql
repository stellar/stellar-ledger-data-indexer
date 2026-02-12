-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied
-- Description: Optimize contract_data indexes for lab-backend TB-scale performance
--
-- NOTE: CONCURRENTLY not supported in migrations

-- 3. Sort index for durability with tiebreaker that improves /storage endpoint
CREATE INDEX IF NOT EXISTS idx_contract_data_contract_id_durability
ON public.contract_data (contract_id, durability DESC, key_hash DESC);


-- +migrate Down

DROP INDEX IF EXISTS idx_contract_data_contract_id_durability;
