-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied
-- Description: Optimize contract_data indexes for lab-backend TB-scale performance
--
-- NOTE: CONCURRENTLY not supported in migrations


-- 2. Sort index for closed_at with tiebreaker that improves /storage endpoint
CREATE INDEX IF NOT EXISTS idx_contract_data_contract_id_closed_at
ON public.contract_data (contract_id, closed_at DESC, key_hash DESC);

-- +migrate Down

DROP INDEX IF EXISTS idx_contract_data_contract_id_closed_at;
