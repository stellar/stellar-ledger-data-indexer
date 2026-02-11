-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied
-- Description: Optimize contract_data indexes for lab-backend TB-scale performance
--
-- NOTE: CONCURRENTLY not supported in migrations

-- 4. Sort index for live_until_ledger_sequence with tiebreaker that improves /storage endpoint
CREATE INDEX IF NOT EXISTS idx_contract_data_contract_id_live_until
ON public.contract_data (contract_id, live_until_ledger_sequence DESC, key_hash DESC);

-- 5. Cleanup: single-column key_symbol redundant with composite above
DROP INDEX IF EXISTS idx_contract_data_key_symbol;


-- +migrate Down

CREATE INDEX IF NOT EXISTS idx_contract_data_key_symbol ON public.contract_data (key_symbol);

DROP INDEX IF EXISTS idx_contract_data_contract_id_live_until;
