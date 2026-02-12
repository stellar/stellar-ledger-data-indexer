-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied
-- Description: Optimize contract_data indexes for lab-backend TB-scale performance
--
-- NOTE: CONCURRENTLY not supported in migrations

-- 1. Keys endpoint: DISTINCT key_symbol per contract
CREATE INDEX IF NOT EXISTS idx_contract_data_contract_id_key_symbol
ON public.contract_data (contract_id, key_symbol)
WHERE key_symbol IS NOT NULL;

-- +migrate Down

DROP INDEX IF EXISTS idx_contract_data_contract_id_key_symbol;
