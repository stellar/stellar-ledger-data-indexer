-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied
-- Description: Drop and recreate idx_contract_data_contract_id_live_until to purge
-- dead tuple entries accumulated during concurrent backfill ingestion.
-- Dead tuples from upsert conflicts cause the index scan to follow thousands of
-- dead heap pointers before finding live rows, inflating buffer hits significantly.

DROP INDEX IF EXISTS idx_contract_data_contract_id_live_until;

CREATE INDEX idx_contract_data_contract_id_live_until
ON public.contract_data (contract_id, live_until_ledger_sequence DESC, key_hash DESC);

-- +migrate Down

DROP INDEX IF EXISTS idx_contract_data_contract_id_live_until;

CREATE INDEX idx_contract_data_contract_id_live_until
ON public.contract_data (contract_id, live_until_ledger_sequence DESC, key_hash DESC);
