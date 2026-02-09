-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied
ALTER TABLE contract_data
ADD column IF NOT EXISTS live_until_ledger_sequence INTEGER;


-- +migrate Down
-- SQL section 'Down' is executed when this migration is rolled back
ALTER TABLE contract_data
DROP COLUMN IF EXISTS live_until_ledger_sequence;
