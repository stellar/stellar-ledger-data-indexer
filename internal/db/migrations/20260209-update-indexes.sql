-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied

CREATE INDEX IF NOT EXISTS idx_ledger_sequence ON contract_data (ledger_sequence);

-- +migrate Down
-- SQL section 'Down' is executed when this migration is rolled back
DROP INDEX IF EXISTS idx_ledger_sequence;
