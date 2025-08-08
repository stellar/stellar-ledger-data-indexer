-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied
CREATE TABLE IF NOT EXISTS contract_data (
    id TEXT,
    ledger_sequence INTEGER NOT NULL,
    durability TEXT NOT NULL,
    key TEXT NOT NULL,
    key_decoded TEXT NOT NULL,
    val TEXT NOT NULL,
    val_decoded TEXT NOT NULL,
    closed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_contract_id ON contract_data (id);


-- +migrate Down
-- SQL section 'Down' is executed when this migration is rolled back
DROP INDEX IF EXISTS idx_contract_id;
DROP TABLE IF EXISTS contract_data;
