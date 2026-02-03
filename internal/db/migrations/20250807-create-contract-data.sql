-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied
CREATE TABLE IF NOT EXISTS contract_data (
    contract_id TEXT,
    ledger_sequence INTEGER NOT NULL,
    key_hash TEXT,
    durability TEXT,
    key_symbol TEXT,
    key BYTEA,
    val BYTEA,
    closed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    live_until_ledger_sequence INTEGER,
    PRIMARY KEY (key_hash)
);
CREATE INDEX IF NOT EXISTS idx_contract_id ON contract_data (contract_id);
CREATE INDEX IF NOT EXISTS idx_key_symbol ON contract_data (key_symbol);


-- +migrate Down
-- SQL section 'Down' is executed when this migration is rolled back
DROP INDEX IF EXISTS idx_contract_id;
DROP INDEX IF EXISTS idx_symbol;
DROP TABLE IF EXISTS contract_data;
