-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied
CREATE TABLE IF NOT EXISTS transaction (
    id TEXT,
    ledger_sequence INTEGER NOT NULL,
    transaction_hash TEXT PRIMARY KEY,
    closed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    tx_envelope TEXT NOT NULL,
    tx_result TEXT NOT NULL,
    tx_meta TEXT NOT NULL,
    tx_fee_meta TEXT NOT NULL,
    successful BOOLEAN NOT NULL,
    diagnostic_events TEXT NOT NULL,
    transaction_events TEXT NOT NULL,
    contract_events TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_transaction_hash ON transaction (transaction_hash);


-- +migrate Down
-- SQL section 'Down' is executed when this migration is rolled back
DROP INDEX IF EXISTS idx_transaction_hash;
DROP TABLE IF EXISTS transaction;
