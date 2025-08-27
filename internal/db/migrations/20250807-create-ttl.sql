-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied
CREATE TABLE IF NOT EXISTS ttl (
    key_hash TEXT,
    ledger_sequence INTEGER NOT NULL,
    live_until_ledger_sequence INTEGER NOT NULL,
    closed_at TIMESTAMP WITH TIME ZONE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_key_hash ON ttl (key_hash);


-- +migrate Down
-- SQL section 'Down' is executed when this migration is rolled back
DROP INDEX IF EXISTS idx_key_hash;
DROP TABLE IF EXISTS ttl;
