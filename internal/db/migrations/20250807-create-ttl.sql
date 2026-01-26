-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied
CREATE TABLE IF NOT EXISTS ttl (
    key_hash TEXT,
    ledger_sequence INTEGER NOT NULL,
    live_until_ledger_sequence INTEGER NOT NULL,
    closed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (key_hash)
);


-- +migrate Down
-- SQL section 'Down' is executed when this migration is rolled back
DROP TABLE IF EXISTS ttl;
