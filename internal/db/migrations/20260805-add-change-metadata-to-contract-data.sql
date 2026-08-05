-- +migrate Up
-- SQL in section 'Up' is executed when this migration is applied
-- Description: Persist the two pieces of change metadata the transform already
-- computes for every entry but had nowhere to store.
--
-- 1. deleted -- whether the entry still exists on-chain.
--
-- The indexer already computes this for every removal but the write layer bound
-- no such column, so removals were written as ordinary upserts. That did not
-- merely leave a stale row behind: because the upsert refreshes ledger_sequence
-- and closed_at from the removal ledger, a destroyed entry ended up looking more
-- recently confirmed than while it was actually live, and no column existed for a
-- reader to filter it out.
--
-- A tombstone rather than a DELETE, for two reasons. MAX(ledger_sequence) on this
-- table is also this service's ingestion resume cursor, so rows vanishing would
-- let that cursor move backwards. And a tombstone lets consumers tell "never
-- existed" apart from "existed and was removed", which a DELETE discards.
--
-- When deleted is true, ledger_sequence is the ledger the entry was removed in.
--
-- 2. ledger_entry_change -- which kind of change last wrote this row.
--
-- The raw xdr.LedgerEntryChangeType enum value, matching the INTEGER typing used
-- for the same column throughout Hubble and stellar-dbt-public:
--
--   0  CREATED   entry did not exist before this ledger
--   1  UPDATED   entry existed and its value changed
--   2  REMOVED   entry was deleted on-chain     (implies deleted = true)
--   3  STATE     never stored -- the SDK folds these into the Pre image of the
--                change they accompany and never surfaces them as changes
--   4  RESTORED  an archived entry was brought back (implies deleted = false)
--
-- This is deliberately redundant with deleted, which is exactly
-- (ledger_entry_change = 2). Both are worth keeping: deleted is the stable
-- semantic flag readers filter on, while ledger_entry_change is the provenance
-- detail that additionally separates created from updated from restored among the
-- live rows -- a distinction deleted cannot express.
--
-- Adding columns with constant DEFAULTs is a metadata-only change on
-- PostgreSQL 11+, so this does not rewrite the table.
ALTER TABLE contract_data
ADD COLUMN IF NOT EXISTS deleted BOOLEAN NOT NULL DEFAULT false,
ADD COLUMN IF NOT EXISTS ledger_entry_change INTEGER;


-- +migrate Down
-- SQL section 'Down' is executed when this migration is rolled back
ALTER TABLE contract_data
DROP COLUMN IF EXISTS ledger_entry_change,
DROP COLUMN IF EXISTS deleted;
