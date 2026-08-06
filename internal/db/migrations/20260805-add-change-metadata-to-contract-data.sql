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
-- Both columns are nullable with no default, and that is load-bearing rather
-- than incidental. Rows written before this migration cannot be classified:
-- removals were recorded as ordinary live-looking rows, and the information
-- needed to tell them apart exists only in archived ledger metadata, not in this
-- table. The indexer resumes from MAX(ledger_sequence) and only ever moves
-- forward, so it will not revisit them. Defaulting them to false would assert
-- "this entry still exists" about rows we never actually checked, and the read
-- side would then filter them "correctly" while silently passing every
-- pre-migration removal through as live.
--
-- So there are three states, and consumers need to handle all three:
--
--   NULL   unknown -- written before this migration, never reclassified
--   false  known live
--   true   known removed (tombstone)
--
-- IMPORTANT for readers: filter with `deleted IS NOT TRUE`, not `NOT deleted`.
-- NOT NULL evaluates to NULL, so `WHERE NOT deleted` would silently drop every
-- legacy row from the result instead of including it.
--
-- The NULL population is the backlog: SELECT count(*) ... WHERE deleted IS NULL
-- measures how much of the table a historical reingestion still has to correct,
-- and shrinks to zero as it completes. Clearing it requires a re-index, which
-- has to wait for the key_symbol sanitization fix so a backfill cannot die
-- mid-range.
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
-- Adding nullable columns with no default is a metadata-only change, so this
-- does not rewrite the table.
ALTER TABLE contract_data
ADD COLUMN IF NOT EXISTS deleted BOOLEAN,
ADD COLUMN IF NOT EXISTS ledger_entry_change INTEGER;


-- +migrate Down
-- SQL section 'Down' is executed when this migration is rolled back
ALTER TABLE contract_data
DROP COLUMN IF EXISTS ledger_entry_change,
DROP COLUMN IF EXISTS deleted;
