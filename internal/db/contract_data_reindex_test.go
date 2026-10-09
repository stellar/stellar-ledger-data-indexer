package db

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/support/db/dbtest"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// postgresSessionForTest brings up the scratch database these tests need. CI
// runs them against the postgres:16 service; a laptop without one skips rather
// than fails, so `go test ./internal/...` stays usable locally.
func postgresSessionForTest(t *testing.T) *DBSession {
	t.Helper()

	conn, err := net.DialTimeout("tcp", "localhost:5432", time.Second)
	if err != nil {
		t.Skipf("skipping DB-backed test: no postgres on localhost:5432 (%v)", err)
	}
	_ = conn.Close()

	testDB := dbtest.Postgres(t)
	t.Cleanup(testDB.Close)

	session, err := NewPostgresSession(context.Background(), testDB.DSN)
	require.NoError(t, err, "NewPostgresSession also applies the migrations")

	// Registered after testDB.Close, so it runs first: cleanups are last-added,
	// first-called. That order matters. testDB.Close drops the scratch database,
	// and DROP DATABASE fails with "is being accessed by other users" while this
	// session still holds a pooled connection -- dbtest attempts
	// pg_terminate_backend but documents it as best effort.
	t.Cleanup(func() {
		if err := session.session.Close(); err != nil {
			t.Logf("closing test session: %v", err)
		}
	})

	return session
}

// TestUpsertReindexTombstonesLegacyPhantomRow is the regression test for the
// conflict guard. It reconstructs the pre-fix state — an entry removed at ledger
// 2100, written as an ordinary upsert so the row sits at ledger_sequence 2100
// with deleted still NULL — and asserts that replaying that same ledger lands
// the tombstone. Under a strictly-greater guard the write is skipped and a
// historical re-index corrects nothing.
func TestUpsertReindexTombstonesLegacyPhantomRow(t *testing.T) {
	session := postgresSessionForTest(t)
	ctx := context.Background()

	const keyHash = "legacy-phantom"
	const removalLedger = uint32(2100)

	legacy, err := contractDataUpsertFields([]interface{}{
		testOutput(keyHash, removalLedger, xdr.LedgerEntryChangeTypeLedgerEntryRemoved),
	})
	require.NoError(t, err)
	legacy = withoutColumns(legacy, "deleted", "ledger_entry_change")
	_, err = session.UpsertRows(ctx, "contract_data", "key_hash", legacy, nil)
	require.NoError(t, err)
	assertDeleted(t, session, keyHash, nil)

	replay, err := contractDataUpsertFields([]interface{}{
		testOutput(keyHash, removalLedger, xdr.LedgerEntryChangeTypeLedgerEntryRemoved),
	})
	require.NoError(t, err)
	_, err = session.UpsertRows(ctx, "contract_data", "key_hash", replay, contractDataUpsertConditions())
	require.NoError(t, err)

	assertDeleted(t, session, keyHash, boolPtr(true))
}

// TestUpsertDoesNotRewindNewerRows is the bound that makes the equal case safe:
// a replayed older ledger must still lose to newer state already stored, so a
// re-index cannot undo live ingestion running alongside it.
func TestUpsertDoesNotRewindNewerRows(t *testing.T) {
	session := postgresSessionForTest(t)
	ctx := context.Background()

	const keyHash = "newer-wins"

	current, err := contractDataUpsertFields([]interface{}{
		testOutput(keyHash, 2200, xdr.LedgerEntryChangeTypeLedgerEntryUpdated),
	})
	require.NoError(t, err)
	_, err = session.UpsertRows(ctx, "contract_data", "key_hash", current, nil)
	require.NoError(t, err)

	stale, err := contractDataUpsertFields([]interface{}{
		testOutput(keyHash, 2100, xdr.LedgerEntryChangeTypeLedgerEntryRemoved),
	})
	require.NoError(t, err)
	_, err = session.UpsertRows(ctx, "contract_data", "key_hash", stale, contractDataUpsertConditions())
	require.NoError(t, err)

	assertDeleted(t, session, keyHash, boolPtr(false))
	assertLedgerSequence(t, session, keyHash, 2200)
}

// withoutColumns drops bound columns so a test can write a row the way a
// pre-migration build would have written it.
func withoutColumns(fields []UpsertField, drop ...string) []UpsertField {
	dropped := make(map[string]bool, len(drop))
	for _, name := range drop {
		dropped[name] = true
	}
	kept := make([]UpsertField, 0, len(fields))
	for _, f := range fields {
		if !dropped[f.name] {
			kept = append(kept, f)
		}
	}
	return kept
}

func assertDeleted(t *testing.T, session *DBSession, keyHash string, want *bool) {
	t.Helper()
	var got *bool
	require.NoError(t, session.session.GetRaw(context.Background(), &got,
		"SELECT deleted FROM contract_data WHERE key_hash = $1", keyHash))
	if want == nil {
		assert.Nil(t, got, "deleted should still be NULL (unknown)")
		return
	}
	require.NotNil(t, got, "deleted should have been written")
	assert.Equal(t, *want, *got)
}

func assertLedgerSequence(t *testing.T, session *DBSession, keyHash string, want int64) {
	t.Helper()
	var got int64
	require.NoError(t, session.session.GetRaw(context.Background(), &got,
		"SELECT ledger_sequence FROM contract_data WHERE key_hash = $1", keyHash))
	assert.Equal(t, want, got)
}

func boolPtr(b bool) *bool { return &b }
