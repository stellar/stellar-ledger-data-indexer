package db

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/support/db/dbtest"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-ledger-data-indexer/internal/contract"
	"github.com/stellar/stellar-ledger-data-indexer/internal/utils"
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
	_, err = session.UpsertRows(ctx, "contract_data", "key_hash", legacy, nil, nil)
	require.NoError(t, err)
	assertDeleted(t, session, keyHash, nil)

	replay, err := contractDataUpsertFields([]interface{}{
		testOutput(keyHash, removalLedger, xdr.LedgerEntryChangeTypeLedgerEntryRemoved),
	})
	require.NoError(t, err)
	_, err = session.UpsertRows(ctx, "contract_data", "key_hash", replay, nil, contractDataUpsertConditions())
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
	_, err = session.UpsertRows(ctx, "contract_data", "key_hash", current, nil, nil)
	require.NoError(t, err)

	stale, err := contractDataUpsertFields([]interface{}{
		testOutput(keyHash, 2100, xdr.LedgerEntryChangeTypeLedgerEntryRemoved),
	})
	require.NoError(t, err)
	_, err = session.UpsertRows(ctx, "contract_data", "key_hash", stale, nil, contractDataUpsertConditions())
	require.NoError(t, err)

	assertDeleted(t, session, keyHash, boolPtr(false))
	assertLedgerSequence(t, session, keyHash, 2200)
}

// TestUpsertTTLMerge runs the contract_data and ttl writers in ledger order.
// The TTL carried on a contract_data row makes the ttl dataset's write for the
// same ledger a no-op; a replay must not lower a TTL raised by a later TTL-only
// ledger, while a newer ledger can still lower it (delete and recreate).
func TestUpsertTTLMerge(t *testing.T) {
	session := postgresSessionForTest(t)
	ctx := context.Background()
	metrics := &upsertCountRecorder{}
	contractData := NewContractDataDBOperator(*session, metrics)
	ttl := NewTTLDBOperator(*session, metrics)

	const keyHash = "ttl-merge"
	withTTL := func(ledger uint32, liveUntil *uint32) []interface{} {
		row := testOutput(keyHash, ledger, xdr.LedgerEntryChangeTypeLedgerEntryUpdated)
		row.LiveUntilLedgerSeq = liveUntil
		return []interface{}{row}
	}
	ttlChange := func(liveUntil uint32) []interface{} {
		return []interface{}{contract.TtlOutput{KeyHash: keyHash, LiveUntilLedgerSeq: liveUntil}}
	}

	// Ledger 100 changes data and TTL: one row version, the ttl write finds nothing to do.
	require.NoError(t, contractData.Upsert(ctx, withTTL(100, uint32Ptr(1000))))
	assertLiveUntil(t, session, keyHash, uint32Ptr(1000))
	require.NoError(t, ttl.Upsert(ctx, ttlChange(1000)))
	assert.Equal(t, int64(0), metrics.last["ttl"], "ttl write after the merged upsert should touch no row")
	assertLiveUntil(t, session, keyHash, uint32Ptr(1000))

	// A later TTL-only ledger raises the TTL without advancing ledger_sequence.
	require.NoError(t, ttl.Upsert(ctx, ttlChange(2000)))
	assert.Equal(t, int64(1), metrics.last["ttl"])
	assertLiveUntil(t, session, keyHash, uint32Ptr(2000))

	// A replay of ledger 100 keeps the raised TTL.
	require.NoError(t, contractData.Upsert(ctx, withTTL(100, uint32Ptr(1000))))
	assertLiveUntil(t, session, keyHash, uint32Ptr(2000))

	// A newer ledger that did not change the TTL keeps it too.
	require.NoError(t, contractData.Upsert(ctx, withTTL(110, nil)))
	assertLiveUntil(t, session, keyHash, uint32Ptr(2000))
	assertLedgerSequence(t, session, keyHash, 110)

	// A newer ledger that deleted and recreated the entry lowers it.
	require.NoError(t, contractData.Upsert(ctx, withTTL(120, uint32Ptr(500))))
	assertLiveUntil(t, session, keyHash, uint32Ptr(500))
	assertLedgerSequence(t, session, keyHash, 120)
}

// TestUpsertTTLMergeFillsMissingTTLOnReplay covers a row whose ttl write never
// landed, e.g. the pod stopped between the two datasets' writes of a ledger.
// The replay of that ledger fills the TTL in the contract_data upsert itself,
// so the ttl dataset's write that follows is a no-op.
func TestUpsertTTLMergeFillsMissingTTLOnReplay(t *testing.T) {
	session := postgresSessionForTest(t)
	ctx := context.Background()
	metrics := &upsertCountRecorder{}
	contractData := NewContractDataDBOperator(*session, metrics)
	ttl := NewTTLDBOperator(*session, metrics)

	const keyHash = "ttl-missing"
	row := testOutput(keyHash, 200, xdr.LedgerEntryChangeTypeLedgerEntryUpdated)
	require.NoError(t, contractData.Upsert(ctx, []interface{}{row}))
	assertLiveUntil(t, session, keyHash, nil)

	row.LiveUntilLedgerSeq = uint32Ptr(700)
	require.NoError(t, contractData.Upsert(ctx, []interface{}{row}))
	assertLiveUntil(t, session, keyHash, uint32Ptr(700))
	assertLedgerSequence(t, session, keyHash, 200)

	require.NoError(t, ttl.Upsert(ctx, []interface{}{contract.TtlOutput{KeyHash: keyHash, LiveUntilLedgerSeq: 700}}))
	assert.Equal(t, int64(0), metrics.last["ttl"], "ttl write after the merged upsert should touch no row")
}

// upsertCountRecorder keeps the rows-affected count of the last write per dataset.
type upsertCountRecorder struct {
	utils.MetricRecorder
	last map[string]int64
}

func (r *upsertCountRecorder) RecordUpsertCount(dataset string, count int64) {
	if r.last == nil {
		r.last = map[string]int64{}
	}
	r.last[dataset] = count
}

func assertLiveUntil(t *testing.T, session *DBSession, keyHash string, want *uint32) {
	t.Helper()
	var got *int64
	require.NoError(t, session.session.GetRaw(context.Background(), &got,
		"SELECT live_until_ledger_sequence FROM contract_data WHERE key_hash = $1", keyHash))
	if want == nil {
		assert.Nil(t, got, "live_until_ledger_sequence should still be NULL")
		return
	}
	require.NotNil(t, got, "live_until_ledger_sequence should have been written")
	assert.Equal(t, int64(*want), *got)
}

func uint32Ptr(n uint32) *uint32 { return &n }

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
