package utils

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lib/pq"
	sdkdb "github.com/stellar/go-stellar-sdk/support/db"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- test doubles -----------------------------------------------------------

// fakeOperator is a DBOperator whose Upsert outcome is scripted per call, so a
// test can express "the batch fails, then these individual rows fail".
type fakeOperator struct {
	table   string
	dataset string
	session *sdkdb.MockSession

	// upsert returns the error for a given call, receiving the batch that was
	// passed so it can fail selectively on a row's contents.
	upsert func(call int, batch []interface{}) error

	calls  int
	passed [][]interface{}
}

func (f *fakeOperator) Upsert(ctx context.Context, data any) error {
	batch, _ := data.([]interface{})
	f.passed = append(f.passed, batch)
	f.calls++
	if f.upsert == nil {
		return nil
	}
	return f.upsert(f.calls-1, batch)
}

func (f *fakeOperator) TableName() string                                    { return f.table }
func (f *fakeOperator) DatasetName() string                                  { return f.dataset }
func (f *fakeOperator) Session() sdkdb.SessionInterface                      { return f.session }
func (f *fakeOperator) GetMaxLedgerSequence(context.Context) (uint32, error) { return 0, nil }

// recordingMetrics captures RecordSkippedRow so tests can assert the labels.
type recordingMetrics struct {
	MetricRecorder
	skipped []string // "dataset/reason" per call
}

func (r *recordingMetrics) RecordSkippedRow(dataset string, reason string) {
	r.skipped = append(r.skipped, dataset+"/"+reason)
}

func pqErr(code string) error {
	return &pq.Error{Code: pq.ErrorCode(code), Message: "test error " + code}
}

// permanentErr is the reported failure: a NUL byte reaching a text column.
func permanentErr() error { return pqErr("22021") }

// transientErr is a serialization failure, which should be retried.
func transientErr() error { return pqErr("40001") }

// newAdapter wires an adapter with a near-zero backoff so retry paths do not
// make tests slow.
func newAdapter(op *fakeOperator, m MetricRecorder) *PostgresAdapter {
	return &PostgresAdapter{
		DBOperator:     op,
		Logger:         log.New(),
		MetricRecorder: m,
		BaseBackoff:    time.Millisecond,
	}
}

// session builds a MockSession that accepts any number of Begin/Commit/Rollback.
func session() *sdkdb.MockSession {
	s := &sdkdb.MockSession{}
	s.On("Begin", mock.Anything).Return(nil).Maybe()
	s.On("Commit").Return(nil).Maybe()
	s.On("Rollback").Return(nil).Maybe()
	return s
}

// --- tests ------------------------------------------------------------------

func TestWrite_HappyPath(t *testing.T) {
	op := &fakeOperator{table: "contract_data", dataset: "contract_data", session: session()}
	a := newAdapter(op, nil)

	err := a.Write(context.Background(), Message{Payload: []interface{}{"a", "b"}})

	require.NoError(t, err)
	assert.Equal(t, 1, op.calls, "a clean batch should be written once, not salvaged row by row")
	assert.Equal(t, []interface{}{"a", "b"}, op.passed[0])
}

func TestWrite_SingleRecordPayloadIsWrapped(t *testing.T) {
	op := &fakeOperator{table: "contract_data", dataset: "contract_data", session: session()}
	a := newAdapter(op, nil)

	require.NoError(t, a.Write(context.Background(), Message{Payload: "solo"}))
	assert.Equal(t, []interface{}{"solo"}, op.passed[0])
}

// TestWrite_OnePermanentRowAmongValidRows is the core of the fix: one
// un-representable row must cost only itself, not the whole ledger.
func TestWrite_OnePermanentRowAmongValidRows(t *testing.T) {
	op := &fakeOperator{table: "contract_data", dataset: "contract_data", session: session()}
	op.upsert = func(call int, batch []interface{}) error {
		if len(batch) > 1 {
			return permanentErr() // the batch attempt
		}
		if batch[0] == "poison" {
			return permanentErr() // only this row is unwritable
		}
		return nil
	}
	m := &recordingMetrics{}
	a := newAdapter(op, m)

	err := a.Write(context.Background(), Message{Payload: []interface{}{"ok1", "poison", "ok2"}})

	require.NoError(t, err, "ingestion must continue after dropping an unwritable row")
	// 1 batch attempt + 3 individual rows.
	assert.Equal(t, 4, op.calls)
	assert.Equal(t, []string{"contract_data/22021"}, m.skipped,
		"exactly the poison row should be counted as skipped")
}

// TestWrite_SkippedRowIsLabelledByDatasetNotTable covers the TTL operator, which
// writes into table contract_data but is its own dataset. A table label would
// make skipped TTL updates indistinguishable from contract-data rows.
func TestWrite_SkippedRowIsLabelledByDatasetNotTable(t *testing.T) {
	op := &fakeOperator{table: "contract_data", dataset: "ttl", session: session()}
	op.upsert = func(call int, batch []interface{}) error { return permanentErr() }
	m := &recordingMetrics{}
	a := newAdapter(op, m)

	require.NoError(t, a.Write(context.Background(), Message{Payload: []interface{}{"x"}}))
	assert.Equal(t, []string{"ttl/22021"}, m.skipped)
}

// TestWrite_TransientDuringSalvageIsRetried pins the retry contract: a transient
// failure while salvaging must fall back into the backoff loop rather than
// aborting ingestion.
func TestWrite_TransientDuringSalvageIsRetried(t *testing.T) {
	op := &fakeOperator{table: "contract_data", dataset: "contract_data", session: session()}
	// Attempt 1: batch fails permanently, then the row hits a transient error.
	// Attempt 2: the batch succeeds.
	op.upsert = func(call int, batch []interface{}) error {
		switch call {
		case 0:
			return permanentErr()
		case 1:
			return transientErr()
		default:
			return nil
		}
	}
	m := &recordingMetrics{}
	a := newAdapter(op, m)

	err := a.Write(context.Background(), Message{Payload: []interface{}{"a"}})

	require.NoError(t, err, "a transient error during salvage must be retried, not returned")
	assert.Equal(t, 3, op.calls, "batch, transient row, then a successful retry of the batch")
	assert.Empty(t, m.skipped, "a transient failure is not a dropped row")
}

func TestWrite_TransientErrorRetriesThenSucceeds(t *testing.T) {
	op := &fakeOperator{table: "contract_data", dataset: "contract_data", session: session()}
	op.upsert = func(call int, batch []interface{}) error {
		if call < 2 {
			return transientErr()
		}
		return nil
	}
	a := newAdapter(op, nil)

	require.NoError(t, a.Write(context.Background(), Message{Payload: []interface{}{"a"}}))
	assert.Equal(t, 3, op.calls)
}

func TestWrite_ExceedsRetriesOnPersistentTransientError(t *testing.T) {
	op := &fakeOperator{table: "contract_data", dataset: "contract_data", session: session()}
	op.upsert = func(call int, batch []interface{}) error { return transientErr() }
	a := newAdapter(op, nil)

	err := a.Write(context.Background(), Message{Payload: []interface{}{"a"}})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeded retries for table contract_data")
	assert.Equal(t, maxRetries, op.calls)
}

func TestWrite_ContextCancellationStopsBackoff(t *testing.T) {
	op := &fakeOperator{table: "contract_data", dataset: "contract_data", session: session()}
	op.upsert = func(call int, batch []interface{}) error { return transientErr() }
	a := &PostgresAdapter{
		DBOperator:  op,
		Logger:      log.New(),
		BaseBackoff: time.Hour, // long enough that only cancellation can end it
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()

	err := a.Write(ctx, Message{Payload: []interface{}{"a"}})
	require.ErrorIs(t, err, context.Canceled)
}

// TestWrite_RollsBackWhenUpsertFails checks the transaction is not left open.
func TestWrite_RollsBackWhenUpsertFails(t *testing.T) {
	s := &sdkdb.MockSession{}
	s.On("Begin", mock.Anything).Return(nil).Maybe()
	s.On("Rollback").Return(nil)
	s.On("Commit").Return(nil).Maybe()

	op := &fakeOperator{table: "contract_data", dataset: "contract_data", session: s}
	op.upsert = func(call int, batch []interface{}) error { return permanentErr() }
	a := newAdapter(op, &recordingMetrics{})

	require.NoError(t, a.Write(context.Background(), Message{Payload: []interface{}{"a"}}))
	s.AssertCalled(t, "Rollback")
}

func TestWrite_BeginFailureIsReported(t *testing.T) {
	s := &sdkdb.MockSession{}
	s.On("Begin", mock.Anything).Return(fmt.Errorf("cannot begin"))

	op := &fakeOperator{table: "contract_data", dataset: "contract_data", session: s}
	a := &PostgresAdapter{DBOperator: op, Logger: log.New(), BaseBackoff: time.Millisecond}

	err := a.Write(context.Background(), Message{Payload: []interface{}{"a"}})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin failed for contract_data")
	assert.Equal(t, 0, op.calls, "Upsert must not run when the transaction never opened")
}

// TestFlush_CommitFailureRollsBack covers the commit path and its rollback.
func TestFlush_CommitFailureRollsBack(t *testing.T) {
	s := &sdkdb.MockSession{}
	s.On("Commit").Return(fmt.Errorf("commit exploded"))
	s.On("Rollback").Return(nil)

	op := &fakeOperator{table: "contract_data", dataset: "contract_data", session: s}
	a := newAdapter(op, nil)

	err := a.Flush(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "rolled back successfully")
	s.AssertCalled(t, "Rollback")
}

func TestFlush_CommitAndRollbackBothFail(t *testing.T) {
	s := &sdkdb.MockSession{}
	s.On("Commit").Return(fmt.Errorf("commit exploded"))
	s.On("Rollback").Return(fmt.Errorf("rollback exploded"))

	op := &fakeOperator{table: "contract_data", dataset: "contract_data", session: s}
	a := newAdapter(op, nil)

	err := a.Flush(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "rollback also failed")
}

func TestClose_ClosesSession(t *testing.T) {
	s := &sdkdb.MockSession{}
	s.On("Close").Return(nil)

	op := &fakeOperator{table: "contract_data", dataset: "contract_data", session: s}
	newAdapter(op, nil).Close()

	s.AssertCalled(t, "Close")
}

func TestGetMaxLedgerSequence_DelegatesToOperator(t *testing.T) {
	op := &fakeOperator{table: "contract_data", dataset: "contract_data", session: session()}
	seq, err := newAdapter(op, nil).GetMaxLedgerSequence(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint32(0), seq)
}

// TestWrite_BatchesAreChunked confirms a payload larger than the batch size is
// split, so the salvage path can only ever affect one chunk.
func TestWrite_BatchesAreChunked(t *testing.T) {
	op := &fakeOperator{table: "contract_data", dataset: "contract_data", session: session()}
	a := newAdapter(op, nil)

	records := make([]interface{}, 2500)
	for i := range records {
		records[i] = i
	}
	require.NoError(t, a.Write(context.Background(), Message{Payload: records}))

	require.Equal(t, 3, op.calls)
	assert.Len(t, op.passed[0], 1000)
	assert.Len(t, op.passed[1], 1000)
	assert.Len(t, op.passed[2], 500)
}
