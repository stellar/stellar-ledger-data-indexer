package utils

import (
	"fmt"
	"testing"

	"github.com/lib/pq"
	sdkerrors "github.com/stellar/go-stellar-sdk/support/errors"
	"github.com/stretchr/testify/assert"
)

func TestIsPermanentWriteErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error is not permanent",
			err:  nil,
			want: false,
		},
		{
			name: "plain error is not permanent",
			err:  fmt.Errorf("connection reset by peer"),
			want: false,
		},
		{
			// The reported failure: a NUL byte reaching a text column.
			name: "character_not_in_repertoire is permanent",
			err:  &pq.Error{Code: "22021", Message: `invalid byte sequence for encoding "UTF8": 0x00`},
			want: true,
		},
		{
			name: "untranslatable_character is permanent",
			err:  &pq.Error{Code: "22P05"},
			want: true,
		},
		{
			name: "string_data_right_truncation is permanent",
			err:  &pq.Error{Code: "22001"},
			want: true,
		},
		// Constraint and schema failures are deliberately NOT permanent. They fail
		// every row identically, so classifying them as droppable would let the
		// row-by-row salvage erase a whole batch and report success.
		{
			name: "datatype_mismatch is not droppable (schema disagreement)",
			err:  &pq.Error{Code: "42804"},
			want: false,
		},
		{
			name: "not_null_violation is not droppable (code defect)",
			err:  &pq.Error{Code: "23502"},
			want: false,
		},
		{
			name: "unique_violation is not droppable (upsert resolves the PK)",
			err:  &pq.Error{Code: "23505"},
			want: false,
		},
		{
			name: "check_violation is not droppable (migration defect)",
			err:  &pq.Error{Code: "23514"},
			want: false,
		},
		{
			name: "serialization_failure is retryable",
			err:  &pq.Error{Code: "40001"},
			want: false,
		},
		{
			name: "deadlock_detected is retryable",
			err:  &pq.Error{Code: "40P01"},
			want: false,
		},
		{
			name: "admin_shutdown is retryable",
			err:  &pq.Error{Code: "57P01"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isPermanentWriteErr(tt.err))
		})
	}
}

// TestIsPermanentWriteErrThroughRealWrapping guards the assumption the
// classifier is built on: by the time a driver error reaches PostgresAdapter it
// has been wrapped by DBSession.UpsertRows (fmt.Errorf %w) around the SDK's
// Session.ExecRaw (support/errors.Wrap, i.e. github.com/pkg/errors). If any
// layer ever stops supporting Unwrap, errors.As silently stops matching, every
// permanent error is misclassified as transient, and the crash-loop this guards
// against comes straight back. Assert the real chain rather than a bare error.
func TestIsPermanentWriteErrThroughRealWrapping(t *testing.T) {
	driverErr := &pq.Error{
		Code:    "22021",
		Message: `invalid byte sequence for encoding "UTF8": 0x00`,
	}

	// Mirrors support/db.Session.ExecRaw.
	wrapped := sdkerrors.Wrap(driverErr, "exec failed")
	// Mirrors db.DBSession.UpsertRows.
	wrapped = fmt.Errorf("upsert rows exec failed: %w", wrapped)

	assert.True(t, isPermanentWriteErr(wrapped),
		"errors.As must reach *pq.Error through the real wrapping chain; got %v", wrapped)
	assert.Equal(t, "22021", skipReason(wrapped))
}

func TestSkipReasonUnknownForNonPQError(t *testing.T) {
	assert.Equal(t, "unknown", skipReason(fmt.Errorf("some transport failure")))
}

func TestChunkRecords(t *testing.T) {
	records := make([]int, 0, 5)
	for i := 0; i < 5; i++ {
		records = append(records, i)
	}

	assert.Equal(t, [][]int{{0, 1}, {2, 3}, {4}}, chunkRecords(records, 2))
	assert.Equal(t, [][]int{{0, 1, 2, 3, 4}}, chunkRecords(records, 10))
	assert.Nil(t, chunkRecords([]int{}, 3))
}
