package input

import (
	"testing"

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/log"
	"github.com/stretchr/testify/assert"
)

func TestGetLedgerBound(t *testing.T) {
	logger := log.New()

	tests := []struct {
		name                string
		startLedger         uint32
		endLedger           uint32
		latestNetworkLedger uint32
		backfill            bool
		maxLedgerInDB       uint32
		expectedRange       ledgerbackend.Range
		shouldProceed       bool
		description         string
	}{
		// Database empty, non-backfill mode tests
		{
			name:                "DB empty, start <= 1, unbounded - use latest network",
			startLedger:         0,
			endLedger:           1,
			latestNetworkLedger: 100,
			backfill:            false,
			maxLedgerInDB:       0,
			expectedRange:       ledgerbackend.UnboundedRange(100),
			shouldProceed:       true,
		},
		{
			name:                "DB empty, start <= 1, unbounded - use latest network (start=1)",
			startLedger:         1,
			endLedger:           1,
			latestNetworkLedger: 100,
			backfill:            false,
			maxLedgerInDB:       0,
			expectedRange:       ledgerbackend.UnboundedRange(100),
			shouldProceed:       true,
		},
		{
			name:                "DB empty, start < latest, unbounded - use requested start",
			startLedger:         50,
			endLedger:           0,
			latestNetworkLedger: 100,
			backfill:            false,
			maxLedgerInDB:       0,
			expectedRange:       ledgerbackend.UnboundedRange(50),
			shouldProceed:       true,
		},
		{
			name:                "DB empty, start < latest, bounded - use requested start",
			startLedger:         50,
			endLedger:           70,
			latestNetworkLedger: 100,
			backfill:            false,
			maxLedgerInDB:       0,
			expectedRange:       ledgerbackend.BoundedRange(50, 70),
			shouldProceed:       true,
		},
		{
			name:                "DB empty, start > latest - error",
			startLedger:         150,
			endLedger:           200,
			latestNetworkLedger: 100,
			backfill:            false,
			maxLedgerInDB:       0,
			expectedRange:       ledgerbackend.Range{},
			shouldProceed:       false,
		},

		// Database has data, non-backfill mode tests
		{
			name:                "DB has data, start < max DB, unbounded - use max DB",
			startLedger:         2,
			endLedger:           1, // unbounded
			latestNetworkLedger: 200,
			backfill:            false,
			maxLedgerInDB:       100,
			expectedRange:       ledgerbackend.UnboundedRange(100),
			shouldProceed:       true,
		},
		{
			name:                "DB has data, start >= max DB, unbounded - use requested start",
			startLedger:         100,
			endLedger:           1, // unbounded
			latestNetworkLedger: 200,
			backfill:            false,
			maxLedgerInDB:       50,
			expectedRange:       ledgerbackend.UnboundedRange(100),
			shouldProceed:       true,
		},
		{
			name:                "DB has data, start < max DB, bounded, max >= end - nothing to do",
			startLedger:         2,
			endLedger:           50,
			latestNetworkLedger: 200,
			backfill:            false,
			maxLedgerInDB:       100,
			expectedRange:       ledgerbackend.Range{},
			shouldProceed:       false,
		},
		{
			name:                "DB has data, start < max DB, bounded, max == end - nothing to do",
			startLedger:         2,
			endLedger:           100,
			latestNetworkLedger: 200,
			backfill:            false,
			maxLedgerInDB:       100,
			expectedRange:       ledgerbackend.Range{},
			shouldProceed:       false,
		},
		{
			name:                "DB has data, start < max DB, bounded, max < end - resume from max",
			startLedger:         2,
			endLedger:           100,
			latestNetworkLedger: 200,
			backfill:            false,
			maxLedgerInDB:       50,
			expectedRange:       ledgerbackend.BoundedRange(50, 100),
			shouldProceed:       true,
		},
		{
			name:                "DB has data, start >= max DB, bounded - use requested start",
			startLedger:         100,
			endLedger:           200,
			latestNetworkLedger: 300,
			backfill:            false,
			maxLedgerInDB:       50,
			expectedRange:       ledgerbackend.BoundedRange(100, 200),
			shouldProceed:       true,
		},
		{
			name:                "DB has data, start == max DB, bounded - use requested start",
			startLedger:         50,
			endLedger:           100,
			latestNetworkLedger: 200,
			backfill:            false,
			maxLedgerInDB:       50,
			expectedRange:       ledgerbackend.BoundedRange(50, 100),
			shouldProceed:       true,
		},

		// Backfill mode tests
		{
			name:                "Backfill, bounded - uses exact range",
			startLedger:         10,
			endLedger:           100,
			latestNetworkLedger: 200,
			backfill:            true,
			maxLedgerInDB:       50, // Should be ignored in backfill mode
			expectedRange:       ledgerbackend.BoundedRange(10, 100),
			shouldProceed:       true,
		},
		{
			name:                "Backfill, unbounded - uses exact start",
			startLedger:         10,
			endLedger:           1,
			latestNetworkLedger: 200,
			backfill:            true,
			maxLedgerInDB:       50, // Should be ignored in backfill mode
			expectedRange:       ledgerbackend.UnboundedRange(10),
			shouldProceed:       true,
		},
		{
			name:                "Backfill, start <= 1, unbounded - use latest network",
			startLedger:         0,
			endLedger:           1,
			latestNetworkLedger: 200,
			backfill:            true,
			maxLedgerInDB:       50, // Should be ignored in backfill mode
			expectedRange:       ledgerbackend.UnboundedRange(200),
			shouldProceed:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, proceed := GetLedgerBound(tt.startLedger, tt.endLedger, tt.latestNetworkLedger, tt.backfill, tt.maxLedgerInDB, logger)
			assert.Equal(t, tt.shouldProceed, proceed, "shouldProceed mismatch")
			if proceed {
				assert.Equal(t, tt.expectedRange, result, "Range mismatch")
			}
		})
	}
}
