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
		maxLedgerInDB       uint32
		expectedRange       ledgerbackend.Range
		shouldProceed       bool
		description         string
	}{
		{
			name:                "start_ledger_0_uses_latest",
			startLedger:         0,
			endLedger:           1,
			latestNetworkLedger: 100,
			maxLedgerInDB:       0,
			expectedRange:       ledgerbackend.UnboundedRange(100),
			shouldProceed:       true,
		},
		{
			name:                "start_ledger_1_uses_latest",
			startLedger:         1,
			endLedger:           1,
			latestNetworkLedger: 100,
			maxLedgerInDB:       0,
			expectedRange:       ledgerbackend.UnboundedRange(100),
			shouldProceed:       true,
		},
		{
			name:                "no_end_ledger_unbounded",
			startLedger:         50,
			endLedger:           0,
			latestNetworkLedger: 100,
			maxLedgerInDB:       0,
			expectedRange:       ledgerbackend.UnboundedRange(50),
			shouldProceed:       true,
		},
		{
			name:                "end_ledger_greater_than_latest_unbounded",
			startLedger:         50,
			endLedger:           150,
			latestNetworkLedger: 100,
			maxLedgerInDB:       0,
			expectedRange:       ledgerbackend.UnboundedRange(50),
			shouldProceed:       true,
		},
		{
			name:                "bounded_range",
			startLedger:         50,
			endLedger:           70,
			latestNetworkLedger: 100,
			maxLedgerInDB:       0,
			expectedRange:       ledgerbackend.BoundedRange(50, 70),
			shouldProceed:       true,
		},
		{
			name:                "Empty database - use requested start",
			startLedger:         2,
			endLedger:           1, // unbounded
			latestNetworkLedger: 200,
			maxLedgerInDB:       0,
			expectedRange:       ledgerbackend.UnboundedRange(2),
			shouldProceed:       true,
		},
		{
			name:                "Unbounded mode - resume from max ledger",
			startLedger:         2,
			endLedger:           1, // unbounded
			latestNetworkLedger: 200,
			maxLedgerInDB:       100,
			expectedRange:       ledgerbackend.UnboundedRange(100),
			shouldProceed:       true,
		},
		{
			name:                "Bounded mode - all data ingested",
			startLedger:         2,
			endLedger:           50,
			latestNetworkLedger: 200,
			maxLedgerInDB:       100,
			expectedRange:       ledgerbackend.Range{},
			shouldProceed:       false,
		},
		{
			name:                "Bounded mode - exact match, all data ingested",
			startLedger:         2,
			endLedger:           100,
			latestNetworkLedger: 200,
			maxLedgerInDB:       100,
			expectedRange:       ledgerbackend.Range{},
			shouldProceed:       false,
		},
		{
			name:                "Bounded mode - partial data, resume from max",
			startLedger:         2,
			endLedger:           100,
			latestNetworkLedger: 200,
			maxLedgerInDB:       50,
			expectedRange:       ledgerbackend.BoundedRange(50, 100),
			shouldProceed:       true,
		},
		{
			name:                "Bounded mode - max in DB before start, use requested start",
			startLedger:         100,
			endLedger:           200,
			latestNetworkLedger: 300,
			maxLedgerInDB:       50,
			expectedRange:       ledgerbackend.BoundedRange(100, 200),
			shouldProceed:       true,
		},
		{
			name:                "Bounded mode - max equals start, resume from max",
			startLedger:         50,
			endLedger:           100,
			latestNetworkLedger: 200,
			maxLedgerInDB:       50,
			expectedRange:       ledgerbackend.BoundedRange(50, 100),
			shouldProceed:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, proceed := GetLedgerBound(tt.startLedger, tt.endLedger, tt.latestNetworkLedger, tt.maxLedgerInDB, logger)
			assert.Equal(t, tt.shouldProceed, proceed, "shouldProceed mismatch")
			if proceed {
				assert.Equal(t, tt.expectedRange, result, "Range mismatch")
			}
		})
	}
}
