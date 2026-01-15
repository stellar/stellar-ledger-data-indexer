package input

import (
	"testing"

	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/log"
)

func TestGetLedgerBound(t *testing.T) {
	logger := log.New()

	tests := []struct {
		startLedger         uint32
		endLedger           uint32
		latestNetworkLedger uint32
		expectedRange       ledgerbackend.Range
	}{
		{0, 1, 100, ledgerbackend.UnboundedRange(100)},
		{1, 1, 100, ledgerbackend.UnboundedRange(100)},
		{50, 0, 100, ledgerbackend.UnboundedRange(50)},
		{50, 150, 100, ledgerbackend.UnboundedRange(50)},
		{50, 70, 100, ledgerbackend.BoundedRange(50, 70)},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := GetLedgerBound(tt.startLedger, tt.endLedger, tt.latestNetworkLedger, logger)
			if result != tt.expectedRange {
				t.Errorf("expected %v, got %v", tt.expectedRange, result)
			}
		})
	}
}
