package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostgresConnString(t *testing.T) {
	actualPostgresString := PostgresConnString(
		PostgresConfig{
			Host:     "localhost",
			Port:     5432,
			Database: "ledger_data",
			User:     "ledger_user",
		},
	)
	expected := "host=localhost port=5432 user=ledger_user dbname=ledger_data sslmode=disable"
	assert.Equal(t, expected, actualPostgresString, "expected %s, got %s", expected, actualPostgresString)
}

func TestDetermineStartLedger(t *testing.T) {
	tests := []struct {
		name            string
		requestedStart  uint32
		requestedEnd    uint32
		maxLedgerInDB   uint32
		expectedStart   uint32
		shouldProceed   bool
	}{
		{
			name:           "Empty database - use requested start",
			requestedStart: 2,
			requestedEnd:   1, // unbounded
			maxLedgerInDB:  0,
			expectedStart:  2,
			shouldProceed:  true,
		},
		{
			name:           "Unbounded mode - resume from max ledger",
			requestedStart: 2,
			requestedEnd:   1, // unbounded
			maxLedgerInDB:  100,
			expectedStart:  100,
			shouldProceed:  true,
		},
		{
			name:           "Bounded mode - all data ingested",
			requestedStart: 2,
			requestedEnd:   50,
			maxLedgerInDB:  100,
			shouldProceed:  false,
		},
		{
			name:           "Bounded mode - exact match, all data ingested",
			requestedStart: 2,
			requestedEnd:   100,
			maxLedgerInDB:  100,
			shouldProceed:  false,
		},
		{
			name:           "Bounded mode - partial data, resume from max",
			requestedStart: 2,
			requestedEnd:   100,
			maxLedgerInDB:  50,
			expectedStart:  50,
			shouldProceed:  true,
		},
		{
			name:           "Bounded mode - max in DB before start, use requested start",
			requestedStart: 100,
			requestedEnd:   200,
			maxLedgerInDB:  50,
			expectedStart:  100,
			shouldProceed:  true,
		},
		{
			name:           "Bounded mode - max equals start, resume from max",
			requestedStart: 50,
			requestedEnd:   100,
			maxLedgerInDB:  50,
			expectedStart:  50,
			shouldProceed:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, proceed := DetermineStartLedger(tt.requestedStart, tt.requestedEnd, tt.maxLedgerInDB)
			assert.Equal(t, tt.shouldProceed, proceed, "shouldProceed mismatch")
			if proceed {
				assert.Equal(t, tt.expectedStart, start, "Start ledger mismatch")
			}
		})
	}
}
