package transform

import (
	"testing"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/processors/contract"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func TestGetTTLDetails(t *testing.T) {
	type transformTest struct {
		input      []ingest.Change
		wantOutput []contract.TtlOutput
		wantErr    error
	}

	hardCodedInput := makeTtlTestInput()
	hardCodedOutput := makeTtlTestOutput()
	tests := []transformTest{
		{
			[]ingest.Change{
				{
					ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
					Type:       xdr.LedgerEntryTypeOffer,
					Pre:        nil,
					Post: &xdr.LedgerEntry{
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeOffer,
						},
					},
				},
			},
			// Any non contract data (eg: LedgerEntryTypeOffer) is skipped
			[]contract.TtlOutput{}, nil,
		},
	}

	tests = append(tests, transformTest{
		input:      hardCodedInput,
		wantOutput: hardCodedOutput,
		wantErr:    nil,
	})

	for _, test := range tests {
		header := xdr.LedgerHeaderHistoryEntry{
			Header: xdr.LedgerHeader{
				ScpValue: xdr.StellarValue{
					CloseTime: 1000,
				},
				LedgerSeq: 10,
			},
		}
		actualOutput, actualError := GetTTLDataDetails(test.input, header)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeTtlTestInput() []ingest.Change {
	var hash xdr.Hash

	preTtlLedgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 0,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTtl,
			Ttl: &xdr.TtlEntry{
				KeyHash:            hash,
				LiveUntilLedgerSeq: 0,
			},
		},
	}

	TtlLedgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 1,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTtl,
			Ttl: &xdr.TtlEntry{
				KeyHash:            hash,
				LiveUntilLedgerSeq: 123,
			},
		},
	}

	return []ingest.Change{
		{
			ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
			Type:       xdr.LedgerEntryTypeTtl,
			Pre:        &preTtlLedgerEntry,
			Post:       &TtlLedgerEntry,
		},
	}
}

func makeTtlTestOutput() []contract.TtlOutput {
	return []contract.TtlOutput{
		{
			KeyHash:            "0000000000000000000000000000000000000000000000000000000000000000",
			LiveUntilLedgerSeq: 123,
			LastModifiedLedger: 1,
			LedgerEntryChange:  1,
			Deleted:            false,
			LedgerSequence:     10,
			ClosedAt:           time.Date(1970, time.January, 1, 0, 16, 40, 0, time.UTC),
		},
	}
}

// TestGetTTLDetailsWithDuplicates tests the deduplication behavior when multiple changes
// to the same TTL entry occur within a single ledger, preventing regression of issue #25
func TestGetTTLDetailsWithDuplicates(t *testing.T) {
	var hash xdr.Hash
	expectedKeyHash := "0000000000000000000000000000000000000000000000000000000000000000"

	// First change: initial update to LiveUntilLedgerSeq = 100
	preTtlLedgerEntry1 := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 0,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTtl,
			Ttl: &xdr.TtlEntry{
				KeyHash:            hash,
				LiveUntilLedgerSeq: 0,
			},
		},
	}

	postTtlLedgerEntry1 := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 1,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTtl,
			Ttl: &xdr.TtlEntry{
				KeyHash:            hash,
				LiveUntilLedgerSeq: 100,
			},
		},
	}

	// Second change: another update to the same KeyHash, LiveUntilLedgerSeq = 200
	preTtlLedgerEntry2 := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 1,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTtl,
			Ttl: &xdr.TtlEntry{
				KeyHash:            hash,
				LiveUntilLedgerSeq: 100,
			},
		},
	}

	postTtlLedgerEntry2 := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 2,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTtl,
			Ttl: &xdr.TtlEntry{
				KeyHash:            hash,
				LiveUntilLedgerSeq: 200,
			},
		},
	}

	// Third change: final update to the same KeyHash, LiveUntilLedgerSeq = 300
	preTtlLedgerEntry3 := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 2,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTtl,
			Ttl: &xdr.TtlEntry{
				KeyHash:            hash,
				LiveUntilLedgerSeq: 200,
			},
		},
	}

	postTtlLedgerEntry3 := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 3,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTtl,
			Ttl: &xdr.TtlEntry{
				KeyHash:            hash,
				LiveUntilLedgerSeq: 300,
			},
		},
	}

	// Create multiple changes to the same TTL entry within a single ledger
	changes := []ingest.Change{
		{
			ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
			Type:       xdr.LedgerEntryTypeTtl,
			Pre:        &preTtlLedgerEntry1,
			Post:       &postTtlLedgerEntry1,
		},
		{
			ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
			Type:       xdr.LedgerEntryTypeTtl,
			Pre:        &preTtlLedgerEntry2,
			Post:       &postTtlLedgerEntry2,
		},
		{
			ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
			Type:       xdr.LedgerEntryTypeTtl,
			Pre:        &preTtlLedgerEntry3,
			Post:       &postTtlLedgerEntry3,
		},
	}

	header := xdr.LedgerHeaderHistoryEntry{
		Header: xdr.LedgerHeader{
			ScpValue: xdr.StellarValue{
				CloseTime: 1000,
			},
			LedgerSeq: 10,
		},
	}

	actualOutput, actualError := GetTTLDataDetails(changes, header)

	// Should not error
	assert.NoError(t, actualError)

	// Should only have 1 entry after deduplication (the latest one)
	assert.Equal(t, 1, len(actualOutput))

	// The retained entry should be the latest one with LiveUntilLedgerSeq = 300
	expectedOutput := contract.TtlOutput{
		KeyHash:            expectedKeyHash,
		LiveUntilLedgerSeq: 300,
		LastModifiedLedger: 3,
		LedgerEntryChange:  1, // Updated entry
		Deleted:            false,
		LedgerSequence:     10,
		ClosedAt:           time.Date(1970, time.January, 1, 0, 16, 40, 0, time.UTC),
	}

	assert.Equal(t, expectedOutput, actualOutput[0])
}
