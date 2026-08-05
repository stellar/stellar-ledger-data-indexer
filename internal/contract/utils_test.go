package contract

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExtractEntryFromChange documents the SDK contract this package relies on:
// a removed entry carries its state in Pre with Post nil, so a removal is read
// from the pre-deletion image and reported with deleted == true. The write layer
// must persist that flag; see contractDataUpsertFields.
func TestExtractEntryFromChange(t *testing.T) {
	preEntry := xdr.LedgerEntry{LastModifiedLedgerSeq: 100}
	postEntry := xdr.LedgerEntry{LastModifiedLedgerSeq: 200}

	tests := []struct {
		name        string
		change      ingest.Change
		wantSeq     xdr.Uint32
		wantDeleted bool
	}{
		{
			name: "created reads Post and is not deleted",
			change: ingest.Change{
				ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
				Post:       &postEntry,
			},
			wantSeq:     200,
			wantDeleted: false,
		},
		{
			name: "updated reads Post and is not deleted",
			change: ingest.Change{
				ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
				Pre:        &preEntry,
				Post:       &postEntry,
			},
			wantSeq:     200,
			wantDeleted: false,
		},
		{
			name: "restored reads Post and clears deleted",
			change: ingest.Change{
				ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryRestored,
				Post:       &postEntry,
			},
			wantSeq:     200,
			wantDeleted: false,
		},
		{
			name: "removed reads Pre and is deleted",
			change: ingest.Change{
				ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
				Pre:        &preEntry,
			},
			wantSeq:     100,
			wantDeleted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry, changeType, deleted, err := ExtractEntryFromChange(tt.change)
			require.NoError(t, err)
			assert.Equal(t, tt.change.ChangeType, changeType)
			assert.Equal(t, tt.wantSeq, entry.LastModifiedLedgerSeq)
			assert.Equal(t, tt.wantDeleted, deleted)
		})
	}
}

func TestExtractEntryFromChange_UnknownChangeType(t *testing.T) {
	_, _, deleted, err := ExtractEntryFromChange(ingest.Change{
		ChangeType: xdr.LedgerEntryChangeType(99),
	})
	require.Error(t, err)
	assert.False(t, deleted)
}
