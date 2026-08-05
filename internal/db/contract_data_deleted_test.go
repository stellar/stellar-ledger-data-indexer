package db

import (
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-ledger-data-indexer/internal/contract"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fieldByName looks an UpsertField up by column name so assertions do not depend
// on the ordering of the slice.
func fieldByName(t *testing.T, fields []UpsertField, name string) UpsertField {
	t.Helper()
	for _, f := range fields {
		if f.name == name {
			return f
		}
	}
	t.Fatalf("no upsert field named %q; got %v", name, columnNames(fields))
	return UpsertField{}
}

func columnNames(fields []UpsertField) []string {
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		names = append(names, f.name)
	}
	return names
}

// testOutput builds what TransformContractData would emit for the given change
// type, including the Deleted flag it derives from it.
func testOutput(keyHash string, ledgerSeq uint32, changeType xdr.LedgerEntryChangeType) contract.ContractDataOutput {
	return contract.ContractDataOutput{
		ContractId:         "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
		ContractDurability: "ContractDataDurabilityPersistent",
		LedgerSequence:     ledgerSeq,
		LedgerKeyHash:      keyHash,
		ClosedAt:           time.Unix(1000, 0).UTC(),
		Key:                map[string]string{"type": "Vec", "value": "AAAAEA=="},
		KeyDecoded:         map[string]string{"type": "Vec", "value": "[Balance]"},
		Val:                map[string]string{"type": "B", "value": "AAAAAAAAAAE="},
		ValDecoded:         map[string]string{"type": "B", "value": "true"},
		Deleted:            changeType == xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
		LedgerEntryChange:  uint32(changeType),
	}
}

// TestContractDataUpsertFields_IncludesChangeMetadata is the regression test for
// the reported defect: the transform computed Deleted and LedgerEntryChange for
// every entry and the write layer silently dropped both, because neither column
// was bound.
func TestContractDataUpsertFields_IncludesChangeMetadata(t *testing.T) {
	fields, err := contractDataUpsertFields([]interface{}{
		testOutput("aaaa", 2000, xdr.LedgerEntryChangeTypeLedgerEntryCreated),
		testOutput("bbbb", 2100, xdr.LedgerEntryChangeTypeLedgerEntryRemoved),
	})
	require.NoError(t, err)

	assert.Contains(t, columnNames(fields), "deleted",
		"a removal has nowhere to be recorded unless deleted is bound")
	assert.Contains(t, columnNames(fields), "ledger_entry_change",
		"the change type has nowhere to be recorded unless ledger_entry_change is bound")

	deleted := fieldByName(t, fields, "deleted")
	assert.Equal(t, "boolean", deleted.dbType)
	assert.Equal(t, []interface{}{false, true}, deleted.objects,
		"the Deleted flag must be carried through in record order")

	change := fieldByName(t, fields, "ledger_entry_change")
	assert.Equal(t, "int", change.dbType,
		"typed INTEGER to match the same column across Hubble and stellar-dbt-public")
	assert.Equal(t, []interface{}{uint32(0), uint32(2)}, change.objects,
		"the raw xdr.LedgerEntryChangeType value must be carried through in record order")
}

// TestContractDataUpsertFields_ChangeTypesRoundTrip pins the enum values that can
// actually reach the writer. STATE (3) is absent by design: the SDK folds those
// into the Pre image of the change they accompany.
func TestContractDataUpsertFields_ChangeTypesRoundTrip(t *testing.T) {
	tests := []struct {
		changeType  xdr.LedgerEntryChangeType
		wantValue   uint32
		wantDeleted bool
	}{
		{xdr.LedgerEntryChangeTypeLedgerEntryCreated, 0, false},
		{xdr.LedgerEntryChangeTypeLedgerEntryUpdated, 1, false},
		{xdr.LedgerEntryChangeTypeLedgerEntryRemoved, 2, true},
		{xdr.LedgerEntryChangeTypeLedgerEntryRestored, 4, false},
	}

	for _, tt := range tests {
		t.Run(tt.changeType.String(), func(t *testing.T) {
			fields, err := contractDataUpsertFields([]interface{}{
				testOutput("aaaa", 2000, tt.changeType),
			})
			require.NoError(t, err)

			assert.Equal(t, []interface{}{tt.wantValue},
				fieldByName(t, fields, "ledger_entry_change").objects)
			assert.Equal(t, []interface{}{tt.wantDeleted},
				fieldByName(t, fields, "deleted").objects)
		})
	}
}

// TestContractDataUpsertFields_DeletedMatchesRemovedChangeType documents the
// invariant the two columns hold to. deleted is redundant with
// ledger_entry_change = 2 on purpose: it is the stable flag readers filter on,
// and it should never disagree with the change type.
func TestContractDataUpsertFields_DeletedMatchesRemovedChangeType(t *testing.T) {
	for _, changeType := range []xdr.LedgerEntryChangeType{
		xdr.LedgerEntryChangeTypeLedgerEntryCreated,
		xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
		xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
		xdr.LedgerEntryChangeTypeLedgerEntryRestored,
	} {
		fields, err := contractDataUpsertFields([]interface{}{
			testOutput("aaaa", 2000, changeType),
		})
		require.NoError(t, err)

		isRemoved := fieldByName(t, fields, "ledger_entry_change").objects[0] == uint32(2)
		assert.Equal(t, isRemoved, fieldByName(t, fields, "deleted").objects[0],
			"deleted must equal (ledger_entry_change = 2) for %s", changeType)
	}
}

// TestContractDataUpsertFields_TombstoneKeepsRowIdentity checks that a removal is
// still written as a full row. The tombstone is deliberately not a DELETE:
// MAX(ledger_sequence) doubles as this service's ingestion resume cursor, so the
// row and its sequence have to survive.
func TestContractDataUpsertFields_TombstoneKeepsRowIdentity(t *testing.T) {
	fields, err := contractDataUpsertFields([]interface{}{
		testOutput("bbbb", 2100, xdr.LedgerEntryChangeTypeLedgerEntryRemoved),
	})
	require.NoError(t, err)

	assert.Equal(t, []interface{}{"bbbb"}, fieldByName(t, fields, "key_hash").objects)
	assert.Equal(t, []interface{}{uint32(2100)}, fieldByName(t, fields, "ledger_sequence").objects)
	assert.Equal(t, []interface{}{true}, fieldByName(t, fields, "deleted").objects)
	// The last live value is retained alongside the tombstone rather than blanked,
	// so consumers can still see what the entry held when it was removed.
	assert.Equal(t, []interface{}{[]byte("AAAAAAAAAAE=")}, fieldByName(t, fields, "val").objects)
}

func TestContractDataUpsertFields_AllColumnsPresent(t *testing.T) {
	fields, err := contractDataUpsertFields([]interface{}{
		testOutput("aaaa", 2000, xdr.LedgerEntryChangeTypeLedgerEntryCreated),
	})
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{
		"contract_id", "ledger_sequence", "key_hash", "durability",
		"key_symbol", "key", "val", "closed_at", "deleted", "ledger_entry_change",
	}, columnNames(fields))

	for _, f := range fields {
		assert.Len(t, f.objects, 1, "column %q should have one value per record", f.name)
	}
}

func TestContractDataUpsertFields_RejectsWrongType(t *testing.T) {
	_, err := contractDataUpsertFields([]interface{}{"not a ContractDataOutput"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected ContractDataOutput")
}

func TestContractDataUpsertFields_EmptyBatch(t *testing.T) {
	fields, err := contractDataUpsertFields(nil)
	require.NoError(t, err)
	assert.Contains(t, columnNames(fields), "deleted")
	assert.Contains(t, columnNames(fields), "ledger_entry_change")
	assert.Empty(t, fieldByName(t, fields, "deleted").objects)
	assert.Empty(t, fieldByName(t, fields, "ledger_entry_change").objects)
}
