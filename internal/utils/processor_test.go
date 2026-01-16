package utils

import (
	"testing"
	"time"

	"github.com/stellar/go/processors/contract"
)

func TestRemoveDuplicatesByFields(t *testing.T) {
	now := time.Now()

	input := []contract.ContractDataOutput{
		{
			ContractId:          "C1",
			LedgerKeyHash:       "HASH_A",
			LedgerSequence:      100,
			Key:                 map[string]string{"type": "balance"},
			ContractDataBalance: "10",
			ClosedAt:            now,
		},
		{
			// Duplicate PK → should overwrite previous
			ContractId:          "C1",
			LedgerKeyHash:       "HASH_A",
			LedgerSequence:      100,
			Key:                 map[string]string{"type": "balance"},
			ContractDataBalance: "20", // latest value
			ClosedAt:            now.Add(time.Minute),
		},
		{
			// Unique row
			ContractId:          "C2",
			LedgerKeyHash:       "HASH_B",
			LedgerSequence:      200,
			Key:                 map[string]string{"type": "supply"},
			ContractDataBalance: "1000",
			ClosedAt:            now,
		},
	}

	out := RemoveDuplicatesByFields(
		input,
		[]string{"ContractId", "LedgerKeyHash", "LedgerSequence", "Key"},
	)

	if len(out) != 2 {
		t.Fatalf("expected 2 unique rows, got %d", len(out))
	}

	// Validate that the latest duplicate was kept
	foundC1 := false
	foundC2 := false
	for _, row := range out {
		if row.ContractId == "C1" &&
			row.LedgerKeyHash == "HASH_A" &&
			row.LedgerSequence == 100 &&
			len(row.Key) == 1 &&
			row.Key["type"] == "balance" {

			foundC1 = true
			if row.ContractDataBalance != "20" {
				t.Fatalf(
					"expected latest ContractDataBalance '20', got %q",
					row.ContractDataBalance,
				)
			}
		}

		// Validate that the unique row (C2) is present
		if row.ContractId == "C2" &&
			row.LedgerKeyHash == "HASH_B" &&
			row.LedgerSequence == 200 &&
			len(row.Key) == 1 &&
			row.Key["type"] == "supply" {

			foundC2 = true
			if row.ContractDataBalance != "1000" {
				t.Fatalf(
					"expected ContractDataBalance '1000' for C2, got %q",
					row.ContractDataBalance,
				)
			}
		}
	}

	if !foundC1 {
		t.Fatal("expected to find C1 row in output")
	}
	if !foundC2 {
		t.Fatal("expected to find C2 row in output")
	}
}
