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
			ContractId:          "CAJJZSGMMM3PD7N33TAPHGBUGTB43OC73HVIK2L2G6BNGGGYOSSYBXBD",
			LedgerKeyHash:       "ad520948ba9b01c4e202b5f784de5ed57bd56d18a5de485a54db4b752c0cf61d",
			LedgerSequence:      59561994,
			Key:                 map[string]string{"type": "balance"},
			ContractDataBalance: "10",
			ClosedAt:            now,
		},
		{
			// Duplicate PK → should overwrite previous
			ContractId:          "CAJJZSGMMM3PD7N33TAPHGBUGTB43OC73HVIK2L2G6BNGGGYOSSYBXBD",
			LedgerKeyHash:       "ad520948ba9b01c4e202b5f784de5ed57bd56d18a5de485a54db4b752c0cf61d",
			LedgerSequence:      59561994,
			Key:                 map[string]string{"type": "balance"},
			ContractDataBalance: "20", // latest value
			ClosedAt:            now.Add(time.Minute),
		},
		{
			// Unique row
			ContractId:          "CDL74RF5BLYR2YBLCCI7F5FB6TPSCLKEJUBSD2RSVWZ4YHF3VMFAIGWA",
			LedgerKeyHash:       "1703ffd608a8566e7d89a503b501564d241c0e8aec25cdf7e272bc3f01a225f1",
			LedgerSequence:      59561994,
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
		if row.ContractId == "CAJJZSGMMM3PD7N33TAPHGBUGTB43OC73HVIK2L2G6BNGGGYOSSYBXBD" &&
			row.LedgerKeyHash == "ad520948ba9b01c4e202b5f784de5ed57bd56d18a5de485a54db4b752c0cf61d" &&
			row.LedgerSequence == 59561994 &&
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
		if row.ContractId == "CDL74RF5BLYR2YBLCCI7F5FB6TPSCLKEJUBSD2RSVWZ4YHF3VMFAIGWA" &&
			row.LedgerKeyHash == "1703ffd608a8566e7d89a503b501564d241c0e8aec25cdf7e272bc3f01a225f1" &&
			row.LedgerSequence == 59561994 &&
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
