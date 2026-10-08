package transform

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-ledger-data-indexer/internal/contract"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetContractDataDetails(t *testing.T) {
	type transformTest struct {
		input      []ingest.Change
		passphrase string
		wantOutput []contract.ContractDataOutput
		wantErr    error
	}

	hardCodedInput := makeContractDataTestInput()
	hardCodedOutput := makeContractDataTestOutput()
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
			"Any non contract data (eg: LedgerEntryTypeOffer) is skipped",
			[]contract.ContractDataOutput{}, nil,
		},
	}

	tests = append(tests, transformTest{
		input:      hardCodedInput,
		passphrase: "unit test",
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
		actualOutput, actualError := GetContractDataDetails(test.input, header, test.passphrase)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func TestGetContractDataDetailsCarriesSameLedgerTTL(t *testing.T) {
	header := xdr.LedgerHeaderHistoryEntry{
		Header: xdr.LedgerHeader{
			ScpValue:  xdr.StellarValue{CloseTime: 1000},
			LedgerSeq: 10,
		},
	}
	ttlEntry := func(keyHashHex string, liveUntil uint32) *xdr.LedgerEntry {
		raw, err := hex.DecodeString(keyHashHex)
		require.NoError(t, err)
		var keyHash xdr.Hash
		copy(keyHash[:], raw)
		return &xdr.LedgerEntry{
			Data: xdr.LedgerEntryData{
				Type: xdr.LedgerEntryTypeTtl,
				Ttl:  &xdr.TtlEntry{KeyHash: keyHash, LiveUntilLedgerSeq: xdr.Uint32(liveUntil)},
			},
		}
	}
	ttlChange := func(keyHashHex string, liveUntil uint32) ingest.Change {
		return ingest.Change{
			ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
			Type:       xdr.LedgerEntryTypeTtl,
			Pre:        &xdr.LedgerEntry{},
			Post:       ttlEntry(keyHashHex, liveUntil),
		}
	}
	ttlRemoved := func(keyHashHex string, liveUntil uint32) ingest.Change {
		return ingest.Change{
			ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
			Type:       xdr.LedgerEntryTypeTtl,
			Pre:        ttlEntry(keyHashHex, liveUntil),
		}
	}
	entryKeyHash := makeContractDataTestOutput()[0].LedgerKeyHash
	otherKeyHash := strings.Repeat("ff", 32)

	t.Run("last TTL change of the same key in the ledger", func(t *testing.T) {
		changes := append(makeContractDataTestInput(),
			ttlChange(entryKeyHash, 100),
			ttlChange(otherKeyHash, 999),
			ttlChange(entryKeyHash, 300),
		)
		out, err := GetContractDataDetails(changes, header, "unit test")
		require.NoError(t, err)
		require.Len(t, out, 1)
		require.NotNil(t, out[0].LiveUntilLedgerSeq)
		assert.Equal(t, uint32(300), *out[0].LiveUntilLedgerSeq)
	})

	t.Run("key deleted and recreated with a shorter TTL in the ledger", func(t *testing.T) {
		changes := append(makeContractDataTestInput(),
			ttlChange(entryKeyHash, 10000),
			ttlRemoved(entryKeyHash, 10000),
			ttlChange(entryKeyHash, 6000),
		)
		out, err := GetContractDataDetails(changes, header, "unit test")
		require.NoError(t, err)
		require.Len(t, out, 1)
		require.NotNil(t, out[0].LiveUntilLedgerSeq)
		assert.Equal(t, uint32(6000), *out[0].LiveUntilLedgerSeq)
	})

	t.Run("key only deleted in the ledger keeps its last TTL, as the ttl dataset does", func(t *testing.T) {
		changes := append(makeContractDataTestInput(), ttlRemoved(entryKeyHash, 10000))
		out, err := GetContractDataDetails(changes, header, "unit test")
		require.NoError(t, err)
		require.Len(t, out, 1)
		require.NotNil(t, out[0].LiveUntilLedgerSeq)
		assert.Equal(t, uint32(10000), *out[0].LiveUntilLedgerSeq)
	})

	t.Run("no TTL change for the key", func(t *testing.T) {
		changes := append(makeContractDataTestInput(), ttlChange(otherKeyHash, 999))
		out, err := GetContractDataDetails(changes, header, "unit test")
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Nil(t, out[0].LiveUntilLedgerSeq)
	})
}

func makeContractDataTestInput() []ingest.Change {
	var contractID xdr.ContractId
	var hash xdr.Hash
	var scStr xdr.ScString = "a"
	var testVal = true

	contractDataLedgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 24229503,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.ContractDataEntry{
				Contract: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &contractID,
				},
				Key: xdr.ScVal{
					Type: xdr.ScValTypeScvContractInstance,
					Instance: &xdr.ScContractInstance{
						Executable: xdr.ContractExecutable{
							Type:     xdr.ContractExecutableTypeContractExecutableWasm,
							WasmHash: &hash,
						},
						Storage: &xdr.ScMap{
							xdr.ScMapEntry{
								Key: xdr.ScVal{
									Type: xdr.ScValTypeScvString,
									Str:  &scStr,
								},
								Val: xdr.ScVal{
									Type: xdr.ScValTypeScvString,
									Str:  &scStr,
								},
							},
						},
					},
				},
				Durability: xdr.ContractDataDurabilityPersistent,
				Val: xdr.ScVal{
					Type: xdr.ScValTypeScvBool,
					B:    &testVal,
				},
			},
		},
	}

	return []ingest.Change{
		{
			ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
			Type:       xdr.LedgerEntryTypeContractData,
			Pre:        &xdr.LedgerEntry{},
			Post:       &contractDataLedgerEntry,
		},
	}
}

func makeContractDataTestOutput() []contract.ContractDataOutput {
	key := map[string]string{
		"type":  "Instance",
		"value": "AAAAEwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAABAAAADgAAAAFhAAAAAAAADgAAAAFhAAAA",
	}

	keyDecoded := map[string]string{
		"type":  "Instance",
		"value": "0000000000000000000000000000000000000000000000000000000000000000: [{a a}]",
	}

	val := map[string]string{
		"type":  "B",
		"value": "AAAAAAAAAAE=",
	}

	valDecoded := map[string]string{
		"type":  "B",
		"value": "true",
	}

	return []contract.ContractDataOutput{
		{
			ContractId:                "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
			ContractKeyType:           "ScValTypeScvContractInstance",
			ContractDurability:        "ContractDataDurabilityPersistent",
			ContractDataAssetCode:     "",
			ContractDataAssetIssuer:   "",
			ContractDataAssetType:     "",
			ContractDataBalanceHolder: "",
			ContractDataBalance:       "",
			LastModifiedLedger:        24229503,
			LedgerEntryChange:         1,
			Deleted:                   false,
			LedgerSequence:            10,
			ClosedAt:                  time.Date(1970, time.January, 1, 0, 16, 40, 0, time.UTC),
			LedgerKeyHash:             "abfc33272095a9df4c310cff189040192a8aee6f6a23b6b462889114d80728ca",
			Key:                       key,
			KeyDecoded:                keyDecoded,
			Val:                       val,
			ValDecoded:                valDecoded,
			ContractDataXDR:           "AAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAQAAAA4AAAABYQAAAAAAAA4AAAABYQAAAAAAAAEAAAAAAAAAAQ==",
		},
	}
}
