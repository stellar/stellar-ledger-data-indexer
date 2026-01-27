package transform

import (
	"testing"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/processors/contract"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
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
