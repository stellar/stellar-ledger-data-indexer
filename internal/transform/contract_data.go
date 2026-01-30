package transform

import (
	"context"
	"fmt"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/processors/contract"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-ledger-data-indexer/internal/utils"
)

type ContractDataProcessor struct {
	utils.BaseProcessor
}

func GetContractDataDetails(changes []ingest.Change, lhe xdr.LedgerHeaderHistoryEntry, passPhrase string) ([]contract.ContractDataOutput, error) {
	contractDataOutputs := []contract.ContractDataOutput{}
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeContractData {
			continue
		}

		TransformContractData := contract.NewTransformContractDataStruct(contract.AssetFromContractData, contract.ContractBalanceFromContractData)
		contractDataOutput, err, _ := TransformContractData.TransformContractData(change, passPhrase, lhe)

		if err != nil {
			return contractDataOutputs, fmt.Errorf("could not transform contract data %w", err)
		}

		// Empty contract data that has no error is a nonce. Does not need to be recorded
		if contractDataOutput.ContractId == "" {
			continue
		}

		contractDataOutputs = append(contractDataOutputs, contractDataOutput)

	}
	// It is possible to have multiple changes to the same contract data entry in a single ledger
	// example: CAJJZSGMMM3PD7N33TAPHGBUGTB43OC73HVIK2L2G6BNGGGYOSSYBXBD, ad520948ba9b01c4e202b5f784de5ed57bd56d18a5de485a54db4b752c0cf61d, 59561994
	contractDataOutputs = utils.RemoveDuplicatesByFields(contractDataOutputs, []string{"ContractId", "LedgerKeyHash", "LedgerSequence", "Key"})
	return contractDataOutputs, nil
}

func (p *ContractDataProcessor) Process(ctx context.Context, msg utils.Message) error {
	latestNetworkLedger, err := datastore.FindLatestLedgerSequence(ctx, p.DataStore)
	p.MetricRecorder.RecordLatestNetworkLedger("contract_data", latestNetworkLedger)

	ledgerCloseMeta, err := p.ExtractLedgerCloseMeta(msg)
	if err != nil {
		return err
	}
	lhe := ledgerCloseMeta.LedgerHeaderHistoryEntry()
	changes, err := p.ReadIngestChanges(ctx, msg)
	if err != nil {
		return err
	}

	contracts, err := GetContractDataDetails(changes, lhe, p.Passphrase)
	if err != nil {
		return err
	}

	p.Logger.Infof("Processed %d contracts in ledger sequence %d", len(contracts), lhe.Header.LedgerSeq)
	var data []interface{}
	for _, tx := range contracts {
		data = append(data, tx)
	}
	return p.SendInfo(ctx, data)

}
