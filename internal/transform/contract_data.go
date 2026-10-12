package transform

import (
	"context"
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-ledger-data-indexer/internal/contract"
	"github.com/stellar/stellar-ledger-data-indexer/internal/utils"
)

type ContractDataProcessor struct {
	utils.BaseProcessor
}

// liveUntilByKeyHash returns the live_until of each TTL entry changed in the ledger. Changes
// come in application order, so the last one for a key is the value the ttl dataset writes;
// an entry deleted and recreated within the ledger legitimately ends with a lower TTL.
func liveUntilByKeyHash(changes []ingest.Change, lhe xdr.LedgerHeaderHistoryEntry) (map[string]uint32, error) {
	liveUntil := map[string]uint32{}
	for _, change := range changes {
		if change.Type != xdr.LedgerEntryTypeTtl {
			continue
		}
		ttl, err := contract.TransformTtl(change, lhe)
		if err != nil {
			return nil, fmt.Errorf("could not transform ttl data %w", err)
		}
		liveUntil[ttl.KeyHash] = ttl.LiveUntilLedgerSeq
	}
	return liveUntil, nil
}

func GetContractDataDetails(changes []ingest.Change, lhe xdr.LedgerHeaderHistoryEntry, passPhrase string) ([]contract.ContractDataOutput, error) {
	contractDataOutputs := []contract.ContractDataOutput{}
	// A ledger that changes an entry's data usually changes its TTL too. Carrying the TTL
	// on the contract data row lets the upsert write one row version instead of two.
	liveUntil, err := liveUntilByKeyHash(changes, lhe)
	if err != nil {
		return contractDataOutputs, err
	}
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
		if seq, ok := liveUntil[contractDataOutput.LedgerKeyHash]; ok {
			contractDataOutput.LiveUntilLedgerSeq = &seq
		}

		contractDataOutputs = append(contractDataOutputs, contractDataOutput)

	}
	// It is possible to have multiple changes to the same contract data entry in a single ledger
	// example: CAJJZSGMMM3PD7N33TAPHGBUGTB43OC73HVIK2L2G6BNGGGYOSSYBXBD, ad520948ba9b01c4e202b5f784de5ed57bd56d18a5de485a54db4b752c0cf61d, 59561994
	contractDataOutputs = utils.RemoveDuplicatesByFields(contractDataOutputs, []string{"ContractId", "LedgerKeyHash", "LedgerSequence", "Key"})
	return contractDataOutputs, nil
}

func (p *ContractDataProcessor) Process(ctx context.Context, msg utils.Message) error {
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

	p.MetricRecorder.RecordProcessingLedgerSequence("contract_data", uint32(lhe.Header.LedgerSeq))
	p.Logger.Infof("Processed %d contracts in ledger sequence %d", len(contracts), lhe.Header.LedgerSeq)
	var data []interface{}
	for _, tx := range contracts {
		data = append(data, tx)
	}
	return p.SendInfo(ctx, data)

}
