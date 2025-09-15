package transform

import (
	"context"
	"fmt"
	"io"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	"github.com/stellar/go/processors/contract"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-ledger-data-indexer/internal/utils"
)

type ContractDataProcessor struct {
	utils.BaseProcessor
}

func (p *ContractDataProcessor) createContractDataReader(ledgerCloseMeta xdr.LedgerCloseMeta) (*ingest.LedgerChangeReader, error) {
	return ingest.NewLedgerChangeReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, ledgerCloseMeta)
}

func getContractDataDetails(ledgerChangeReader *ingest.LedgerChangeReader, lhe xdr.LedgerHeaderHistoryEntry) ([]contract.ContractDataOutput, error) {
	contractDataOutputs := []contract.ContractDataOutput{}
	for {
		change, err := ledgerChangeReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return contractDataOutputs, fmt.Errorf("could not read contract data %w", err)
		}
		if change.Type != xdr.LedgerEntryTypeContractData {
			continue
		}

		TransformContractData := contract.NewTransformContractDataStruct(contract.AssetFromContractData, contract.ContractBalanceFromContractData)
		contractDataOutput, err, _ := TransformContractData.TransformContractData(change, network.PublicNetworkPassphrase, lhe)

		if err != nil {
			return contractDataOutputs, fmt.Errorf("could not transform contract data %w", err)
		}

		// Empty contract data that has no error is a nonce. Does not need to be recorded
		if contractDataOutput.ContractId == "" {
			continue
		}

		contractDataOutputs = append(contractDataOutputs, contractDataOutput)

	}
	contractDataOutputs = utils.RemoveFullRowDupes(contractDataOutputs)
	return contractDataOutputs, nil
}

func (p *ContractDataProcessor) Process(ctx context.Context, msg utils.Message) error {
	ledgerCloseMeta, err := p.ExtractLedgerCloseMeta(msg)
	if err != nil {
		return err
	}

	contractDataReader, err := p.createContractDataReader(ledgerCloseMeta)
	if err != nil {
		return err
	}

	lhe := ledgerCloseMeta.LedgerHeaderHistoryEntry()
	contracts, err := getContractDataDetails(contractDataReader, lhe)
	if err != nil {
		return err
	}

	p.Logger.Info("Processed %d contracts in ledger sequence %d", len(contracts), lhe.Header.LedgerSeq)
	var data []interface{}
	for _, tx := range contracts {
		data = append(data, tx)
	}
	return p.SendInfo(ctx, data)

}
