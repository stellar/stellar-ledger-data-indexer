package transform

import (
	"context"
	"fmt"
	"io"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/processors/contract"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-ledger-data-indexer/internal/utils"
)

type TTLDataProcessor struct {
	utils.BaseProcessor
}

func getTTLDataDetails(ledgerChangeReader *ingest.LedgerChangeReader, lhe xdr.LedgerHeaderHistoryEntry) ([]contract.TtlOutput, error) {
	ttlDataOutputs := []contract.TtlOutput{}
	for {
		change, err := ledgerChangeReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return ttlDataOutputs, fmt.Errorf("could not read ttl data %w", err)
		}
		if change.Type != xdr.LedgerEntryTypeTtl {
			continue
		}

		TransformTTLData, err := contract.TransformTtl(change, lhe)

		if err != nil {
			return ttlDataOutputs, fmt.Errorf("could not transform ttl data %w", err)
		}

		ttlDataOutputs = append(ttlDataOutputs, TransformTTLData)

	}
	return ttlDataOutputs, nil
}

func (p *TTLDataProcessor) Process(ctx context.Context, msg utils.Message) error {
	ledgerCloseMeta, err := p.ExtractLedgerCloseMeta(msg)
	if err != nil {
		return err
	}

	ttlDataReader, err := p.CreateLCMDataReader(ledgerCloseMeta)
	if err != nil {
		return err
	}

	lhe := ledgerCloseMeta.LedgerHeaderHistoryEntry()
	ttls, err := getTTLDataDetails(ttlDataReader, lhe)
	if err != nil {
		return err
	}

	// p.Logger.Info("Processed %d ttls in ledger sequence %d", len(ttls), lhe.Header.LedgerSeq)
	var data []interface{}
	for _, tx := range ttls {
		data = append(data, tx)
	}
	return p.SendInfo(ctx, data)

}
