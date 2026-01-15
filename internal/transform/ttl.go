package transform

import (
	"context"
	"fmt"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/processors/contract"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-ledger-data-indexer/internal/utils"
)

type TTLDataProcessor struct {
	utils.BaseProcessor
}

func getTTLDataDetails(changes []ingest.Change, lhe xdr.LedgerHeaderHistoryEntry) ([]contract.TtlOutput, error) {
	ttlDataOutputs := []contract.TtlOutput{}
	for _, change := range changes {
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
	lhe := ledgerCloseMeta.LedgerHeaderHistoryEntry()
	changes, err := p.ReadIngestChanges(ctx, msg)
	if err != nil {
		return err
	}
	ttls, err := getTTLDataDetails(changes, lhe)
	if err != nil {
		return err
	}

	p.Logger.Infof("Processed %d ttls in ledger sequence %d", len(ttls), lhe.Header.LedgerSeq)
	var data []interface{}
	for _, tx := range ttls {
		data = append(data, tx)
	}
	return p.SendInfo(ctx, data)

}
