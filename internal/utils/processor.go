package utils

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

type Processor interface {
	Process(context.Context, Message) error
}

type BaseProcessor struct {
	OutboundAdapters []OutboundAdapter
	Logger           *log.Entry
}

func (p *BaseProcessor) ExtractLedgerCloseMeta(msg Message) (xdr.LedgerCloseMeta, error) {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("invalid payload type")
	}
	return ledgerCloseMeta, nil
}

func (p *BaseProcessor) SendInfo(ctx context.Context, data interface{}) error {
	for _, adapter := range p.OutboundAdapters {
		err := adapter.Write(ctx, Message{Payload: data})
		if err != nil {
			return fmt.Errorf("error sending data to outbound adapter: %w", err)
		}
	}
	return nil
}

func RemoveFullRowDupes[T any](rows []T) []T {
	seen := make(map[string]bool)
	unique := []T{}

	for _, row := range rows {
		b, _ := json.Marshal(row)
		hash := sha256.Sum256(b)
		key := hex.EncodeToString(hash[:])

		if seen[key] {
			continue
		}
		seen[key] = true
		unique = append(unique, row)
	}
	return unique
}
