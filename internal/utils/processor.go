package utils

import (
	"context"
	"fmt"

	"github.com/stellar/go/xdr"
)

type Processor interface {
	Process(context.Context, Message) error
}

type BaseProcessor struct {
	OutboundAdapters []OutboundAdapter
}

func (p *BaseProcessor) ExtractLedgerCloseMeta(msg Message) (xdr.LedgerCloseMeta, error) {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("invalid payload type")
	}
	return ledgerCloseMeta, nil
}

// SendInfo marshals and sends the transaction information.
func (p *BaseProcessor) SendInfo(ctx context.Context, transactions []interface{}) error {
	for _, adapter := range p.OutboundAdapters {
		for _, transaction := range transactions {
			err := adapter.Write(ctx, Message{Payload: transaction})
			if err != nil {
				fmt.Println("Error sending Transaction info to outbound adapter:", err)
				//	return err
			}
		}
	}
	return nil
}
