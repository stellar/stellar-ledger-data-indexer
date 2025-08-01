package internal

import (
	"context"
	"fmt"
	"io"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	"github.com/stellar/go/processors/transaction"
	"github.com/stellar/go/xdr"
)

type Processor interface {
	Process(context.Context, Message) error
}

type processor struct {
	outboundAdapters []OutboundAdapter
}

type Events struct {
	DiagnosticEvents  []string
	TransactionEvents []string
	ContractEvents    [][]string
}

type TransactionOutput struct {
	transactionOutput transaction.TransactionOutput
	Events            Events
}

func getTransactionDetails(transactionReader *ingest.LedgerTransactionReader, lhe xdr.LedgerHeaderHistoryEntry) ([]TransactionOutput, error) {
	transactions := []TransactionOutput{}
	for {
		tx, err := transactionReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return transactions, fmt.Errorf("could not read transaction %w", err)
		}

		transactionOutput, err := transaction.TransformTransaction(tx, lhe)
		results := TransactionOutput{
			transactionOutput: transactionOutput,
			Events:            Events{DiagnosticEvents: make([]string, 0), TransactionEvents: make([]string, 0), ContractEvents: make([][]string, 0)},
		}
		if err != nil {
			return transactions, fmt.Errorf("could not transform transaction %w", err)
		}

		allEvents, err := tx.GetTransactionEvents()
		if err != nil {
			return transactions, fmt.Errorf("couldn't encode transaction Events: %w", err)
		}

		diagEvents, err := tx.GetDiagnosticEvents()
		if err != nil {
			return transactions, fmt.Errorf("couldn't encode diagnostic Events: %w", err)
		}

		if err = parseEvents(&results, allEvents, diagEvents); err != nil {
			return transactions, err
		}
		transactions = append(transactions, results)

	}
	return transactions, nil
}

// parseEvents parses diagnostic, transaction and contract events
func parseEvents(tx *TransactionOutput, allEvents ingest.TransactionEvents, diagnosticEvents []xdr.DiagnosticEvent) error {
	// Encode TransactionEvents
	tx.Events.TransactionEvents = make([]string, 0, len(allEvents.TransactionEvents))
	for i, event := range allEvents.TransactionEvents {
		bytes, err := xdr.MarshalBase64(event)
		if err != nil {
			return fmt.Errorf("couldn't encode TransactionEvent %d: %w", i, err)
		}
		tx.Events.TransactionEvents = append(tx.Events.TransactionEvents, string(bytes))
	}

	// Encode ContractEvents (slice of slices)
	tx.Events.ContractEvents = make([][]string, 0, len(allEvents.OperationEvents))
	for opIndex, opEvents := range allEvents.OperationEvents {
		events := make([]string, 0, len(opEvents))
		for i, event := range opEvents {
			bytes, err := xdr.MarshalBase64(event)
			if err != nil {
				return fmt.Errorf("couldn't encode ContractEvent %d for operation %d: %w", i, opIndex, err)
			}
			events = append(events, string(bytes))
		}
		tx.Events.ContractEvents = append(tx.Events.ContractEvents, events)
	}

	// Encode DiagnosticEvents
	tx.Events.DiagnosticEvents = make([]string, 0, len(diagnosticEvents))
	for i, event := range diagnosticEvents {
		bytes, err := xdr.MarshalBase64(event)
		if err != nil {
			return fmt.Errorf("couldn't encode DiagnosticEvent %d: %w", i, err)
		}
		tx.Events.DiagnosticEvents = append(tx.Events.DiagnosticEvents, string(bytes))
	}

	return nil
}

func (p *processor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, err := p.extractLedgerCloseMeta(msg)
	if err != nil {
		return err
	}

	transactionReader, err := p.createTransactionReader(ledgerCloseMeta)
	if err != nil {
		return err
	}
	lhe := ledgerCloseMeta.LedgerHeaderHistoryEntry()
	transactions, err := getTransactionDetails(transactionReader, lhe)
	if err != nil {
		return err
	}

	fmt.Println("Transactions count", len(transactions))
	fmt.Printf("%s Ledger: %s Transaction Hash: %s ClosedAt \n", transactions[0].transactionOutput.LedgerSequence, transactions[0].transactionOutput.TransactionHash, transactions[0].transactionOutput.ClosedAt)
	return p.sendTransactionInfo(ctx, transactions)
}

func (p *processor) extractLedgerCloseMeta(msg Message) (xdr.LedgerCloseMeta, error) {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("invalid payload type")
	}
	return ledgerCloseMeta, nil
}

func (p *processor) createTransactionReader(ledgerCloseMeta xdr.LedgerCloseMeta) (*ingest.LedgerTransactionReader, error) {
	return ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, ledgerCloseMeta)
}

// sendTransactionInfo marshals and sends the transaction information.
func (p *processor) sendTransactionInfo(ctx context.Context, transactions []TransactionOutput) error {
	for _, adapter := range p.outboundAdapters {
		for _, transaction := range transactions {
			fmt.Println("Sending Transaction info to outbound adapter:", transaction.transactionOutput.TransactionHash, transaction.transactionOutput.LedgerSequence, transaction.transactionOutput.ClosedAt)
			err := adapter.Write(ctx, Message{Payload: transaction})
			if err != nil {
				fmt.Println("Error sending Transaction info to outbound adapter:", err)
				//	return err
			}
		}
	}
	return nil
}
