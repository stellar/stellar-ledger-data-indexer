package transform

import (
	"context"
	"fmt"
	"io"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	"github.com/stellar/go/processors/transaction"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-ledger-data-indexer/internal/utils"
)

type Events struct {
	DiagnosticEvents  []string
	TransactionEvents []string
	ContractEvents    [][]string
}

type TransactionOutput struct {
	TransactionOutput transaction.TransactionOutput
	Events            Events
}

type TransactionProcessor struct {
	utils.BaseProcessor
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
			TransactionOutput: transactionOutput,
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

func (p *TransactionProcessor) createTransactionReader(ledgerCloseMeta xdr.LedgerCloseMeta) (*ingest.LedgerTransactionReader, error) {
	return ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, ledgerCloseMeta)
}

func (p *TransactionProcessor) Process(ctx context.Context, msg utils.Message) error {
	ledgerCloseMeta, err := p.ExtractLedgerCloseMeta(msg)
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

	var data []interface{}
	for _, tx := range transactions {
		data = append(data, tx)
	}

	fmt.Printf("%s Ledger: %s Transaction Hash: %s ClosedAt \n", transactions[0].TransactionOutput.LedgerSequence, transactions[0].TransactionOutput.TransactionHash, transactions[0].TransactionOutput.ClosedAt)
	return p.SendInfo(ctx, data)
}
