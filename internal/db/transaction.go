package db

import (
	"context"
	"encoding/json"

	"github.com/stellar/go/support/db"
	"github.com/stellar/stellar-ledger-data-indexer/internal/transform"
)

type TransactionBatchInsertBuilder interface {
	Add(data any) error
	Exec(ctx context.Context) error
	Len() int
	TableName() string
	Close() error
	Reset()
	Session() db.SessionInterface
}

type transactionBatchInsertBuilder struct {
	session db.SessionInterface
	builder db.FastBatchInsertBuilder
	table   string
}

func (q *Q) NewTransactionBatchInsertBuilder() TransactionBatchInsertBuilder {
	return &transactionBatchInsertBuilder{
		session: q,
		builder: db.FastBatchInsertBuilder{},
		table:   "transaction",
	}
}

// Add adds a new transaction data to the batch
func (i *transactionBatchInsertBuilder) Add(data any) error {
	transaction, ok := data.(transform.TransactionOutput)
	if !ok {
		panic("InsertArgs: invalid type passed, expected TransactionOutput")
	}

	DiagnosticEventsBytes, _ := json.Marshal(transaction.Events.DiagnosticEvents)
	TransactionEventsBytes, _ := json.Marshal(transaction.Events.TransactionEvents)
	ContractEventsBytes, _ := json.Marshal(transaction.Events.ContractEvents)

	return i.builder.Row(map[string]interface{}{
		"id":                 transaction.TransactionOutput.TransactionID,
		"ledger_sequence":    transaction.TransactionOutput.LedgerSequence,
		"transaction_hash":   transaction.TransactionOutput.TransactionHash,
		"closed_at":          transaction.TransactionOutput.ClosedAt,
		"tx_envelope":        transaction.TransactionOutput.TxEnvelope,
		"tx_result":          transaction.TransactionOutput.TxResult,
		"tx_meta":            transaction.TransactionOutput.TxMeta,
		"tx_fee_meta":        transaction.TransactionOutput.TxFeeMeta,
		"successful":         transaction.TransactionOutput.Successful,
		"diagnostic_events":  DiagnosticEventsBytes,
		"transaction_events": TransactionEventsBytes,
		"contract_events":    ContractEventsBytes,
	})
}

// Exec writes the batch of transaction data to the database.
func (i *transactionBatchInsertBuilder) Exec(ctx context.Context) error {
	return i.builder.Exec(ctx, i.session, i.table)
}

// Len returns the number of elements in the batch
func (i *transactionBatchInsertBuilder) Len() int {
	return i.builder.Len()
}

// TableName returns the name of the table for the batch insert
func (i *transactionBatchInsertBuilder) TableName() string {
	return i.table
}

func (i *transactionBatchInsertBuilder) Close() error {
	return i.session.Close()
}

func (i *transactionBatchInsertBuilder) Reset() {
	i.builder.Reset()
}

func (i *transactionBatchInsertBuilder) Session() db.SessionInterface {
	return i.session
}
