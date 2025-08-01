package internal

import (
	"encoding/json"
)

type TransactionDBOutput struct {
	TransactionOutput
}

func NewTransactionDBOutput() (*TransactionDBOutput, error) {
	output := &TransactionDBOutput{}
	return output, nil
}
func (t TransactionDBOutput) TableName() string {
	return "transactions"
}

func (t TransactionDBOutput) CreateTableSQL() string {
	return `CREATE TABLE IF NOT EXISTS transactions (
		id TEXT,
		ledger_sequence INTEGER NOT NULL,
		transaction_hash TEXT PRIMARY KEY,
		closed_at TIMESTAMP WITH TIME ZONE NOT NULL,
		tx_envelope TEXT NOT NULL,
		tx_result TEXT NOT NULL,
		tx_meta TEXT NOT NULL,
		tx_fee_meta TEXT NOT NULL,
		successful BOOLEAN NOT NULL,
		diagnostic_events TEXT NOT NULL,
		transaction_events TEXT NOT NULL,
		contract_events TEXT NOT NULL
	);`
}

func (t TransactionDBOutput) CreateIndexSQL() string {
	return `CREATE INDEX IF NOT EXISTS idx_transaction_hash ON transactions (transaction_hash);`
}

func (t TransactionDBOutput) InsertSQL() string {
	return `INSERT INTO transactions (id, ledger_sequence, transaction_hash, closed_at, tx_envelope, tx_result, tx_meta, tx_fee_meta, successful, diagnostic_events, transaction_events, contract_events)
	        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);`
}

func (t TransactionDBOutput) InsertArgs(tx any) []any {
	txOutput, ok := tx.(TransactionOutput)
	if !ok {
		panic("InsertArgs: invalid type passed, expected TransactionOutput")
	}

	DiagnosticEventsBytes, _ := json.Marshal(txOutput.Events.DiagnosticEvents)
	TransactionEventsBytes, _ := json.Marshal(txOutput.Events.TransactionEvents)
	ContractEventsBytes, _ := json.Marshal(txOutput.Events.ContractEvents)

	return []any{
		txOutput.transactionOutput.TransactionID,
		txOutput.transactionOutput.LedgerSequence,
		txOutput.transactionOutput.TransactionHash,
		txOutput.transactionOutput.ClosedAt,
		txOutput.transactionOutput.TxEnvelope,
		txOutput.transactionOutput.TxResult,
		txOutput.transactionOutput.TxMeta,
		txOutput.transactionOutput.TxFeeMeta,
		txOutput.transactionOutput.Successful,
		DiagnosticEventsBytes,
		TransactionEventsBytes,
		ContractEventsBytes,
	}
}
