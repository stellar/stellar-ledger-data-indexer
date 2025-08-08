package db

import (
	"context"
	"encoding/json"

	"github.com/stellar/go/processors/contract"
	"github.com/stellar/go/support/db"
)

type ContractDataBatchInsertBuilder interface {
	Add(data any) error
	Exec(ctx context.Context) error
	Len() int
	TableName() string
	Close() error
	Reset()
	Session() db.SessionInterface
}

type contractDataBatchInsertBuilder struct {
	session db.SessionInterface
	builder db.FastBatchInsertBuilder
	table   string
}

func (q *Q) NewContractDataBatchInsertBuilder() ContractDataBatchInsertBuilder {
	return &contractDataBatchInsertBuilder{
		session: q,
		builder: db.FastBatchInsertBuilder{},
		table:   "contract_data",
	}
}

// Add adds a new contract data to the batch
func (i *contractDataBatchInsertBuilder) Add(data any) error {
	contractData, ok := data.(contract.ContractDataOutput)
	if !ok {
		panic("InsertArgs: invalid type passed, expected ContractDataOutput")
	}

	KeyBytes, _ := json.Marshal(contractData.Key)
	ValBytes, _ := json.Marshal(contractData.Val)
	KeyDecodedBytes, _ := json.Marshal(contractData.KeyDecoded)
	ValDecodedBytes, _ := json.Marshal(contractData.ValDecoded)

	return i.builder.Row(map[string]interface{}{
		"id":              contractData.ContractId,
		"ledger_sequence": contractData.LedgerSequence,
		"durability":      contractData.ContractDurability,
		"key":             KeyBytes,
		"key_decoded":     KeyDecodedBytes,
		"val":             ValBytes,
		"val_decoded":     ValDecodedBytes,
		"closed_at":       contractData.ClosedAt,
		"data":            contractData.ContractDataXDR,
	})
}

// Exec writes the batch of contract data to the database.
func (i *contractDataBatchInsertBuilder) Exec(ctx context.Context) error {
	return i.builder.Exec(ctx, i.session, i.table)
}

// Len returns the number of elements in the batch
func (i *contractDataBatchInsertBuilder) Len() int {
	return i.builder.Len()
}

// TableName returns the name of the table for the batch insert
func (i *contractDataBatchInsertBuilder) TableName() string {
	return i.table
}

func (i *contractDataBatchInsertBuilder) Close() error {
	return i.session.Close()
}

func (i *contractDataBatchInsertBuilder) Reset() {
	i.builder.Reset()
}

func (i *contractDataBatchInsertBuilder) Session() db.SessionInterface {
	return i.session
}
