package db

import (
	"context"

	"github.com/stellar/go/processors/contract"
	"github.com/stellar/go/support/db"
)

type TTLDataBatchInsertBuilder interface {
	Add(data any) error
	Exec(ctx context.Context) error
	Len() int
	TableName() string
	Close() error
	Reset()
	Session() db.SessionInterface
}

type ttlDataBatchInsertBuilder struct {
	session db.SessionInterface
	builder db.FastBatchInsertBuilder
	table   string
}

func (q *Q) NewTTLDataBatchInsertBuilder() TTLDataBatchInsertBuilder {
	return &ttlDataBatchInsertBuilder{
		session: q,
		builder: db.FastBatchInsertBuilder{},
		table:   "ttl",
	}
}

// Add adds a new ttl data to the batch
func (i *ttlDataBatchInsertBuilder) Add(data any) error {
	ttlData, ok := data.(contract.TtlOutput)
	if !ok {
		panic("InsertArgs: invalid type passed, expected TTLDataOutput")
	}

	return i.builder.Row(map[string]interface{}{
		"key_hash":                   ttlData.KeyHash,
		"live_until_ledger_sequence": ttlData.LiveUntilLedgerSeq,
		"ledger_sequence":            ttlData.LedgerSequence,
		"closed_at":                  ttlData.ClosedAt,
	})
}

// Exec writes the batch of ttl data to the database.
func (i *ttlDataBatchInsertBuilder) Exec(ctx context.Context) error {
	return i.builder.Exec(ctx, i.session, i.table)
}

// Len returns the number of elements in the batch
func (i *ttlDataBatchInsertBuilder) Len() int {
	return i.builder.Len()
}

// TableName returns the name of the table for the batch insert
func (i *ttlDataBatchInsertBuilder) TableName() string {
	return i.table
}

func (i *ttlDataBatchInsertBuilder) Close() error {
	return i.session.Close()
}

func (i *ttlDataBatchInsertBuilder) Reset() {
	i.builder.Reset()
}

func (i *ttlDataBatchInsertBuilder) Session() db.SessionInterface {
	return i.session
}
