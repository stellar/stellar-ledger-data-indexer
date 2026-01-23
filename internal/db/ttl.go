package db

import (
	"context"

	"github.com/stellar/go/processors/contract"
	"github.com/stellar/go/support/db"
)

type TTLDataBatchInsertBuilder interface {
	Add(data any) error
	Exec(ctx context.Context) error
	TableName() string
	Close() error
	Reset()
	Session() db.SessionInterface
}

type ttlDataBatchInsertBuilder struct {
	session DBSession
	builder db.FastBatchInsertBuilder
	table   string
}

func (dbSession *DBSession) NewTTLDataBatchInsertBuilder() TTLDataBatchInsertBuilder {
	return &ttlDataBatchInsertBuilder{
		session: DBSession{dbSession},
		builder: db.FastBatchInsertBuilder{},
		table:   "ttl",
	}
}

// Add adds a new ttl data to the batch
func (i *ttlDataBatchInsertBuilder) Add(data any) error {
	return i.UpsertTTL(context.Background(), data)
}

func (i *ttlDataBatchInsertBuilder) UpsertTTL(ctx context.Context, data any) error {
	ttlData, ok := data.(contract.TtlOutput)
	if !ok {
		panic("InsertArgs: invalid type passed, expected TTLDataOutput")
	}

	var keyHash, liveUntilLedgerSequence, ledgerSequence, closedAt interface{}
	keyHash = ttlData.KeyHash
	liveUntilLedgerSequence = ttlData.LiveUntilLedgerSeq
	closedAt = ttlData.ClosedAt
	ledgerSequence = ttlData.LedgerSequence

	upsertFields := []UpsertField{
		{"key_hash", "text", []interface{}{&keyHash}},
		{"live_until_ledger_sequence", "int", []interface{}{&liveUntilLedgerSequence}},
		{"ledger_sequence", "int", []interface{}{&ledgerSequence}},
		{"closed_at", "timestamp", []interface{}{&closedAt}},
	}

	UpsertConditions := []UpsertCondition{
		{"ledger_sequence", OpGT},
	}
	return i.session.UpsertRows(ctx, "ttl", "key_hash", upsertFields, UpsertConditions)
}

// Exec writes the batch of ttl data to the database.
func (i *ttlDataBatchInsertBuilder) Exec(ctx context.Context) error {
	return i.builder.Exec(ctx, i.session, i.table)
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
