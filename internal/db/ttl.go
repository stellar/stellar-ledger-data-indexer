package db

import (
	"context"

	"github.com/stellar/go/processors/contract"
)

type TTLDBSession interface {
	UpsertData(ctx context.Context, data any) error
	TableName() string
	Close() error
}

type ttlDBSession struct {
	session DBSession
	table   string
}

func NewTTLDBSession(dbSession DBSession) TTLDBSession {
	return &ttlDBSession{session: dbSession, table: "ttl"}
}

func (i *ttlDBSession) UpsertData(ctx context.Context, data any) error {
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
	return i.session.UpsertRows(ctx, i.table, "key_hash", upsertFields, UpsertConditions)
}

func (i *ttlDBSession) TableName() string {
	return i.table
}

func (i *ttlDBSession) Close() error {
	return i.session.session.Close()
}
