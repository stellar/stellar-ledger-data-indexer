package db

import (
	"context"
	"fmt"

	"github.com/stellar/go/processors/contract"
	"github.com/stellar/go/support/db"
)

type TTLDBOperator interface {
	Upsert(ctx context.Context, data any) error
	TableName() string
	Session() db.SessionInterface
	GetMaxLedgerSequence(ctx context.Context) (uint32, error)
}

type ttlDBOperator struct {
	session DBSession
	table   string
}

func NewTTLDBOperator(dbSession DBSession) TTLDBOperator {
	return &ttlDBOperator{session: dbSession, table: "contract_data"}
}

func (i *ttlDBOperator) Upsert(ctx context.Context, data any) error {
	rawRecords := data.([]interface{})

	var keyHash, liveUntilLedgerSequence, ledgerSequence, closedAt []interface{}
	for _, rawRecord := range rawRecords {
		ttlData, ok := rawRecord.(contract.TtlOutput)
		if !ok {
			return fmt.Errorf("InsertArgs: invalid type passed, expected TTLDataOutput")
		}
		keyHash = append(keyHash, ttlData.KeyHash)
		liveUntilLedgerSequence = append(liveUntilLedgerSequence, ttlData.LiveUntilLedgerSeq)
		closedAt = append(closedAt, ttlData.ClosedAt)
		ledgerSequence = append(ledgerSequence, ttlData.LedgerSequence)
	}

	upsertFields := []UpsertField{
		{"key_hash", "text", keyHash},
		{"live_until_ledger_sequence", "int", liveUntilLedgerSequence},
	}

	// upsertConditions := []UpsertCondition{
	// 	{"", "", "(contract_data.live_until_ledger_sequence is null or contract_data.live_until_ledger_sequence < data_source.live_until_ledger_sequence)"},
	// }
	upsertConditions := []UpsertCondition{}
	return i.session.UpdateExistingRows(ctx, i.table, "key_hash", upsertFields, upsertConditions)
}

func (i *ttlDBOperator) TableName() string {
	return i.table
}

func (i *ttlDBOperator) Session() db.SessionInterface {
	return i.session.session
}

func (i *ttlDBOperator) GetMaxLedgerSequence(ctx context.Context) (uint32, error) {
	return i.session.GetMaxLedgerSequence(ctx, i.table)
}
