package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/stellar/go-stellar-sdk/support/db"
	"github.com/stellar/stellar-ledger-data-indexer/internal/contract"
	"github.com/stellar/stellar-ledger-data-indexer/internal/utils"
)

type ContractDataDBOperator interface {
	Upsert(ctx context.Context, data any) error
	TableName() string
	Session() db.SessionInterface
	GetMaxLedgerSequence(ctx context.Context) (uint32, error)
}

type contractDataDBOperator struct {
	session        DBSession
	table          string
	dataset        string
	metricRecorder utils.MetricRecorder
}

func NewContractDataDBOperator(dbSession DBSession, metricRecorder utils.MetricRecorder) ContractDataDBOperator {
	return &contractDataDBOperator{session: dbSession, table: "contract_data", dataset: "contract_data", metricRecorder: metricRecorder}
}

func ExtractSymbol(keyDecoded map[string]string) string {
	KeyDecodedBytes, _ := json.Marshal(keyDecoded)
	var obj struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(KeyDecodedBytes, &obj); err != nil {
		panic(err)
	}
	fields := strings.Fields(obj.Value)
	symbol := ""
	if len(fields) != 0 && obj.Type == "Vec" {
		symbol = strings.TrimLeft(fields[0], "[")
		symbol = strings.TrimRight(symbol, "]")
	}
	return symbol
}

func (i *contractDataDBOperator) Upsert(ctx context.Context, data any) error {
	rawRecords := data.([]interface{})
	var contractId, ledgerSequence, ledgerKeyHash, contractDurability, keySymbol, closedAt, key, val, liveUntil []interface{}

	for _, rawRecord := range rawRecords {
		contractData, ok := rawRecord.(contract.ContractDataOutput)
		if !ok {
			return fmt.Errorf("InsertArgs: invalid type passed, expected ContractDataOutput")
		}
		keyBytes := []byte(contractData.Key["value"])
		valBytes := []byte(contractData.Val["value"])

		symbol := ExtractSymbol(contractData.KeyDecoded)

		if contractData.ContractDurability == "ContractDataDurabilityPersistent" {
			contractData.ContractDurability = "persistent"
		} else {
			contractData.ContractDurability = "temporary"
		}
		contractId = append(contractId, contractData.ContractId)
		ledgerSequence = append(ledgerSequence, contractData.LedgerSequence)
		ledgerKeyHash = append(ledgerKeyHash, contractData.LedgerKeyHash)
		contractDurability = append(contractDurability, contractData.ContractDurability)
		keySymbol = append(keySymbol, symbol)
		closedAt = append(closedAt, contractData.ClosedAt)
		key = append(key, keyBytes)
		val = append(val, valBytes)
		if contractData.LiveUntilLedgerSeq != nil {
			liveUntil = append(liveUntil, *contractData.LiveUntilLedgerSeq)
		} else {
			liveUntil = append(liveUntil, nil)
		}
	}

	upsertFields := []UpsertField{
		{"contract_id", "text", contractId},
		{"ledger_sequence", "int", ledgerSequence},
		{"key_hash", "text", ledgerKeyHash},
		{"durability", "text", contractDurability},
		{"key_symbol", "text", keySymbol},
		{"key", "bytea", key},
		{"val", "bytea", val},
		{"closed_at", "timestamp", closedAt},
		{"live_until_ledger_sequence", "int", liveUntil},
	}
	// A NULL means this ledger did not change the key's TTL, so the stored value stays.
	// Otherwise the carried value is the key's TTL after this ledger; it can be lower than
	// the stored one when an entry was deleted and recreated, and the ledger_sequence
	// condition below already rejects replays of older ledgers.
	upsertSetExprs := []UpsertSetExpr{
		{"live_until_ledger_sequence", fmt.Sprintf("COALESCE(excluded.live_until_ledger_sequence, %s.live_until_ledger_sequence)", i.table)},
	}
	upsertConditions := []UpsertCondition{
		{"ledger_sequence", OpGT},
	}
	rowsAffected, err := i.session.UpsertRows(ctx, i.table, "key_hash", upsertFields, upsertSetExprs, upsertConditions)
	i.metricRecorder.RecordUpsertCount(i.dataset, rowsAffected)
	return err
}

func (i *contractDataDBOperator) TableName() string {
	return i.table
}

func (i *contractDataDBOperator) Session() db.SessionInterface {
	return i.session.session
}

func (i *contractDataDBOperator) GetMaxLedgerSequence(ctx context.Context) (uint32, error) {
	return i.session.GetMaxLedgerSequence(ctx, i.table)
}
