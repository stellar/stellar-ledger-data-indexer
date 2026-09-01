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

// contractDataUpsertFields flattens a batch of ContractDataOutput into the
// column-major form UpsertRows expects.
//
// Note that removals are written, not skipped. A removal carries the entry's
// pre-deletion state (see contract.ExtractEntryFromChange) together with
// Deleted == true, and persisting it as a tombstone is what lets a reader tell
// that the entry is gone. Dropping removals here instead would leave the last
// live value in the table looking current forever.
//
// deleted and ledger_entry_change are both bound even though deleted is exactly
// (ledger_entry_change == 2). deleted is the stable semantic flag readers filter
// on; ledger_entry_change is the raw xdr.LedgerEntryChangeType, which also
// separates created (0) from updated (1) from restored (4) among the live rows.
// STATE (3) never appears: the SDK folds those into the Pre image of the change
// they accompany rather than surfacing them.
func contractDataUpsertFields(rawRecords []interface{}) ([]UpsertField, error) {
	var contractId, ledgerSequence, ledgerKeyHash, contractDurability, keySymbol, closedAt, key, val, deleted, ledgerEntryChange []interface{}

	for _, rawRecord := range rawRecords {
		contractData, ok := rawRecord.(contract.ContractDataOutput)
		if !ok {
			return nil, fmt.Errorf("InsertArgs: invalid type passed, expected ContractDataOutput")
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
		deleted = append(deleted, contractData.Deleted)
		ledgerEntryChange = append(ledgerEntryChange, contractData.LedgerEntryChange)
	}

	return []UpsertField{
		{"contract_id", "text", contractId},
		{"ledger_sequence", "int", ledgerSequence},
		{"key_hash", "text", ledgerKeyHash},
		{"durability", "text", contractDurability},
		{"key_symbol", "text", keySymbol},
		{"key", "bytea", key},
		{"val", "bytea", val},
		{"closed_at", "timestamp", closedAt},
		{"deleted", "boolean", deleted},
		{"ledger_entry_change", "int", ledgerEntryChange},
	}, nil
}

func (i *contractDataDBOperator) Upsert(ctx context.Context, data any) error {
	rawRecords := data.([]interface{})

	upsertFields, err := contractDataUpsertFields(rawRecords)
	if err != nil {
		return err
	}

	// A removal always arrives in a later ledger than the entry it removes, so
	// this condition passes and the tombstone is applied. It also means a later
	// LedgerEntryRestored change clears the tombstone, since ON CONFLICT assigns
	// deleted = excluded.deleted.
	upsertConditions := []UpsertCondition{
		{"ledger_sequence", OpGT},
	}
	rowsAffected, err := i.session.UpsertRows(ctx, i.table, "key_hash", upsertFields, upsertConditions)
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
