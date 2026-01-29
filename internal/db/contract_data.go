package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/stellar/go/processors/contract"
	"github.com/stellar/go/support/db"
)

type ContractDataDBOperator interface {
	Upsert(ctx context.Context, data any) error
	TableName() string
	Session() db.SessionInterface
	GetMaxLedgerSequence(ctx context.Context) (uint32, error)
}

type contractDataDBOperator struct {
	session DBSession
	table   string
}

func NewContractDataDBOperator(dbSession DBSession) ContractDataDBOperator {
	return &contractDataDBOperator{session: dbSession, table: "contract_data"}
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
	var contractId, ledgerSequence, ledgerKeyHash, contractDurability, keySymbol, closedAt, key, val []interface{}

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
	}
	upsertConditions := []UpsertCondition{
		{"ledger_sequence", OpGT},
	}
	return i.session.UpsertRows(ctx, i.table, "key_hash", upsertFields, upsertConditions)
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
