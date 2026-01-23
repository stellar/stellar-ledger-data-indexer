package db

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/stellar/go/processors/contract"
)

type ContractDataDBSession interface {
	UpsertData(ctx context.Context, data any) error
	TableName() string
	Close() error
}

type contractDataDBSession struct {
	session DBSession
	table   string
}

func NewContractDataDBSession(dbSession DBSession) ContractDataDBSession {
	return &contractDataDBSession{session: dbSession, table: "contract_data"}
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

func (i *contractDataDBSession) UpsertData(ctx context.Context, data any) error {
	contractData, ok := data.(contract.ContractDataOutput)
	if !ok {
		panic("InsertArgs: invalid type passed, expected ContractDataOutput")
	}

	KeyBytes := []byte(contractData.Key["value"])
	ValBytes := []byte(contractData.Val["value"])

	symbol := ExtractSymbol(contractData.KeyDecoded)

	if contractData.ContractDurability == "ContractDataDurabilityPersistent" {
		contractData.ContractDurability = "persistent"
	} else {
		contractData.ContractDurability = "temporary"
	}
	var contractId, ledgerSequence, ledgerKeyHash, contractDurability, keySymbol, closedAt interface{}
	contractId = contractData.ContractId
	ledgerSequence = contractData.LedgerSequence
	ledgerKeyHash = contractData.LedgerKeyHash
	contractDurability = contractData.ContractDurability
	keySymbol = symbol
	closedAt = contractData.ClosedAt

	upsertFields := []UpsertField{
		{"contract_id", "text", []interface{}{&contractId}},
		{"ledger_sequence", "int", []interface{}{&ledgerSequence}},
		{"key_hash", "text", []interface{}{&ledgerKeyHash}},
		{"durability", "text", []interface{}{&contractDurability}},
		{"key_symbol", "text", []interface{}{&keySymbol}},
		{"key", "bytea", []interface{}{&KeyBytes}},
		{"val", "bytea", []interface{}{&ValBytes}},
		{"closed_at", "timestamp", []interface{}{&closedAt}},
	}
	UpsertConditions := []UpsertCondition{
		{"ledger_sequence", OpGT},
	}
	return i.session.UpsertRows(ctx, i.table, "key_hash", upsertFields, UpsertConditions)
}

func (i *contractDataDBSession) TableName() string {
	return i.table
}

func (i *contractDataDBSession) Close() error {
	return i.session.session.Close()
}
