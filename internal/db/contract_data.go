package db

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/stellar/go/processors/contract"
	"github.com/stellar/go/support/db"
)

type ContractDataBatchInsertBuilder interface {
	Add(data any) error
	Exec(ctx context.Context) error
	TableName() string
	Close() error
	Reset()
	Session() db.SessionInterface
}

type contractDataBatchInsertBuilder struct {
	session DBSession
	builder db.FastBatchInsertBuilder
	table   string
}

func (dbSession *DBSession) NewContractDataBatchInsertBuilder() ContractDataBatchInsertBuilder {
	return &contractDataBatchInsertBuilder{
		session: DBSession{dbSession},
		builder: db.FastBatchInsertBuilder{},
		table:   "contract_data",
	}
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

// Add adds a new contract data to the batch
func (i *contractDataBatchInsertBuilder) Add(data any) error {
	return i.UpsertContractData(context.Background(), data)
}

func (i *contractDataBatchInsertBuilder) UpsertContractData(ctx context.Context, data any) error {
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
	return i.session.UpsertRows(ctx, "contract_data", "key_hash", upsertFields, UpsertConditions)
}

// Exec writes the batch of contract data to the database.
func (i *contractDataBatchInsertBuilder) Exec(ctx context.Context) error {
	return i.builder.Exec(ctx, i.session, i.table)
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
