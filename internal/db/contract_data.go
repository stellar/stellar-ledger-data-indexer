package db

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

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

func (dbSession *DBSession) NewContractDataBatchInsertBuilder() ContractDataBatchInsertBuilder {
	return &contractDataBatchInsertBuilder{
		session: dbSession,
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

func GetContractDataRecord(data any) []string {
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
	return []string{
		contractData.ContractId,
		strconv.FormatUint(uint64(contractData.LedgerSequence), 10),
		contractData.LedgerKeyHash,
		contractData.ContractDurability,
		symbol,
		string(KeyBytes),
		string(ValBytes),
		contractData.ClosedAt.Format(time.RFC3339Nano),
	}
}

// Add adds a new contract data to the batch
func (i *contractDataBatchInsertBuilder) Add(data any) error {
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

	return i.builder.Row(map[string]interface{}{
		"contract_id":     contractData.ContractId,
		"ledger_sequence": contractData.LedgerSequence,
		"key_hash":        contractData.LedgerKeyHash,
		"durability":      contractData.ContractDurability,
		"key_symbol":      symbol,
		"key":             KeyBytes,
		"val":             ValBytes,
		"closed_at":       contractData.ClosedAt,
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
