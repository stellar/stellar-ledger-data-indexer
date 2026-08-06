package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/stellar/go-stellar-sdk/support/db"
	"github.com/stellar/stellar-ledger-data-indexer/internal/contract"
	"github.com/stellar/stellar-ledger-data-indexer/internal/utils"
)

type ContractDataDBOperator interface {
	Upsert(ctx context.Context, data any) error
	TableName() string
	DatasetName() string
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

// maxSymbolLen mirrors SCSYMBOL_LIMIT in the Stellar contract XDR: a Soroban
// Symbol is at most 32 bytes long.
const maxSymbolLen = 32

// ExtractSymbol derives the leading Symbol-shaped discriminant from a decoded
// contract-data key, or "" when the key does not have one.
//
// Keys written by the Stellar Asset Contract and by convention-following
// contracts look like Vec[Symbol("Balance"), Address(...)], and that leading
// symbol is what lab-backend exposes as its filter_key parameter.
//
// The returned value is always safe to bind to a text column: see
// sanitizeKeySymbol for why that matters.
func ExtractSymbol(keyDecoded map[string]string) string {
	if keyDecoded["type"] != "Vec" {
		return ""
	}
	fields := strings.Fields(keyDecoded["value"])
	if len(fields) == 0 {
		return ""
	}
	symbol := strings.TrimLeft(fields[0], "[")
	symbol = strings.TrimRight(symbol, "]")
	return sanitizeKeySymbol(symbol)
}

// sanitizeKeySymbol constrains a value derived from untrusted on-chain data to
// the charset a Soroban Symbol is allowed to use, returning "" for anything
// else.
//
// A contract-data key is an arbitrary ScVal chosen by the contract author. XDR
// declares `typedef string SCString<>` with no charset restriction, and the
// Soroban host does not validate SCString bytes the way it validates SCSymbol,
// so a key may legitimately carry any byte -- including 0x00, which PostgreSQL
// cannot represent in a text column under any encoding. Binding an unsanitized
// value to contract_data.key_symbol therefore lets a single ledger entry fail
// its batch upsert permanently, and because this service derives its resume
// point from MAX(ledger_sequence), a ledger it cannot write is a ledger it can
// never advance past.
//
// Rejecting rather than escaping is deliberate: a value outside this charset is
// not a Symbol, so there is nothing for filter_key to match. No information is
// lost either way, because the complete key is preserved losslessly in the key
// BYTEA column.
func sanitizeKeySymbol(symbol string) string {
	if len(symbol) > maxSymbolLen {
		return ""
	}
	for i := 0; i < len(symbol); i++ {
		c := symbol[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
		case c == '_':
		default:
			return ""
		}
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
	rowsAffected, err := i.session.UpsertRows(ctx, i.table, "key_hash", upsertFields, upsertConditions)
	i.metricRecorder.RecordUpsertCount(i.dataset, rowsAffected)
	return err
}

func (i *contractDataDBOperator) TableName() string {
	return i.table
}

// DatasetName is the logical dataset, which differs from TableName for the TTL
// operator: it enriches rows in table contract_data but is its own dataset.
func (i *contractDataDBOperator) DatasetName() string {
	return i.dataset
}

func (i *contractDataDBOperator) Session() db.SessionInterface {
	return i.session.session
}

func (i *contractDataDBOperator) GetMaxLedgerSequence(ctx context.Context) (uint32, error) {
	return i.session.GetMaxLedgerSequence(ctx, i.table)
}
