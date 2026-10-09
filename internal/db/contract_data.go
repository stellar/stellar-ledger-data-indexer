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

// contractDataUpsertConditions is the ON CONFLICT guard deciding whether an
// incoming row may overwrite the one already stored under the same key_hash.
// UpsertRows renders it as
//
//	WHERE excluded.ledger_sequence >= contract_data.ledger_sequence
//
// A removal arrives in a later ledger than the entry it removes, so the guard
// passes and the tombstone is applied; a later LedgerEntryRestored clears it
// again, since ON CONFLICT assigns deleted = excluded.deleted.
//
// Greater-or-equal rather than strictly greater, because a pre-fix removal was
// written as an ordinary upsert and so bumped ledger_sequence to the removal
// ledger. A legacy phantom row already carries the exact ledger a re-index has
// to replay to correct it:
//
//	ledger 2000  created                 -> row at ledger_sequence = 2000
//	ledger 2100  removed on-chain        -> row at ledger_sequence = 2100, deleted NULL
//	replay 2100  removal, deleted = true -> 2100 > 2100 is false, write skipped
//
// Under strictly greater the row that most needs the tombstone is the one the
// guard rejects, so a re-index would run to completion and change nothing.
//
// Admitting the equal case is safe. An older ledger still loses, so neither a
// backfill nor a restart can walk newer state backwards. A same-ledger replay
// rebuilds the row from the same immutable archived metadata, so every column
// is written back identically apart from the ones being corrected. And the
// transform deduplicates to the final change per key per ledger
// (utils.RemoveDuplicatesByFields), with one Write per ledger, so a single
// batch never holds two rows for the same key_hash for this to arbitrate.
func contractDataUpsertConditions() []UpsertCondition {
	return []UpsertCondition{
		{"ledger_sequence", OpGE},
	}
}

func (i *contractDataDBOperator) Upsert(ctx context.Context, data any) error {
	rawRecords := data.([]interface{})

	upsertFields, err := contractDataUpsertFields(rawRecords)
	if err != nil {
		return err
	}

	upsertConditions := contractDataUpsertConditions()
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
