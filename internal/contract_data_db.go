package internal

import (
	"encoding/json"

	"github.com/stellar/go/processors/contract"
)

type ContractDataDBOutput struct {
	contract.ContractDataOutput
}

func NewContractDataDBOutput() (*ContractDataDBOutput, error) {
	output := &ContractDataDBOutput{}
	return output, nil
}
func (t ContractDataDBOutput) TableName() string {
	return "contract_data"
}

func (t ContractDataDBOutput) CreateTableSQL() string {
	return `CREATE TABLE IF NOT EXISTS contract_data (
		id TEXT,
		ledger_sequence INTEGER NOT NULL,
		durability TEXT NOT NULL,
		key TEXT NOT NULL,
		key_decoded TEXT NOT NULL,
		val TEXT NOT NULL,
		val_decoded TEXT NOT NULL,
		closed_at TIMESTAMP WITH TIME ZONE NOT NULL,
		data TEXT NOT NULL
	);`
}

func (t ContractDataDBOutput) CreateIndexSQL() string {
	return `CREATE INDEX IF NOT EXISTS idx_contract_id ON contract_data (id);`
}

func (t ContractDataDBOutput) InsertSQL() string {
	return `INSERT INTO contract_data (id, ledger_sequence, durability, key, key_decoded, val, val_decoded, closed_at, data)
	        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);`
}

func (t ContractDataDBOutput) InsertArgs(contractData any) []any {
	contractOutput, ok := contractData.(contract.ContractDataOutput)
	if !ok {
		panic("InsertArgs: invalid type passed, expected ContractDataOutput")
	}

	KeyBytes, _ := json.Marshal(contractOutput.Key)
	ValBytes, _ := json.Marshal(contractOutput.Val)
	KeyDecodedBytes, _ := json.Marshal(contractOutput.KeyDecoded)
	ValDecodedBytes, _ := json.Marshal(contractOutput.ValDecoded)

	return []any{
		contractOutput.ContractId,
		contractOutput.LedgerSequence,
		contractOutput.ContractDurability,
		KeyBytes,
		KeyDecodedBytes,
		ValBytes,
		ValDecodedBytes,
		contractOutput.ClosedAt,
		contractOutput.ContractDataXDR,
	}
}
