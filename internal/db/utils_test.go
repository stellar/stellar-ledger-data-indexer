package db

import (
	"strings"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpsertRowsSQL(t *testing.T) {
	fields := []UpsertField{
		{"key_hash", "text", []interface{}{"a", "b"}},
		{"ledger_sequence", "int", []interface{}{uint32(1), uint32(2)}},
		{"live_until_ledger_sequence", "int", []interface{}{uint32(10), nil}},
	}
	setExprs := []UpsertSetExpr{
		{"live_until_ledger_sequence", "COALESCE(excluded.live_until_ledger_sequence, contract_data.live_until_ledger_sequence)"},
	}
	conditions := []UpsertCondition{{"ledger_sequence", OpGT}}

	sql, args, err := upsertRowsSQL("contract_data", "key_hash", fields, setExprs, conditions)
	require.NoError(t, err)

	want := `WITH r AS (SELECT unnest(?::text[]) /* key_hash */,unnest(?::int[]) /* ledger_sequence */,unnest(?::int[]) /* live_until_ledger_sequence */)` +
		` INSERT INTO contract_data (key_hash,ledger_sequence,live_until_ledger_sequence) SELECT * from r` +
		` ON CONFLICT (key_hash) DO UPDATE SET key_hash = excluded.key_hash,ledger_sequence = excluded.ledger_sequence,` +
		`live_until_ledger_sequence = COALESCE(excluded.live_until_ledger_sequence, contract_data.live_until_ledger_sequence)` +
		` WHERE excluded.ledger_sequence > contract_data.ledger_sequence`
	assert.Equal(t, want, strings.Join(strings.Fields(sql), " "))

	require.Len(t, args, len(fields))
	for i, field := range fields {
		assert.Equal(t, pq.Array(field.objects), args[i])
	}
}

func TestUpsertRowsSQLWithoutOverridesOrConditions(t *testing.T) {
	fields := []UpsertField{{"key_hash", "text", []interface{}{"a"}}}

	sql, args, err := upsertRowsSQL("contract_data", "key_hash", fields, nil, nil)
	require.NoError(t, err)

	want := `WITH r AS (SELECT unnest(?::text[]) /* key_hash */) INSERT INTO contract_data (key_hash) SELECT * from r` +
		` ON CONFLICT (key_hash) DO UPDATE SET key_hash = excluded.key_hash`
	assert.Equal(t, want, strings.Join(strings.Fields(sql), " "))
	assert.Equal(t, []interface{}{pq.Array(fields[0].objects)}, args)
}

func TestUpsertRowsSQLRejectsSetExprOnUnknownField(t *testing.T) {
	fields := []UpsertField{{"key_hash", "text", []interface{}{"a"}}}
	setExprs := []UpsertSetExpr{{"live_until_ledger_sequence", "excluded.live_until_ledger_sequence"}}

	sql, args, err := upsertRowsSQL("contract_data", "key_hash", fields, setExprs, nil)
	assert.EqualError(t, err, "set expression for live_until_ledger_sequence, which is not an upsert field")
	assert.Equal(t, "", sql)
	assert.Nil(t, args)
}

func TestUpsertRowsSQLRejectsInvalidOperator(t *testing.T) {
	fields := []UpsertField{{"key_hash", "text", []interface{}{"a"}}}
	conditions := []UpsertCondition{{"ledger_sequence", Operator("DROP")}}

	sql, args, err := upsertRowsSQL("contract_data", "key_hash", fields, nil, conditions)
	assert.EqualError(t, err, "invalid operator for condition on field ledger_sequence")
	assert.Equal(t, "", sql)
	assert.Nil(t, args)
}
