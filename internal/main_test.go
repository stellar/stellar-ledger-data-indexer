package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostgresConnString(t *testing.T) {
	actualPostgresString := PostgresConnString(
		PostgresConfig{
			Host:     "localhost",
			Port:     5432,
			Database: "ledger_data",
			User:     "ledger_user",
		},
	)
	expected := "host=localhost port=5432 user=ledger_user dbname=ledger_data sslmode=disable"
	assert.Equal(t, expected, actualPostgresString, "expected %s, got %s", expected, actualPostgresString)
}
