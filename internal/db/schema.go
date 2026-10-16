package db

import (
	"github.com/stellar/go-stellar-sdk/support/db"
)

type DBSession struct {
	session db.SessionInterface
}

// UpsertField is used in UpsertRows function generating upsert query for
// different tables.
type UpsertField struct {
	name    string
	dbType  string
	objects []interface{}
}

// UpsertSetExpr replaces the default "column = excluded.column" assignment of
// one column in the ON CONFLICT DO UPDATE clause built by UpsertRows.
type UpsertSetExpr struct {
	column string
	expr   string
}

type Operator string

type UpsertCondition struct {
	column   string
	operator Operator
}
