package db

import (
	"github.com/stellar/go/support/db"
)

type DBSession struct {
	session db.SessionInterface
}

// upsertField is used in upsertRows function generating upsert query for
// different tables.
type UpsertField struct {
	name    string
	dbType  string
	objects []interface{}
}

type Operator string

type UpsertCondition struct {
	column   string
	operator Operator
}
