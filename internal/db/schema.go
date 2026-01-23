package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/stellar/go/support/db"
)

type DBSession struct {
	db.SessionInterface
}

// upsertField is used in upsertRows function generating upsert query for
// different tables.
type UpsertField struct {
	name    string
	dbType  string
	objects []interface{}
}

type Operator string

const (
	OpLT Operator = "<"
	OpGT Operator = ">"
	OpLE Operator = "<="
	OpGE Operator = ">="
	OpEQ Operator = "="
)

func (o Operator) Valid() bool {
	switch o {
	case OpLT, OpGT, OpLE, OpGE, OpEQ:
		return true
	default:
		return false
	}
}

type UpsertCondition struct {
	column   string
	operator Operator
}

// Extended from https://github.com/stellar/stellar-horizon/blob/main/internal/db2/history/main.go

func (q *DBSession) UpsertRows(ctx context.Context, table string, conflictField string, fields []UpsertField, conditions []UpsertCondition) error {
	unnestPart := make([]string, 0, len(fields))
	insertFieldsPart := make([]string, 0, len(fields))
	onConflictPart := make([]string, 0, len(fields))
	pqArrays := make([]interface{}, 0, len(fields))
	onConflictConditionPart := make([]string, 0, len(fields))

	for _, field := range fields {
		unnestPart = append(
			unnestPart,
			fmt.Sprintf("unnest(?::%s[]) /* %s */", field.dbType, field.name),
		)
		insertFieldsPart = append(
			insertFieldsPart,
			field.name,
		)
		onConflictPart = append(
			onConflictPart,
			fmt.Sprintf("%s = excluded.%s", field.name, field.name),
		)
		pqArrays = append(
			pqArrays,
			pq.Array(field.objects),
		)
	}
	for _, condition := range conditions {
		if !condition.operator.Valid() {
			return fmt.Errorf("invalid operator for condition on field %s", condition.column)
		}
		onConflictConditionPart = append(
			onConflictConditionPart,
			fmt.Sprintf("excluded.%s %s %s.%s", condition.column, condition.operator, table, condition.column),
		)
	}

	sql := `
	WITH r AS
		(SELECT ` + strings.Join(unnestPart, ",") + `)
	INSERT INTO ` + table + `
		(` + strings.Join(insertFieldsPart, ",") + `)
	SELECT * from r
	ON CONFLICT (` + conflictField + `) DO UPDATE SET
		` + strings.Join(onConflictPart, ",")
	if len(onConflictConditionPart) > 0 {
		sql += " WHERE " + strings.Join(onConflictConditionPart, " AND ")
	}

	_, err := q.ExecRaw(
		context.WithValue(ctx, &db.QueryTypeContextKey, db.UpsertQueryType),
		sql,
		pqArrays...,
	)
	return err
}
