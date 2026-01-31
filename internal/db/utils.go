package db

import (
	"context"
	"embed"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	migrate "github.com/rubenv/sql-migrate"
	"github.com/stellar/go/support/db"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

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

func NewPostgresSession(ctx context.Context, connStr string) (*DBSession, error) {
	session, err := db.Open("postgres", connStr)

	if err != nil {
		return nil, fmt.Errorf("failed to open postgres instance: %w", err)
	}

	if err := session.Ping(ctx, 5*time.Second); err != nil {
		return nil, fmt.Errorf("failed to ping postgres instance: %w", err)
	}

	migrations := &migrate.EmbedFileSystemMigrationSource{
		FileSystem: migrationsFS,
		Root:       "migrations",
	}
	_, err = migrate.Exec(session.DB.DB, "postgres", migrations, migrate.Up)
	if err != nil {
		return nil, fmt.Errorf("failed to apply migrations: %w", err)
	}

	return &DBSession{session: session}, nil
}

// GetMaxLedgerSequence returns the maximum ledger_sequence from the specified table.
// Returns 0 if the table is empty. Returns an error if the table name is invalid or if the query fails.
func (q *DBSession) GetMaxLedgerSequence(ctx context.Context, tableName string) (uint32, error) {
	// Validate table name against allowed tables to prevent SQL injection
	// Using a map to also prepare queries with validated table names
	allowedQueries := map[string]string{
		"contract_data": "SELECT COALESCE(MAX(ledger_sequence), 0) FROM contract_data",
		"ttl":           "SELECT COALESCE(MAX(ledger_sequence), 0) FROM ttl",
	}
	query, ok := allowedQueries[tableName]
	if !ok {
		return 0, fmt.Errorf("invalid table name: %s", tableName)
	}

	var maxLedger uint32
	err := q.session.GetRaw(ctx, &maxLedger, query)
	if err != nil {
		return 0, fmt.Errorf("failed to get max ledger sequence from %s: %w", tableName, err)
	}
	return maxLedger, nil
}

// Extended from https://github.com/stellar/stellar-horizon/blob/main/internal/db2/history/main.go
func (q *DBSession) UpsertRows(ctx context.Context, table string, conflictField string, fields []UpsertField, conditions []UpsertCondition) (rowsAffected int64, err error) {
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
			return 0, fmt.Errorf("invalid operator for condition on field %s", condition.column)
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

	sqlRes, err := q.session.ExecRaw(
		context.WithValue(ctx, &db.QueryTypeContextKey, db.UpsertQueryType),
		sql,
		pqArrays...,
	)
	rowsAffected, _ = sqlRes.RowsAffected()
	return rowsAffected, err
}
