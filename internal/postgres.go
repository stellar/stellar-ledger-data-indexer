package internal

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type PostgresAdapter struct {
	db         *sql.DB
	dbWritable DBWritable
}

func NewPostgresAdapter(connStr string, dbWritable DBWritable) (*PostgresAdapter, error) {
	fmt.Println("opening postgres")
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &PostgresAdapter{db: db, dbWritable: dbWritable}, nil
}

func (p *PostgresAdapter) Write(ctx context.Context, msg Message) error {
	_, err := p.db.ExecContext(ctx, p.dbWritable.CreateTableSQL())
	if err != nil {
		return fmt.Errorf("error creating table: %w", err)
	}

	_, err = p.db.ExecContext(ctx, p.dbWritable.CreateIndexSQL())
	if err != nil {
		return fmt.Errorf("error creating index: %w", err)
	}

	_, err = p.db.ExecContext(ctx, p.dbWritable.InsertSQL(), p.dbWritable.InsertArgs(msg.Payload)...)
	if err != nil {
		return fmt.Errorf("error inserting into %s: %w", p.dbWritable.TableName(), err)
	}
	return err
}

// Close shuts down the DB connection
func (p *PostgresAdapter) Close() {
	if p.db != nil {
		_ = p.db.Close()
	}
}
