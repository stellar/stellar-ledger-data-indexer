package utils

import (
	"context"
	"fmt"
)

func (p *PostgresAdapter) Write(ctx context.Context, msg Message) error {
	p.BatchInsertBuilder.Session().Begin(ctx)

	p.BatchInsertBuilder.Reset()
	err := p.BatchInsertBuilder.Add(msg.Payload)
	if err != nil {
		return fmt.Errorf("error inserting into %s: %w", p.BatchInsertBuilder.TableName(), err)
	}

	err = p.BatchInsertBuilder.Exec(ctx)
	if err != nil {
		return fmt.Errorf("error inserting into %s: %w", p.BatchInsertBuilder.TableName(), err)
	}

	err = p.BatchInsertBuilder.Session().Commit()
	if err != nil {
		panic("Commit failed: " + err.Error())
	}

	p.BatchInsertBuilder.Reset()

	return err
}

// Close shuts down the DB connection
func (p *PostgresAdapter) Close() {
	if p.BatchInsertBuilder != nil {
		_ = p.BatchInsertBuilder.Close()
	}
}
