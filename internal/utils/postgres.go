package utils

import (
	"context"
	"fmt"
)

func (p *PostgresAdapter) Write(ctx context.Context, msg Message) error {
	p.BatchInsertBuilder.Session().Begin(ctx)

	var records []interface{}
	switch msg.Payload.(type) {
	case []interface{}:
		p.Logger.Info("Processing batch insert for multiple records")
		records = msg.Payload.([]interface{})
	default:
		p.Logger.Info("Processing single record insert")
		records = []interface{}{msg.Payload}
	}

	for _, record := range records {
		if err := p.BatchInsertBuilder.Add(record); err != nil {
			return fmt.Errorf("error adding record to %s: %w", p.BatchInsertBuilder.TableName(), err)
		}
	}

	err := p.BatchInsertBuilder.Exec(ctx)
	if err != nil {
		return fmt.Errorf("error inserting into %s: %w", p.BatchInsertBuilder.TableName(), err)
	}

	err = p.BatchInsertBuilder.Session().Commit()
	if err != nil {
		return fmt.Errorf("error committing transaction for %s: %w", p.BatchInsertBuilder.TableName(), err)
	}

	p.Logger.Info("Insert completed successfully", "table", p.BatchInsertBuilder.TableName(), "records", len(records))
	p.BatchInsertBuilder.Reset()

	return err
}

func (p *PostgresAdapter) Close() {
	if p.BatchInsertBuilder != nil {
		_ = p.BatchInsertBuilder.Close()
	}
}
