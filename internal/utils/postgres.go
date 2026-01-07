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

	const batchSize = 1000
	for i, record := range records {
		if err := p.BatchInsertBuilder.Add(record); err != nil {
			return fmt.Errorf("error adding record to %s: %w", p.BatchInsertBuilder.TableName(), err)
		}
		if (i+1)%batchSize == 0 {
			err := p.Flush(ctx)
			if err != nil {
				return fmt.Errorf("error flushing batch insert to %s: %w", p.BatchInsertBuilder.TableName(), err)
			}
		}
	}
	if err := p.Flush(ctx); err != nil {
		return fmt.Errorf("final flush failed for %s: %w",
			p.BatchInsertBuilder.TableName(), err)
	}

	p.Logger.Info("Insert completed successfully", "table", p.BatchInsertBuilder.TableName(), "records", len(records))

	return nil
}

func (p *PostgresAdapter) Flush(ctx context.Context) error {
	err := p.BatchInsertBuilder.Exec(ctx)
	if err != nil {
		return fmt.Errorf("error inserting into %s: %w", p.BatchInsertBuilder.TableName(), err)
	}

	err = p.BatchInsertBuilder.Session().Commit()
	if err != nil {
		return fmt.Errorf("error committing transaction for %s: %w", p.BatchInsertBuilder.TableName(), err)
	}
	p.BatchInsertBuilder.Reset()
	return nil
}

func (p *PostgresAdapter) Close() {
	if p.BatchInsertBuilder != nil {
		_ = p.BatchInsertBuilder.Close()
	}
}
