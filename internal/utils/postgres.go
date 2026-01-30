package utils

import (
	"context"
	"fmt"
)

func chunkRecords[T any](records []T, chunkSize int) [][]T {
	var chunks [][]T
	for i := 0; i < len(records); i += chunkSize {
		end := i + chunkSize
		if end > len(records) {
			end = len(records)
		}
		chunks = append(chunks, records[i:end])
	}
	return chunks
}

func (p *PostgresAdapter) Write(ctx context.Context, msg Message) error {
	p.DBOperator.Session().Begin(ctx)

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
	for _, batch := range chunkRecords(records, batchSize) {
		if err := p.DBOperator.Upsert(ctx, batch); err != nil {
			return fmt.Errorf("error adding batch to %s: %w", p.DBOperator.TableName(), err)
		}
		err := p.Flush(ctx)
		if err != nil {
			return fmt.Errorf("error flushing batch insert to %s: %w", p.DBOperator.TableName(), err)
		}
		p.DBOperator.Session().Begin(ctx)
	}

	if err := p.Flush(ctx); err != nil {
		return fmt.Errorf("final flush failed for %s: %w",
			p.DBOperator.TableName(), err)
	}

	p.Logger.Info("Insert completed successfully", "table", p.DBOperator.TableName(), "records", len(records))

	return nil
}

func (p *PostgresAdapter) Flush(ctx context.Context) error {
	err := p.DBOperator.Session().Commit()
	if err != nil {
		rollbackErr := p.DBOperator.Session().Rollback()
		if rollbackErr != nil {
			return fmt.Errorf(
				"commit failed for %s: %v; rollback also failed: %v",
				p.DBOperator.TableName(), err, rollbackErr,
			)
		}
		return fmt.Errorf("error committing transaction for %s. rolled back successfully: %w", p.DBOperator.TableName(), err)
	}
	return nil
}

func (p *PostgresAdapter) Close() {
	if p.DBOperator != nil {
		_ = p.DBOperator.Session().Close()
	}
}

func (p *PostgresAdapter) GetMaxLedgerSequence(ctx context.Context) (uint32, error) {
	return p.DBOperator.GetMaxLedgerSequence(ctx)
}
