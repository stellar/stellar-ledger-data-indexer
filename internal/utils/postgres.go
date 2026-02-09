package utils

import (
	"context"
	"fmt"
	"time"
)

const (
	maxRetries  = 5
	baseBackoff = 5000 * time.Millisecond
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
		var lastErr error
		for attempt := 0; attempt < maxRetries; attempt++ {
			tx := p.DBOperator.Session()
			tx.Begin(ctx)
			err := p.DBOperator.Upsert(ctx, batch)
			if err == nil {
				err = p.Flush(ctx)
			}
			if err == nil {
				// Success
				lastErr = nil
				break
			}
			// rollback transaction on error
			tx.Rollback()

			lastErr = err
			backoff := time.Duration(attempt+1) * baseBackoff
			p.Logger.Warn(
				"retryable db error, retrying",
				"table", p.DBOperator.TableName(),
				"attempt", attempt+1,
				"backoff", backoff,
				"err", err,
			)
			time.Sleep(backoff)
			continue

			// Non-retryable error
			return fmt.Errorf("error adding batch to %s: %w",
				p.DBOperator.TableName(), err)
		}
		if lastErr != nil {
			return fmt.Errorf(
				"exceeded retries for table %s: %w",
				p.DBOperator.TableName(), lastErr,
			)
		}
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
