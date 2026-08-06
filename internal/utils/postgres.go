package utils

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

const (
	maxRetries  = 5
	baseBackoff = 5000 * time.Millisecond
)

// permanentPQCodes are PostgreSQL error codes attributable to the *value* in a
// single row rather than to the connection or the schema. Retrying them is
// pointless -- the same row fails the same way every time -- so a row that
// trips one of these is dropped rather than allowed to halt ingestion.
//
// Membership is deliberately narrow. A code that indicates a *statement or
// schema* problem must never appear here: it would fail every row identically,
// so the row-by-row salvage below would drop the entire batch and report
// success, silently erasing data for as long as the mismatch lasted. That is
// strictly worse than crashing, because nothing surfaces it. datatype_mismatch
// (42804) is the obvious example and is intentionally absent -- a deployment
// whose expectations disagree with the live schema must fail loudly.
//
// Codes are from https://www.postgresql.org/docs/16/errcodes-appendix.html.
var permanentPQCodes = map[pq.ErrorCode]struct{}{
	"22021": {}, // character_not_in_repertoire  -- e.g. 0x00 in a text column
	"22P05": {}, // untranslatable_character
	"22001": {}, // string_data_right_truncation
	"22003": {}, // numeric_value_out_of_range
	"22007": {}, // invalid_datetime_format
	"22008": {}, // datetime_field_overflow
	"23502": {}, // not_null_violation
	"23505": {}, // unique_violation
	"23514": {}, // check_violation
}

// isPermanentWriteErr reports whether err is a data-shaped failure that cannot
// succeed on retry.
//
// This distinction matters more than it looks. A failed batch aborts the
// ingestion callback, which is fatal to the process, and on restart this service
// derives its resume point from MAX(ledger_sequence) -- so a ledger it cannot
// write is a ledger it can never advance past. Treating an unwritable row as
// merely "transient" converts one bad row into a permanent halt, plus the full
// backoff ladder burned on every restart.
//
// The error arrives wrapped several layers deep (fmt.Errorf %w over
// support/errors.Wrap over *pq.Error), so this relies on errors.As walking the
// chain. TestIsPermanentWriteErrThroughRealWrapping guards that assumption.
func isPermanentWriteErr(err error) bool {
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) {
		return false
	}
	_, permanent := permanentPQCodes[pqErr.Code]
	return permanent
}

// retryBackoff is the base unit of the backoff ladder. It is indirected through
// a method so tests can shrink it; production leaves BaseBackoff unset.
func (p *PostgresAdapter) retryBackoff() time.Duration {
	if p.BaseBackoff > 0 {
		return p.BaseBackoff
	}
	return baseBackoff
}

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
			err := p.upsertBatch(ctx, batch)
			if err == nil {
				lastErr = nil
				break
			}
			lastErr = err

			// A permanent error will fail identically on every retry, so salvage
			// what we can instead of sleeping and then killing the process.
			if isPermanentWriteErr(err) {
				dropped, rowErr := p.upsertRowsIndividually(ctx, batch)
				if rowErr == nil {
					p.Logger.Warnf(
						"dataset %s (table %s): recovered a permanently failing batch of %d by writing rows individually, %d dropped",
						p.DBOperator.DatasetName(), p.DBOperator.TableName(), len(batch), dropped,
					)
					lastErr = nil
					break
				}
				// Salvage itself hit a transient failure. Fall through to the normal
				// backoff rather than aborting ingestion: the retry contract is that
				// transient errors get retried, and re-running the batch is safe
				// because the upsert is idempotent.
				lastErr = rowErr
			}

			backoff := time.Duration(attempt+1) * p.retryBackoff()
			p.Logger.Warn(
				"retryable db error, retrying",
				"table", p.DBOperator.TableName(),
				"attempt", attempt+1,
				"backoff", backoff,
				"err", err,
			)
			// Sleep interruptibly so a shutdown signal is not stuck behind the
			// remainder of the backoff ladder.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
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

// upsertBatch writes one batch inside its own transaction.
func (p *PostgresAdapter) upsertBatch(ctx context.Context, batch []interface{}) error {
	tx := p.DBOperator.Session()
	if err := tx.Begin(ctx); err != nil {
		return fmt.Errorf("begin failed for %s: %w", p.DBOperator.TableName(), err)
	}
	if err := p.DBOperator.Upsert(ctx, batch); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			p.Logger.Warnf(
				"rollback failed for %s after upsert error: %v", p.DBOperator.TableName(), rollbackErr,
			)
		}
		return err
	}
	// Flush commits, and rolls back itself if the commit fails.
	return p.Flush(ctx)
}

// upsertRowsIndividually re-drives a batch that failed permanently, one row per
// transaction, so that a single unwritable row does not cost us every other row
// in the same ledger. It returns the number of rows dropped.
//
// Only permanent, data-shaped failures are dropped. A transient error is
// returned to the caller so the batch can still be retried.
//
// Dropping a row means the index is knowingly incomplete, which is a real cost —
// but a strictly smaller one than halting ingestion for every consumer of this
// table until someone ships a code change. Each drop is logged at error level
// and counted, so it is visible rather than silent.
func (p *PostgresAdapter) upsertRowsIndividually(ctx context.Context, batch []interface{}) (int, error) {
	dropped := 0
	for _, record := range batch {
		err := p.upsertBatch(ctx, []interface{}{record})
		if err == nil {
			continue
		}
		if !isPermanentWriteErr(err) {
			return dropped, err
		}
		dropped++
		p.Logger.Errorf(
			"dataset %s (table %s): dropping a row that PostgreSQL cannot store, it will be missing from the index: %v",
			p.DBOperator.DatasetName(), p.DBOperator.TableName(), err,
		)
		if p.MetricRecorder != nil {
			// Label by dataset, not table: the TTL operator writes to table
			// contract_data (internal/db/ttl.go), so a table label would make
			// skipped TTL updates indistinguishable from skipped contract-data rows.
			p.MetricRecorder.RecordSkippedRow(p.DBOperator.DatasetName(), skipReason(err))
		}
	}
	return dropped, nil
}

// skipReason renders a low-cardinality Prometheus label for a dropped row.
func skipReason(err error) string {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return string(pqErr.Code)
	}
	return "unknown"
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
