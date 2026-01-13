package utils

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"

	"github.com/stellar/go/support/log"
	"github.com/stellar/stellar-ledger-data-indexer/internal/db"
)

type CSVWriter struct {
	file    *os.File
	writer  *csv.Writer
	headers []string
	Logger  *log.Entry
}

func NewCSVWriter(filename string, logger *log.Entry) (*CSVWriter, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	headers := []string{"contract_id", "ledger_sequence", "key_hash", "durability", "key_symbol", "key", "val", "closed_at"}

	writer := csv.NewWriter(file)
	writer.Comma = ',' // Use comma as separator
	err = writer.Write(headers)
	if err != nil {
		return nil, err
	}

	return &CSVWriter{
		file:    file,
		writer:  writer,
		headers: headers,
		Logger:  logger,
	}, nil
}

func (w *CSVWriter) Write(ctx context.Context, msg Message) error {
	var records []interface{}
	switch msg.Payload.(type) {
	case []interface{}:
		w.Logger.Info("Processing batch insert for multiple records")
		records = msg.Payload.([]interface{})
	default:
		w.Logger.Info("Processing single record insert")
		records = []interface{}{msg.Payload}
	}

	for _, record := range records {
		row := db.GetContractDataRecord(record)
		if err := w.writer.Write(row); err != nil {
			return fmt.Errorf("error writing record to CSV: %w", err)
		}
	}

	w.writer.Flush()
	return w.writer.Error()
}

func (w *CSVWriter) Close() {
	w.writer.Flush()
	err := w.file.Close()
	fmt.Printf("error closing CSV file %v", err)
}
