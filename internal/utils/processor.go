package utils

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"

	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
)

type Processor interface {
	Process(context.Context, Message) error
}

type BaseProcessor struct {
	OutboundAdapters []OutboundAdapter
	Logger           *log.Entry
	Passphrase       string
	HistoryArchive   historyarchive.ArchiveInterface
	MetricRecorder   MetricRecorder
}

func (p *BaseProcessor) CreateLCMDataReader(ledgerCloseMeta xdr.LedgerCloseMeta) (*ingest.LedgerChangeReader, error) {
	p.Logger.Infof("Creating LedgerChangeReader with phrase %s", p.Passphrase)
	return ingest.NewLedgerChangeReaderFromLedgerCloseMeta(p.Passphrase, ledgerCloseMeta)
}

func (p *BaseProcessor) ExtractLedgerCloseMeta(msg Message) (xdr.LedgerCloseMeta, error) {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("invalid payload type")
	}
	return ledgerCloseMeta, nil
}

func (p *BaseProcessor) SendInfo(ctx context.Context, data interface{}) error {
	for _, adapter := range p.OutboundAdapters {
		err := adapter.Write(ctx, Message{Payload: data})
		if err != nil {
			return fmt.Errorf("error sending data to outbound adapter: %w", err)
		}
	}
	return nil
}

func (p *BaseProcessor) ReadIngestChanges(ctx context.Context, msg Message) ([]ingest.Change, error) {
	changes := []ingest.Change{}

	ledgerCloseMeta, err := p.ExtractLedgerCloseMeta(msg)
	if err != nil {
		return []ingest.Change{}, err
	}

	dataReader, err := p.CreateLCMDataReader(ledgerCloseMeta)
	if err != nil {
		return []ingest.Change{}, err
	}

	for {
		change, err := dataReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return []ingest.Change{}, fmt.Errorf("could not read ledger data %w", err)
		}
		changes = append(changes, change)
	}
	return changes, nil
}

// RemoveDuplicatesByFields removes duplicate entries from a slice based on given primary key fields.
func RemoveDuplicatesByFields[T any](rows []T, pkFields []string) []T {
	seen := make(map[string]T)

	for _, row := range rows {
		v := reflect.ValueOf(row)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		keyParts := []any{}
		for _, f := range pkFields {
			field := v.FieldByName(f)
			if !field.IsValid() {
				panic("field " + f + " does not exist in struct")
			}
			keyParts = append(keyParts, field.Interface())
		}

		b, _ := json.Marshal(keyParts)
		hash := sha256.Sum256(b)
		key := hex.EncodeToString(hash[:])
		seen[key] = row // overwrite previous, keeping latest
	}

	// Sort keys to ensure deterministic output order
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	unique := make([]T, 0, len(seen))
	for _, k := range keys {
		unique = append(unique, seen[k])
	}

	return unique
}
