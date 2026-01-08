package input

import (
	"context"

	"github.com/pkg/errors"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/support/storage"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-ledger-data-indexer/internal/utils"
)

const (
	UserAgent = "stellar-ledger-data-indexer"
)

type LedgerMetadataReader struct {
	processors         []utils.Processor
	historyArchiveURLs []string
	dataStoreConfig    datastore.DataStoreConfig
	startLedger        uint32
	endLedger          uint32
}

func NewLedgerMetadataReader(config *datastore.DataStoreConfig,
	historyArchiveUrls []string,
	processors []utils.Processor, startLedger uint32, endLedger uint32) (*LedgerMetadataReader, error) {
	if config == nil {
		return nil, errors.New("missing configuration")
	}
	return &LedgerMetadataReader{
		processors:         processors,
		dataStoreConfig:    *config,
		historyArchiveURLs: historyArchiveUrls,
		startLedger:        startLedger,
		endLedger:          endLedger,
	}, nil
}

func (a *LedgerMetadataReader) Run(ctx context.Context, Logger *log.Entry) error {
	historyArchive, err := historyarchive.NewArchivePool(a.historyArchiveURLs, historyarchive.ArchiveOptions{
		ConnectOptions: storage.ConnectOptions{
			UserAgent: UserAgent,
			Context:   ctx,
		},
	})
	if err != nil {
		return errors.Wrap(err, "error creating history archive client")
	}
	latestNetworkLedger, err := historyArchive.GetLatestLedgerSequence()

	if err != nil {
		return errors.Wrap(err, "error getting latest ledger")
	}

	// Determine the actual ledger range to process
	var ledgerRange ledgerbackend.Range

	// If no start ledger specified, start from the latest ledger
	if a.startLedger == 1 {
		a.startLedger = latestNetworkLedger
	}

	// If no end ledger specified, or it's greater than the latest ledger,
	// use an unbounded range from the start ledger
	if a.endLedger == 1 || a.endLedger > latestNetworkLedger {
		Logger.Infof("Starting at ledger %v ...\n", a.startLedger)
		ledgerRange = ledgerbackend.UnboundedRange(a.startLedger)
	} else {
		Logger.Infof("Processing ledgers from %d to %d\n", a.startLedger, a.endLedger)
		ledgerRange = ledgerbackend.BoundedRange(a.startLedger, a.endLedger)
	}
	pubConfig := ingest.PublisherConfig{
		DataStoreConfig:       a.dataStoreConfig,
		BufferedStorageConfig: ingest.DefaultBufferedStorageBackendConfig(a.dataStoreConfig.Schema.LedgersPerFile),
	}
	pubConfig.BufferedStorageConfig.RetryLimit = 20
	pubConfig.BufferedStorageConfig.RetryWait = 3

	return ingest.ApplyLedgerMetadata(ledgerRange, pubConfig, ctx,
		func(lcm xdr.LedgerCloseMeta) error {
			for _, processor := range a.processors {
				if err = processor.Process(ctx, utils.Message{Payload: lcm}); err != nil {
					return err
				}
			}
			return nil
		})
}
