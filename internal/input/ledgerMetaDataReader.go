package input

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest/cdp"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/storage"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-ledger-data-indexer/internal/utils"
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

func (a *LedgerMetadataReader) Run(ctx context.Context) error {
	historyArchive, err := historyarchive.NewArchivePool(a.historyArchiveURLs, historyarchive.ArchiveOptions{
		ConnectOptions: storage.ConnectOptions{
			UserAgent: "ledger-data-indexer",
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
	if a.startLedger == 0 {
		a.startLedger = latestNetworkLedger
	}

	// If no end ledger specified, or it's greater than the latest ledger,
	// use an unbounded range from the start ledger
	if a.endLedger == 0 || a.endLedger > latestNetworkLedger {
		fmt.Printf("Starting at ledger %v ...\n", latestNetworkLedger)
		ledgerRange = ledgerbackend.UnboundedRange(a.startLedger)
	} else {
		fmt.Printf("Processing ledgers from %d to %d\n", a.startLedger, a.endLedger)
		ledgerRange = ledgerbackend.BoundedRange(a.startLedger, a.endLedger)
	}
	pubConfig := cdp.PublisherConfig{
		DataStoreConfig:       a.dataStoreConfig,
		BufferedStorageConfig: cdp.DefaultBufferedStorageBackendConfig(a.dataStoreConfig.Schema.LedgersPerFile),
	}
	pubConfig.BufferedStorageConfig.RetryLimit = 20
	pubConfig.BufferedStorageConfig.RetryWait = 3

	return cdp.ApplyLedgerMetadata(ledgerRange, pubConfig, ctx,
		func(lcm xdr.LedgerCloseMeta) error {
			for _, processor := range a.processors {
				if err = processor.Process(ctx, utils.Message{Payload: lcm}); err != nil {
					return err
				}
			}
			return nil
		})
}
