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
	backfill           bool
	maxLedgerInDB      uint32
}

func NewLedgerMetadataReader(config *datastore.DataStoreConfig,
	historyArchiveUrls []string,
	processors []utils.Processor,
	startLedger uint32,
	endLedger uint32,
	backfill bool,
	maxLedgerInDB uint32) (*LedgerMetadataReader, error) {
	if config == nil {
		return nil, errors.New("missing configuration")
	}
	return &LedgerMetadataReader{
		processors:         processors,
		dataStoreConfig:    *config,
		historyArchiveURLs: historyArchiveUrls,
		startLedger:        startLedger,
		endLedger:          endLedger,
		backfill:           backfill,
		maxLedgerInDB:      maxLedgerInDB,
	}, nil
}

// GetLedgerBound determines the ledger range to process, incorporating database state awareness.
// It combines start ledger determination logic with ledger bound calculation.
// Parameters:
//   - startLedger: requested start ledger
//   - endLedger: requested end ledger (if <= 1, creates unbounded range)
//   - latestNetworkLedger: the latest ledger available on the network
//   - backfill: if true, uses exact start/end ledgers without database state checks
//   - maxLedgerInDB: the maximum ledger sequence already in the database (use 0 if unknown)
//   - Logger: logger for informational messages
//
// Returns the ledger range to process and a boolean indicating if processing should proceed.
func GetLedgerBound(startLedger uint32, endLedger uint32, latestNetworkLedger uint32, backfill bool, maxLedgerInDB uint32, Logger *log.Entry) (ledgerbackend.Range, bool) {
	// Sentinel value for unbounded mode
	const UnboundedModeSentinel = uint32(1)

	// Determine if this is unbounded mode
	isUnbounded := endLedger <= UnboundedModeSentinel

	if backfill {
		// Backfill mode: use given start and end ledger
		Logger.Infof("Backfill mode enabled")

		// If start ledger <= 1, use latest network ledger in unbounded mode
		if startLedger <= 1 {
			startLedger = latestNetworkLedger
			Logger.Infof("Backfill mode: Using latest network ledger %d as start", startLedger)
		}

		if isUnbounded {
			Logger.Infof("Backfill mode: Starting unbounded from ledger %d", startLedger)
			return ledgerbackend.UnboundedRange(startLedger), true
		}

		Logger.Infof("Backfill mode: Processing ledgers from %d to %d", startLedger, endLedger)
		return ledgerbackend.BoundedRange(startLedger, endLedger), true
	}

	// Non-backfill mode
	if maxLedgerInDB > 0 {
		// Database has data
		if startLedger < maxLedgerInDB {
			// Use max db ledger as start
			startLedger = maxLedgerInDB
			Logger.Infof("Non-backfill mode: Starting from max DB ledger %d", startLedger)
		} else {
			// Use provided start ledger
			Logger.Infof("Non-backfill mode: Starting from requested ledger %d (max DB: %d)", startLedger, maxLedgerInDB)
		}

		if isUnbounded {
			Logger.Infof("Non-backfill mode: Starting unbounded from ledger %d", startLedger)
			return ledgerbackend.UnboundedRange(startLedger), true
		}

		// Bounded mode: check if work is already done
		if startLedger >= endLedger {
			Logger.Infof("All ledgers from requested range up to %d are already ingested (max in DB: %d). Nothing to do.", endLedger, maxLedgerInDB)
			return ledgerbackend.Range{}, false
		}

		Logger.Infof("Non-backfill mode: Processing ledgers from %d to %d", startLedger, endLedger)
		return ledgerbackend.BoundedRange(startLedger, endLedger), true
	}

	// Database is empty (maxLedgerInDB == 0)
	if startLedger <= 1 {
		// Use latest network ledger in unbounded mode
		startLedger = latestNetworkLedger
		Logger.Infof("Database empty: Using latest network ledger %d as start in unbounded mode", startLedger)
		return ledgerbackend.UnboundedRange(startLedger), true
	}

	if startLedger < latestNetworkLedger {
		// Start from given start ledger
		Logger.Infof("Database empty: Starting from requested ledger %d", startLedger)

		if isUnbounded {
			return ledgerbackend.UnboundedRange(startLedger), true
		}

		return ledgerbackend.BoundedRange(startLedger, endLedger), true
	}

	// Start ledger > latest network ledger: error
	Logger.Errorf("Start ledger %d is greater than latest network ledger %d", startLedger, latestNetworkLedger)
	return ledgerbackend.Range{}, false
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
	// Uses maxLedgerInDB from struct for database-aware ledger adjustments
	ledgerRange, shouldProceed := GetLedgerBound(a.startLedger, a.endLedger, latestNetworkLedger, a.backfill, a.maxLedgerInDB, Logger)
	if !shouldProceed {
		return nil
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
