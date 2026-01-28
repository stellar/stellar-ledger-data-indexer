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

// MaxLedgerProvider defines the interface for querying the maximum ledger sequence from the database
type MaxLedgerProvider interface {
	GetMaxLedgerSequence(ctx context.Context) (uint32, error)
}

type LedgerMetadataReader struct {
	processors         []utils.Processor
	historyArchiveURLs []string
	dataStoreConfig    datastore.DataStoreConfig
	startLedger        uint32
	endLedger          uint32
	backfill           bool
	maxLedgerProvider  MaxLedgerProvider
}

func NewLedgerMetadataReader(config *datastore.DataStoreConfig,
	historyArchiveUrls []string,
	processors []utils.Processor,
	startLedger uint32,
	endLedger uint32,
	backfill bool,
	maxLedgerProvider MaxLedgerProvider) (*LedgerMetadataReader, error) {
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
		maxLedgerProvider:  maxLedgerProvider,
	}, nil
}

// GetLedgerBound determines the ledger range to process, incorporating database state awareness.
// It combines start ledger determination logic with ledger bound calculation.
// Parameters:
//   - startLedger: requested start ledger (if <= 1, uses latestNetworkLedger)
//   - endLedger: requested end ledger (if <= 1, creates unbounded range)
//   - latestNetworkLedger: the latest ledger available on the network
//   - backfill: if true, skips database checks and uses exact start/end ledgers
//   - maxLedgerProvider: provider to query max ledger from database (can be nil in backfill mode)
//   - ctx: context for database operations
//   - Logger: logger for informational messages
//
// Returns the ledger range to process and a boolean indicating if processing should proceed.
func GetLedgerBound(startLedger uint32, endLedger uint32, latestNetworkLedger uint32, backfill bool, maxLedgerProvider MaxLedgerProvider, ctx context.Context, Logger *log.Entry) (ledgerbackend.Range, bool) {
	// Sentinel value for unbounded mode
	const UnboundedModeSentinel = uint32(1)

	// If no start ledger specified, start from the latest ledger
	if startLedger <= 1 {
		startLedger = latestNetworkLedger
	}

	var maxLedgerInDB uint32

	// In backfill mode, skip database checks and use exact start/end ledgers
	if backfill {
		Logger.Infof("Backfill mode enabled: Using exact start=%d and end=%d ledgers as provided", startLedger, endLedger)
		maxLedgerInDB = 0
	} else if maxLedgerProvider != nil {
		// Query the max ledger sequence from the database
		var err error
		maxLedgerInDB, err = maxLedgerProvider.GetMaxLedgerSequence(ctx)
		if err != nil {
			Logger.Errorf("Failed to get max ledger sequence from database: %v. Proceeding with requested start ledger.", err)
			maxLedgerInDB = 0
		} else {
			Logger.Infof("Max ledger sequence in database: %d", maxLedgerInDB)
		}
	}

	// Apply database-aware start ledger adjustment if maxLedgerInDB is available
	if maxLedgerInDB > 0 {
		// If end ledger is not provided (unbounded mode)
		if endLedger <= UnboundedModeSentinel {
			// Start from the max ledger in DB (it may not have all entries ingested)
			startLedger = maxLedgerInDB
			Logger.Infof("Unbounded mode: Starting from ledger %d (max in DB)", startLedger)
		} else {
			// Bounded mode: end ledger is provided
			if maxLedgerInDB >= endLedger {
				// All requested ledgers are already ingested
				Logger.Infof("All ledgers from %d to %d are already ingested (max in DB: %d). Nothing to do.",
					startLedger, endLedger, maxLedgerInDB)
				return ledgerbackend.Range{}, false
			} else if maxLedgerInDB >= startLedger {
				// Some ledgers are already ingested, start from where we left off
				startLedger = maxLedgerInDB
				Logger.Infof("Bounded mode: Resuming from ledger %d (max in DB) to %d",
					startLedger, endLedger)
			} else {
				// Max in DB is less than requested start, use the requested start
				Logger.Infof("Bounded mode: Starting from requested ledger %d to %d (max in DB %d is before start)",
					startLedger, endLedger, maxLedgerInDB)
			}
		}
	} else if !backfill {
		// No database state available and not in backfill mode
		Logger.Infof("Database is empty or state unknown, starting from requested ledger %d", startLedger)
	}

	// If no end ledger specified, or it's greater than the latest ledger,
	// use an unbounded range from the start ledger
	if endLedger <= UnboundedModeSentinel || endLedger > latestNetworkLedger {
		Logger.Infof("Starting at ledger %v ...\n", startLedger)
		return ledgerbackend.UnboundedRange(startLedger), true
	}

	Logger.Infof("Processing ledgers from %d to %d\n", startLedger, endLedger)
	return ledgerbackend.BoundedRange(startLedger, endLedger), true
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
	// GetLedgerBound will query max ledger from database if not in backfill mode
	ledgerRange, shouldProceed := GetLedgerBound(a.startLedger, a.endLedger, latestNetworkLedger, a.backfill, a.maxLedgerProvider, ctx, Logger)
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
