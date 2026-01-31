package input

import (
	"context"

	"github.com/pkg/errors"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-ledger-data-indexer/internal/utils"
)

const (
	UserAgent = "stellar-ledger-data-indexer"
)

type LedgerMetadataReader struct {
	processors         []utils.Processor
	dataStoreConfig    datastore.DataStoreConfig
	startLedger        uint32
	endLedger          uint32
	backfill           bool
	maxLedgerInDB      uint32
	maxLedgerInGalexie uint32
}

func NewLedgerMetadataReader(config *datastore.DataStoreConfig,
	processors []utils.Processor,
	startLedger uint32,
	endLedger uint32,
	backfill bool,
	maxLedgerInDB uint32,
	maxLedgerInGalexie uint32) (*LedgerMetadataReader, error) {
	if config == nil {
		return nil, errors.New("missing configuration")
	}
	return &LedgerMetadataReader{
		processors:         processors,
		dataStoreConfig:    *config,
		startLedger:        startLedger,
		endLedger:          endLedger,
		backfill:           backfill,
		maxLedgerInDB:      maxLedgerInDB,
		maxLedgerInGalexie: maxLedgerInGalexie,
	}, nil
}

// Returns the ledger range to process and a boolean indicating if processing should proceed.
func GetLedgerBound(startLedger uint32, endLedger uint32, latestNetworkLedger uint32, backfill bool, maxLedgerInDB uint32, logger *log.Entry) (ledgerbackend.Range, bool) {
	const UnboundedSentinel = uint32(1)
	if endLedger > UnboundedSentinel && endLedger < startLedger {
		logger.Errorf("End ledger %d is less than start ledger %d", endLedger, startLedger)
		return ledgerbackend.Range{}, false
	}
	if endLedger > UnboundedSentinel && endLedger > latestNetworkLedger {
		logger.Errorf("End ledger %d is greater than latest network ledger %d", endLedger, latestNetworkLedger)
		return ledgerbackend.Range{}, false
	}

	if startLedger > UnboundedSentinel && startLedger > latestNetworkLedger {
		logger.Errorf("Start ledger %d is greater than latest network ledger %d", startLedger, latestNetworkLedger)
		return ledgerbackend.Range{}, false
	}

	if endLedger <= maxLedgerInDB && endLedger > UnboundedSentinel && backfill == false {
		logger.Infof("End ledger %d is less than or equal to max ledger in DB %d, nothing to process", endLedger, maxLedgerInDB)
		return ledgerbackend.Range{}, false
	}

	if !backfill && maxLedgerInDB > 0 && startLedger <= maxLedgerInDB {
		startLedger = maxLedgerInDB
		logger.Infof("Using max ledger sequence as start")
	}

	isUnbounded := endLedger <= UnboundedSentinel || startLedger <= UnboundedSentinel || startLedger == latestNetworkLedger

	if isUnbounded {
		if startLedger <= UnboundedSentinel {
			startLedger = latestNetworkLedger
		}
		logger.Infof("Operating in unbounded mode from ledger %d", startLedger)
		return ledgerbackend.UnboundedRange(startLedger), true
	}

	logger.Infof("Operating in bounded mode from ledger %d to ledger %d", startLedger, endLedger)
	return ledgerbackend.BoundedRange(startLedger, endLedger), true

}

func (a *LedgerMetadataReader) Run(ctx context.Context, Logger *log.Entry) error {
	latestNetworkLedger := a.maxLedgerInGalexie

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
				if err := processor.Process(ctx, utils.Message{Payload: lcm}); err != nil {
					return err
				}
			}
			return nil
		})
}
