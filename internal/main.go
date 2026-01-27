package internal

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/stellar/stellar-ledger-data-indexer/internal/db"
	"github.com/stellar/stellar-ledger-data-indexer/internal/input"
	"github.com/stellar/stellar-ledger-data-indexer/internal/transform"
	"github.com/stellar/stellar-ledger-data-indexer/internal/utils"
)

func PostgresConnString(cfg PostgresConfig) string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Database,
	)
}

func getProcessor(dataset string, outboundAdapters []utils.OutboundAdapter, passPhrase string) (processor utils.Processor, err error) {
	switch dataset {
	case "contract_data":
		processor := &transform.ContractDataProcessor{
			BaseProcessor: utils.BaseProcessor{
				OutboundAdapters: outboundAdapters,
				Logger:           Logger,
				Passphrase:       passPhrase,
			},
		}
		return processor, nil
	case "ttl":
		processor := &transform.TTLDataProcessor{
			BaseProcessor: utils.BaseProcessor{
				OutboundAdapters: outboundAdapters,
				Logger:           Logger,
				Passphrase:       passPhrase,
			},
		}
		return processor, nil
	default:
		return nil, fmt.Errorf("unsupported dataset: %s", dataset)
	}
}

func getPostgresOutputAdapter(ctx context.Context, dataset string, postgresConfig PostgresConfig) (outboundAdapter utils.OutboundAdapter, err error) {
	envPostgresConnString := os.Getenv("POSTGRES_CONN_STRING")
	var connString string
	if envPostgresConnString != "" {
		connString = envPostgresConnString
		Logger.Infof("Using Postgres connection string from environment variable")
	} else {
		connString = PostgresConnString(postgresConfig)
	}

	Logger.Infof("Opening Postgres session")
	session, err := db.NewPostgresSession(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres session: %w", err)
	}
	Logger.Infof("Opened postgres and applied migrations")

	var dbOperator utils.DBOperator
	switch dataset {
	case "contract_data":
		dbOperator = db.NewContractDataDBOperator(*session)
	case "ttl":
		dbOperator = db.NewTTLDBOperator(*session)
	default:
		return nil, fmt.Errorf("unsupported dataset: %s", dataset)
	}
	postgesAdapter := &utils.PostgresAdapter{DBOperator: dbOperator, Logger: Logger}
	return postgesAdapter, nil
}

func IndexData(config Config) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	var outboundAdapters []utils.OutboundAdapter
	postgresAdapter, err := getPostgresOutputAdapter(ctx, config.Dataset, config.PostgresConfig)
	if err != nil {
		Logger.Fatal(err)
		return
	}
	outboundAdapters = append(outboundAdapters, postgresAdapter)

	// Query the max ledger sequence from postgres
	maxLedgerInDB, err := postgresAdapter.(*utils.PostgresAdapter).GetMaxLedgerSequence(ctx)
	if err != nil {
		Logger.Fatal("Failed to get max ledger sequence from database:", err)
		return
	}

	Logger.Infof("Max ledger sequence in database: %d", maxLedgerInDB)

	// Adjust start and end ledgers based on what's already in the database
	startLedger := config.StartLedger
	endLedger := config.EndLedger

	// If there's data in the database, adjust start ledger
	if maxLedgerInDB > 0 {
		// If end ledger is not provided (unbounded mode)
		if endLedger <= 1 {
			// Start from the next ledger after the max in DB
			startLedger = maxLedgerInDB + 1
			Logger.Infof("Unbounded mode: Starting from ledger %d (max in DB + 1)", startLedger)
		} else {
			// Bounded mode: end ledger is provided
			if maxLedgerInDB >= endLedger {
				// All requested ledgers are already ingested
				Logger.Infof("All ledgers from %d to %d are already ingested (max in DB: %d). Nothing to do.",
					config.StartLedger, endLedger, maxLedgerInDB)
				return
			} else if maxLedgerInDB >= config.StartLedger {
				// Some ledgers are already ingested, start from where we left off
				startLedger = maxLedgerInDB + 1
				Logger.Infof("Bounded mode: Resuming from ledger %d (max in DB + 1) to %d",
					startLedger, endLedger)
			} else {
				// Max in DB is less than requested start, use the requested start
				Logger.Infof("Bounded mode: Starting from requested ledger %d to %d (max in DB %d is before start)",
					startLedger, endLedger, maxLedgerInDB)
			}
		}
	} else {
		Logger.Infof("Database is empty, starting from requested ledger %d", startLedger)
	}

	processor, err := getProcessor(config.Dataset, outboundAdapters, config.StellarCoreConfig.NetworkPassphrase)
	if err != nil {
		Logger.Fatal(err)
		return
	}

	reader, err := input.NewLedgerMetadataReader(
		&config.DataStoreConfig,
		config.StellarCoreConfig.HistoryArchiveUrls,
		[]utils.Processor{processor},
		startLedger,
		endLedger,
	)
	if err != nil {
		Logger.Fatal(err)
		return
	}

	err = reader.Run(ctx, Logger)
	if err != nil {
		Logger.Fatal("ingestion pipeline failed:", err)
	} else {
		Logger.Info("ingestion pipeline ended successfully")
	}

	defer func() {
		for _, adapter := range outboundAdapters {
			adapter.Close()
		}
	}()
}
