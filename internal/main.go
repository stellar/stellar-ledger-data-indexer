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

func getPostgresOutputAdapter(ctx context.Context, dataset string, postgresConfig PostgresConfig) (*utils.PostgresAdapter, error) {
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
	postgresAdapter := &utils.PostgresAdapter{DBOperator: dbOperator, Logger: Logger}
	return postgresAdapter, nil
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

	// Ensure adapters are closed on all exit paths
	defer func() {
		for _, adapter := range outboundAdapters {
			adapter.Close()
		}
	}()

	var startLedger, endLedger uint32
	var maxLedgerInDB uint32

	// In backfill mode, respect the exact start and end ledgers provided
	if config.Backfill {
		startLedger = config.StartLedger
		endLedger = config.EndLedger
		maxLedgerInDB = 0 // Don't adjust based on database in backfill mode
		Logger.Infof("Backfill mode enabled: Using exact start=%d and end=%d ledgers as provided", startLedger, endLedger)
	} else {
		// Query the max ledger sequence from postgres
		maxLedgerInDB, err = postgresAdapter.GetMaxLedgerSequence(ctx)
		if err != nil {
			Logger.Fatal("Failed to get max ledger sequence from database:", err)
			return
		}

		Logger.Infof("Max ledger sequence in database: %d", maxLedgerInDB)
		startLedger = config.StartLedger
		endLedger = config.EndLedger
	}

	processor, err := getProcessor(config.Dataset, outboundAdapters, config.StellarCoreConfig.NetworkPassphrase)
	if err != nil {
		Logger.Fatal(err)
		return
	}

	reader, err := input.NewLedgerMetadataReaderWithMaxLedger(
		&config.DataStoreConfig,
		config.StellarCoreConfig.HistoryArchiveUrls,
		[]utils.Processor{processor},
		startLedger,
		endLedger,
		maxLedgerInDB,
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
}
