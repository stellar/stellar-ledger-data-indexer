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

func getPostgresSession(ctx context.Context, postgresConfig PostgresConfig) (*db.DBSession, error) {
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
	return session, nil
}

func getPostgresOutputAdapter(session *db.DBSession, dataset string) (*utils.PostgresAdapter, error) {
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
	var processors []utils.Processor
	// Order is important here, as contract data entries needs to be processed before ttl entries
	// ttl entries are enrichment to base contract data
	datasets := []string{"contract_data", "ttl"}
	session, err := getPostgresSession(ctx, config.PostgresConfig)
	if err != nil {
		Logger.Fatal(err)
		return
	}
	for _, dataset := range datasets {
		postgresAdapter, err := getPostgresOutputAdapter(session, dataset)
		if err != nil {
			Logger.Fatal(err)
			return
		}
		processor, err := getProcessor(dataset, []utils.OutboundAdapter{postgresAdapter}, config.StellarCoreConfig.NetworkPassphrase)
		if err != nil {
			Logger.Fatal(err)
			return
		}

		outboundAdapters = append(outboundAdapters, postgresAdapter)
		processors = append(processors, processor)
	}

	// Ensure adapters are closed on all exit paths
	defer func() {
		for _, adapter := range outboundAdapters {
			adapter.Close()
		}
	}()

	// Query max ledger sequence from database if not in backfill mode
	var maxLedgerInDB uint32
	if config.Backfill {
		maxLedgerInDB = 0
		Logger.Infof("Backfill mode enabled: Using exact start=%d and end=%d ledgers as provided", config.StartLedger, config.EndLedger)
	} else {
		// Both outbound adapters write to the same database, so querying from the first is sufficient
		maxLedgerInDB, err := outboundAdapters[0].GetMaxLedgerSequence(ctx)
		if err != nil {
			Logger.Errorf("Failed to get max ledger sequence from database: %v. Proceeding with requested start ledger.", err)
			maxLedgerInDB = 0
		} else {
			Logger.Infof("Max ledger sequence in database: %d", maxLedgerInDB)
		}
	}

	reader, err := input.NewLedgerMetadataReader(
		&config.DataStoreConfig,
		config.StellarCoreConfig.HistoryArchiveUrls,
		processors,
		config.StartLedger,
		config.EndLedger,
		config.Backfill,
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
