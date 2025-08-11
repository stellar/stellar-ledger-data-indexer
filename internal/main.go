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

func postgresConnString(cfg PostgresConfig) string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database,
	)
}

func getProcessor(dataset string, outboundAdapters []utils.OutboundAdapter) (processor utils.Processor, err error) {
	switch dataset {
	case "transactions":
		newProcessor := &transform.TransactionProcessor{
			BaseProcessor: utils.BaseProcessor{
				OutboundAdapters: outboundAdapters,
				Logger:           Logger,
			},
		}
		return newProcessor, nil
	case "contract_data":
		newProcessor := &transform.ContractDataProcessor{
			BaseProcessor: utils.BaseProcessor{
				OutboundAdapters: outboundAdapters,
				Logger:           Logger,
			},
		}
		return newProcessor, nil
	default:
		return nil, fmt.Errorf("unsupported dataset: %s", dataset)
	}
}

func getPostgresOutputAdapter(ctx context.Context, dataset string, postgresConfig PostgresConfig) (outboundAdapter utils.OutboundAdapter, err error) {
	connString := postgresConnString(postgresConfig)

	Logger.Infof("Opening Postgres session")
	session, err := db.NewPostgresSession(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres session: %w", err)
	}
	Logger.Infof("Opened postgres and applied migrations")

	var batchInsertBuilder utils.DataBatchInsertBuilder
	switch dataset {
	case "transactions":
		batchInsertBuilder = session.NewTransactionBatchInsertBuilder()
	case "contract_data":
		batchInsertBuilder = session.NewContractDataBatchInsertBuilder()
	default:
		return nil, fmt.Errorf("unsupported dataset: %s", dataset)
	}
	postgesAdapter := &utils.PostgresAdapter{BatchInsertBuilder: batchInsertBuilder, Logger: Logger}
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

	processor, err := getProcessor(config.Dataset, outboundAdapters)
	if err != nil {
		Logger.Fatal(err)
		return
	}

	reader, err := input.NewLedgerMetadataReader(
		&config.DataStoreConfig,
		config.StellarCoreConfig.HistoryArchiveUrls,
		[]utils.Processor{processor},
		config.StartLedger,
		config.EndLedger,
	)
	if err != nil {
		Logger.Fatal(err)
		return
	}

	err = reader.Run(ctx, Logger)
	if err != nil {
		Logger.Error("ingestion pipeline failed:", err)
	} else {
		Logger.Info("ingestion pipeline ended successfully")
	}

	defer func() {
		for _, adapter := range outboundAdapters {
			adapter.Close()
		}
	}()
}
