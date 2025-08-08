package internal

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/stellar/stellar-ledger-data-indexer/internal/db"
	"github.com/stellar/stellar-ledger-data-indexer/internal/input"
	"github.com/stellar/stellar-ledger-data-indexer/internal/transform"
	"github.com/stellar/stellar-ledger-data-indexer/internal/utils"
)

func getProcessors(ctx context.Context, config Config, outboundAdapters []utils.OutboundAdapter) (processors []utils.Processor, err error) {
	connString := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		config.PostgresConfig.Host,
		config.PostgresConfig.Port,
		config.PostgresConfig.User,
		config.PostgresConfig.Password,
		config.PostgresConfig.Database,
	)
	session, _ := db.NewPostgresSession(ctx, connString)

	switch config.Dataset {
	case "transactions":

		transactionBatchInsertBuilder := session.NewTransactionBatchInsertBuilder()
		postgesAdapter := &utils.PostgresAdapter{BatchInsertBuilder: transactionBatchInsertBuilder}

		outboundAdapters = append(outboundAdapters, postgesAdapter)

		newProcessors := []utils.Processor{
			&transform.TransactionProcessor{
				BaseProcessor: utils.BaseProcessor{
					OutboundAdapters: outboundAdapters,
				},
			},
		}
		return newProcessors, nil
	case "contract_data":
		contractDataBatchInsertBuilder := session.NewContractDataBatchInsertBuilder()
		postgesAdapter := &utils.PostgresAdapter{BatchInsertBuilder: contractDataBatchInsertBuilder}

		outboundAdapters = append(outboundAdapters, postgesAdapter)

		newProcessors := []utils.Processor{
			&transform.ContractDataProcessor{
				BaseProcessor: utils.BaseProcessor{
					OutboundAdapters: outboundAdapters,
				},
			},
		}
		return newProcessors, nil
	default:
		return nil, fmt.Errorf("unsupported dataset: %s", config.Dataset)
	}
}

func IndexData(config Config) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	// Create and register outbound adapters
	var outboundAdapters []utils.OutboundAdapter
	zeroMQOutboundAdapter, err := utils.NewZeroMQOutboundAdapter()
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	outboundAdapters = append(outboundAdapters, zeroMQOutboundAdapter)

	processors, err := getProcessors(ctx, config, outboundAdapters)
	if err != nil {
		fmt.Println(err)
		return
	}

	reader, err := input.NewLedgerMetadataReader(
		&config.DataStoreConfig,
		config.StellarCoreConfig.HistoryArchiveUrls,
		processors,
		config.StartLedger,
		config.EndLedger,
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	log.Printf("ingestion pipeline ended %v\n", reader.Run(ctx))
	for _, adapter := range outboundAdapters {
		adapter.Close()
	}
}
