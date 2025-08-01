package internal

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
)

type Message struct {
	Payload interface{}
}

type OutboundAdapter interface {
	Write(ctx context.Context, message Message) error
	Close()
}

type DBWritable interface {
	TableName() string
	CreateTableSQL() string
	CreateIndexSQL() string
	InsertSQL() string
	InsertArgs(any) []any
}

func getProcessors(config Config, outboundAdapters []OutboundAdapter) (processors []Processor, err error) {
	switch config.Dataset {
	case "transactions":
		transactionDBOutput, _ := NewTransactionDBOutput()

		connString := fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			config.PostgresConfig.Host,
			config.PostgresConfig.Port,
			config.PostgresConfig.User,
			config.PostgresConfig.Password,
			config.PostgresConfig.Database,
		)

		postgesAdapter, _ := NewPostgresAdapter(connString, transactionDBOutput)

		outboundAdapters = append(outboundAdapters, postgesAdapter)

		newProcessors := []Processor{
			&TransactionProcessor{
				BaseProcessor: BaseProcessor{
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
	var outboundAdapters []OutboundAdapter
	zeroMQOutboundAdapter, err := NewZeroMQOutboundAdapter()
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	outboundAdapters = append(outboundAdapters, zeroMQOutboundAdapter)

	processors, err := getProcessors(config, outboundAdapters)
	if err != nil {
		fmt.Println(err)
		return
	}

	reader, err := NewLedgerMetadataReader(
		&config.DataStoreConfig,
		config.StellarCoreConfig.HistoryArchiveUrls, processors,
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
