package internal

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/stellar/go/network"
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
	transactionDBOutput, err := NewTransactionDBOutput()
	connString := "host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable"
	postgesAdapter, err := NewPostgresAdapter(connString, transactionDBOutput)

	outboundAdapters = append(outboundAdapters, postgesAdapter)

	processors := []Processor{&processor{
		outboundAdapters: outboundAdapters,
	}}

	reader, err := NewLedgerMetadataReader(
		&config.DataStoreConfig,
		network.PublicNetworkhistoryArchiveURLs, processors,
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
