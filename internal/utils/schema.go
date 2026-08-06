package utils

import (
	"context"
	"time"

	"github.com/stellar/go-stellar-sdk/support/db"
	"github.com/stellar/go-stellar-sdk/support/log"
)

type DBOperator interface {
	Upsert(ctx context.Context, data any) error
	TableName() string
	// DatasetName identifies the logical dataset, which is not always the table:
	// the TTL operator writes into table contract_data while its dataset is ttl.
	// Metrics label by dataset so the two stay distinguishable.
	DatasetName() string
	Session() db.SessionInterface
	GetMaxLedgerSequence(ctx context.Context) (uint32, error)
}

type Message struct {
	Payload interface{}
}

type OutboundAdapter interface {
	Write(ctx context.Context, message Message) error
	Close()
	GetMaxLedgerSequence(ctx context.Context) (uint32, error)
}

type PostgresAdapter struct {
	DBOperator DBOperator
	Logger     *log.Entry
	// MetricRecorder is optional; when nil, dropped rows are logged but not
	// counted.
	MetricRecorder MetricRecorder
	// BaseBackoff overrides the retry backoff unit. Zero means use the default;
	// only tests set it.
	BaseBackoff time.Duration
}
