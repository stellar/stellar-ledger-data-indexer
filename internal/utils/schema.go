package utils

import (
	"context"

	"github.com/stellar/go/support/db"
	"github.com/stellar/go/support/log"
)

type DBOperator interface {
	Upsert(ctx context.Context, data any) error
	TableName() string
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
}
