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
}

type Message struct {
	Payload interface{}
}

type OutboundAdapter interface {
	Write(ctx context.Context, message Message) error
	Close()
}

type PostgresAdapter struct {
	DBOperator DBOperator
	Logger     *log.Entry
}
