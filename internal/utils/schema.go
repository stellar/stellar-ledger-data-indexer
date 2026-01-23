package utils

import (
	"context"

	"github.com/stellar/go/support/log"
)

type DBSession interface {
	UpsertData(ctx context.Context, data any) error
	TableName() string
	Close() error
}

type Message struct {
	Payload interface{}
}

type OutboundAdapter interface {
	Write(ctx context.Context, message Message) error
	Close()
}

type PostgresAdapter struct {
	Session DBSession
	Logger  *log.Entry
}
