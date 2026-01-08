package utils

import (
	"context"

	"github.com/stellar/go/support/db"
	"github.com/stellar/go/support/log"
)

type DataBatchInsertBuilder interface {
	Add(data any) error
	Exec(ctx context.Context) error
	Len() int
	TableName() string
	Close() error
	Reset()
	Session() db.SessionInterface
}

type dataBatchInsertBuilder struct {
	session db.SessionInterface
	builder db.FastBatchInsertBuilder
	table   string
}

type Message struct {
	Payload interface{}
}

type OutboundAdapter interface {
	Write(ctx context.Context, message Message) error
	Close()
}

type PostgresAdapter struct {
	BatchInsertBuilder DataBatchInsertBuilder
	Logger             *log.Entry
}
