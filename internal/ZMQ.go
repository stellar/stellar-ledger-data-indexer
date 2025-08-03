package internal

import (
	"context"
	"encoding/json"
	"log"

	"github.com/zeromq/goczmq"
)

type ZeroMQOutboundAdapter struct {
	Publisher *goczmq.Sock
}

func NewZeroMQOutboundAdapter() (*ZeroMQOutboundAdapter, error) {
	adapter := &ZeroMQOutboundAdapter{}
	var err error
	adapter.Publisher, err = goczmq.NewPub("tcp://127.0.0.1:5555")
	if err != nil {
		log.Printf("error creating 0MQ publisher: %v\n", err)
		return nil, err
	}
	return adapter, nil
}

func (z *ZeroMQOutboundAdapter) Close() {
	z.Publisher.Destroy()
}

func (z *ZeroMQOutboundAdapter) Write(ctx context.Context, msg Message) error {
	outputJSON, err := json.Marshal(msg.Payload)
	if err != nil {
		return err
	}
	_, err = z.Publisher.Write(outputJSON)
	return err
}
