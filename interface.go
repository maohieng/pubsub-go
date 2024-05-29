package pubsub

import (
	"context"
	"fmt"
	"time"
)

type PubLoad struct {
	Topic     string
	Data      []byte
	Offset    int64
	Partition int32
	Timestamp time.Time

	Headers map[string][]byte
}

type ConLoad struct {
	Topic     string
	Partition int32
	// Offset is the initial offset to start consuming from. Default is [OffsetOldest].
	Offset       int64
	ConsumerName string
}

type ConsumerMessage struct {
	Data []byte

	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
}

// ConsumerError is what is provided to the user when an error occurs.
// It wraps an error and includes the topic and partition.
type ConsumerError struct {
	Topic     string
	Partition int32
	Err       error
}

func (ce ConsumerError) Error() string {
	return fmt.Sprintf("pubcon: error while consuming %s/%d: %s", ce.Topic, ce.Partition, ce.Err)
}

func (ce ConsumerError) Unwrap() error {
	return ce.Err
}

type Delivery interface {
	// Messages returns the read channel for the messages that are returned by
	// the broker.
	Messages() <-chan *ConsumerMessage

	// Errors returns a read channel of errors that occurred during consuming, if
	// enabled. By default, errors are logged and not returned over this channel.
	// If you want to implement any custom error handling, set your config's
	// Consumer.Return.Errors setting to true, and read from this channel.
	Errors() <-chan *ConsumerError

	Close() error
}

type Publisher interface {
	Publish(ctx context.Context, load *PubLoad) error
}

type Consumer interface {
	// Consume creates a new consumer on the given topic/partition.
	Consume(ctx context.Context, load ConLoad) (Delivery, error)
}

type Client interface {
	Publisher
	Consumer

	// Close closes the connection and cleans up resources.
	Close() error

	// Stop stops receiving messages.
	// It must be called before Close.
	Stop() error
}
