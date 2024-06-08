package pubsub

import (
	"context"
	"fmt"
	"time"
)

// PubLoad is used for publishing messages.
// It is required to have a non-empty Topic and Data.
type PubLoad struct {
	// Topic is the topic to publish to. Required.
	Topic     string
	Data      []byte
	Offset    int64
	Partition int32
	Timestamp time.Time

	Headers map[string][]byte
}

// ConLoad is used for consuming messages.
// It is required to have a non-empty Topic.
type ConLoad struct {
	// Topic is the topic to consume from. Required.
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

type Publisher interface {
	// Publish publishes messages to the given topic.
	// load is required to have a non-empty Topic.
	// It safe to call from multiple goroutines.
	Publish(ctx context.Context, load *PubLoad) error
}

type Consumer interface {
	// Consume is goroutine ready to consume messages from the given topic.
	// It must be called at most once per topic (after client created).
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
