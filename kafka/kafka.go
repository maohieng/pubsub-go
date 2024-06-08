package kafka

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/IBM/sarama"
	"github.com/maohieng/pubsub"
)

const (
	// OffsetNewest stands for the log head offset, i.e. the offset that will be
	// assigned to the next message that will be produced to the partition. You
	// can send this to a client's GetOffset method to get this offset, or when
	// calling ConsumePartition to start consuming new messages.
	OffsetNewest int64 = sarama.OffsetNewest
	// OffsetOldest stands for the oldest offset available on the broker for a
	// partition. You can send this to a client's GetOffset method to get this
	// offset, or when calling ConsumePartition to start consuming from the
	// oldest offset that is still available on the broker.
	OffsetOldest int64 = sarama.OffsetOldest
)

var (
	ErrNotProducer         = errors.New("client is not a producer")
	ErrNotConsumer         = errors.New("client is not a consumer")
	ErrNoPartitionConsumer = errors.New("no partition consumer")
	ErrTopicRequried       = errors.New("topic is required")
	ErrDataRequired        = errors.New("data is required")
)

type kafkaClient struct {
	addrs []string

	consumerConfig, producerConfig *sarama.Config

	producer sarama.SyncProducer

	consumer sarama.Consumer

	lock               sync.RWMutex
	partitionConsumers map[partitionKey]sarama.PartitionConsumer
}

type partitionKey struct {
	topic     string
	partition int32
}

func New(addrs []string) *kafkaClient {
	return &kafkaClient{
		addrs:              addrs,
		partitionConsumers: make(map[partitionKey]sarama.PartitionConsumer),
	}
}

func (k *kafkaClient) CreateProducer(config *sarama.Config) error {
	conn, err := sarama.NewSyncProducer(k.addrs, config)
	if err != nil {
		return err
	}
	k.producer = conn
	k.producerConfig = config
	return nil
}

func (k *kafkaClient) CreateConsumer(config *sarama.Config) error {
	conn, err := sarama.NewConsumer(k.addrs, config)
	if err != nil {
		return err
	}
	k.consumer = conn
	k.consumerConfig = config
	return nil
}

func (k *kafkaClient) Close() (err error) {
	if k.producer != nil {
		err = errors.Join(k.producer.Close())
	}

	return
}

func (k *kafkaClient) Stop() (err error) {
	if k.consumer != nil {
		err = errors.Join(k.consumer.Close())
	}

	return
}

func (k *kafkaClient) Publish(ctx context.Context, load *pubsub.PubLoad) error {
	if k.producer == nil {
		slog.Error("client is not a producer", "pubsub", "kafka", "method", "Publish")
		return ErrNotProducer
	}

	if load.Topic == "" {
		return ErrTopicRequried
	}
	if load.Data == nil || len(load.Data) == 0 {
		return ErrDataRequired
	}

	msg := &sarama.ProducerMessage{
		Topic:     load.Topic,
		Partition: load.Partition,
		Value:     sarama.StringEncoder(load.Data),
		Timestamp: load.Timestamp,
		Offset:    load.Offset,
	}
	if load.Headers != nil {
		msg.Headers = make([]sarama.RecordHeader, len(load.Headers))
		for k, v := range load.Headers {
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   []byte(k),
				Value: v,
			})
		}
	}

	_, _, err := k.producer.SendMessage(msg)

	return err
}

func (k *kafkaClient) addPartitionConsumer(load pubsub.ConLoad) (partitionKey, error) {
	if k.consumer == nil {
		slog.Error("client is not a consumer", "pubsub", "kafka", "method", "Consume")
		return partitionKey{}, ErrNotConsumer
	}

	if load.Offset == 0 {
		load.Offset = OffsetOldest
	}

	k.lock.Lock()
	pc, err := k.consumer.ConsumePartition(load.Topic, load.Partition, load.Offset)
	if err != nil {
		return partitionKey{}, err
	}

	key := partitionKey{
		topic:     load.Topic,
		partition: load.Partition,
	}
	k.partitionConsumers[key] = pc

	k.lock.Unlock()

	return key, nil
}

func (k *kafkaClient) Consume(ctx context.Context, load pubsub.ConLoad) (pubsub.Delivery, error) {
	key, err := k.addPartitionConsumer(load)
	if err != nil {
		return nil, err
	}

	slog.Info("add partition consumer", "pubsub", "kafka", "method", "Consume", "load", load)

	k.lock.RLock()
	src := k.partitionConsumers[key]
	k.lock.RUnlock()

	if src == nil {
		return nil, ErrNoPartitionConsumer
	}

	channelBufSize := 256

	if k.consumerConfig != nil && k.consumerConfig.ChannelBufferSize > 0 {
		channelBufSize = k.consumerConfig.ChannelBufferSize
	}

	delv := &consumerDelivery{
		consumer: src,
		message:  make(chan *pubsub.ConsumerMessage, channelBufSize),
		err:      make(chan *pubsub.ConsumerError, channelBufSize),
	}

	// Goroutine to pass data from source to destination
	go func(source sarama.PartitionConsumer, dest *consumerDelivery) {
		for {
			select {
			case data, ok := <-source.Messages():
				if !ok {
					close(dest.message)
					return
				}

				dest.message <- &pubsub.ConsumerMessage{
					Data:      data.Value,
					Topic:     data.Topic,
					Partition: data.Partition,
					Offset:    data.Offset,
					Timestamp: data.Timestamp,
				}
			case err, ok := <-source.Errors():
				if !ok {
					slog.Info("channel error is not ok", "pubsub", "kafka", "method", "Consume")
					close(dest.err)
					return
				}

				dest.err <- &pubsub.ConsumerError{
					Topic:     err.Topic,
					Partition: err.Partition,
					Err:       err.Err,
				}
			case <-ctx.Done():
				slog.Info("receive exit signal", "pubsub", "kafka", "method", "Consume")
				close(dest.message)
				close(dest.err)
				return
			}
		}
	}(src, delv)

	return delv, nil
}

type consumerDelivery struct {
	consumer sarama.PartitionConsumer
	message  chan *pubsub.ConsumerMessage
	err      chan *pubsub.ConsumerError
}

func (cd *consumerDelivery) Messages() <-chan *pubsub.ConsumerMessage {
	return cd.message
}

func (cd *consumerDelivery) Errors() <-chan *pubsub.ConsumerError {
	return cd.err
}

func (cd *consumerDelivery) Close() error {
	return cd.consumer.Close()
}
