package rabbitmq

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/maohieng/pubsub"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrNoProducer    = errors.New("no producer for the topic / queue")
	ErrNoConsumer    = errors.New("no consumer for the topic / queue")
	ErrTopicRequried = errors.New("topic is required")
	ErrDataRequired  = errors.New("data is required")
)

type Config struct {
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       map[string]interface{}
}

type consumerKey struct {
	queueName    string
	consumerName string
}

type rabbitmqClient struct {
	conn *amqp.Connection

	lock      sync.Mutex
	producers map[string]*amqp.Channel
	consumers map[consumerKey]*amqp.Channel
}

func New(amqpURI string) (*rabbitmqClient, error) {
	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		return nil, err
	}

	return &rabbitmqClient{
		conn:      conn,
		producers: make(map[string]*amqp.Channel),
		consumers: make(map[consumerKey]*amqp.Channel),
	}, err
}

func (r *rabbitmqClient) CreateProducer(queueName string, config Config) error {
	chann, err := r.conn.Channel()
	if err != nil {
		return err
	}

	_, err = chann.QueueDeclare(
		queueName,
		config.Durable,    // durable
		config.AutoDelete, // autoDelete
		config.Exclusive,  // exclusive
		config.NoWait,     // noWait
		config.Args,       // args
	)
	if err != nil {
		return err
	}

	r.lock.Lock()
	r.producers[queueName] = chann
	r.lock.Unlock()

	return nil
}

func (r *rabbitmqClient) CreateConsumer(queueName, consumerName string, config Config) error {
	chann, err := r.conn.Channel()
	if err != nil {
		return err
	}

	_, err = chann.QueueDeclare(
		queueName,
		config.Durable,    // durable
		config.AutoDelete, // autoDelete
		config.Exclusive,  // exclusive
		config.NoWait,     // noWait
		config.Args,       // args
	)
	if err != nil {
		return err
	}

	r.lock.Lock()
	r.consumers[consumerKey{queueName: queueName, consumerName: consumerName}] = chann
	r.lock.Unlock()

	return nil
}

func (r *rabbitmqClient) Close() (err error) {
	err = errors.Join(err, r.conn.Close())

	if len(r.producers) > 0 {
		for _, chann := range r.producers {
			err = errors.Join(err, chann.Close())
		}
	}

	if len(r.consumers) > 0 {
		for _, chann := range r.consumers {
			err = errors.Join(err, chann.Close())
		}
	}
	return
}

func (r *rabbitmqClient) Stop() (err error) {
	if len(r.consumers) > 0 {
		for key, chann := range r.consumers {
			err = errors.Join(err, chann.Cancel(key.consumerName, false))
		}
	}

	return
}

func (r *rabbitmqClient) Publish(ctx context.Context, load *pubsub.PubLoad) error {
	if load.Topic == "" {
		return ErrTopicRequried
	}
	if load.Data == nil || len(load.Data) == 0 {
		return ErrDataRequired
	}

	chann, ok := r.producers[load.Topic]
	if !ok {
		return ErrNoProducer
	}

	msg := amqp.Publishing{
		ContentType: "application/json",
		Body:        load.Data,
		Timestamp:   load.Timestamp,
	}
	if load.Headers != nil {
		for k, v := range load.Headers {
			msg.Headers[k] = v
		}
	}

	err := chann.PublishWithContext(
		ctx,
		"",
		load.Topic,
		false,
		false,
		msg,
	)

	return err
}

func (r *rabbitmqClient) Consume(ctx context.Context, load pubsub.ConLoad) (pubsub.Delivery, error) {
	chann, ok := r.consumers[consumerKey{queueName: load.Topic, consumerName: load.ConsumerName}]
	if !ok {
		return nil, ErrNoConsumer
	}

	channelBufSize := 256
	dest := &consumerDelivery{
		message: make(chan *pubsub.ConsumerMessage, channelBufSize),
	}

	// Consume messages from the queue
	deliv, err := chann.Consume(
		load.Topic,
		load.ConsumerName, // consumer
		true,              // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	if err != nil {
		return nil, err
	}

	// Goroutine to pass data from source to destination
	go func(source <-chan amqp.Delivery, dest *consumerDelivery) {
		for {
			select {
			case data, ok := <-source:
				if !ok {
					close(dest.message)
					return
				}

				dest.message <- &pubsub.ConsumerMessage{
					Data:      data.Body,
					Timestamp: data.Timestamp,
				}
			case <-ctx.Done():
				slog.Info("receive exit signal", "pubsub", "rabbitmq", "method", "Consume")
				close(dest.message)
				return
			}
		}
	}(deliv, dest)

	return dest, nil
}

type consumerDelivery struct {
	message chan *pubsub.ConsumerMessage
}

func (cd *consumerDelivery) Messages() <-chan *pubsub.ConsumerMessage {
	return cd.message
}

func (cd *consumerDelivery) Errors() <-chan *pubsub.ConsumerError {
	return nil
}

func (cd *consumerDelivery) Close() error {
	return nil
}
