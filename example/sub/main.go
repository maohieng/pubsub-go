package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/maohieng/pubsub"
	"github.com/maohieng/pubsub/kafka"
	"github.com/maohieng/pubsub/rabbitmq"
)

const (
	consumerName = "example_pubsub_sub"

	brokerUrl1 = "localhost:9092"
	rabbitURI  = "amqp://admin:5b3H9mTVb51C@localhost:9004"

	TopicMediaLog      = "media-logs"
	TopicOrderLog      = "order-logs"
	PartTopicMediaLog1 = 0
	PartitionOrderLog1 = 0

	workerPoolSize = 4

	// This is for last offset message processed
	LastMediaProcessedOffset = 114
	LastOrderProcessedOffset = 9
)

var logger *slog.Logger

func main() {
	logHandler := slog.NewTextHandler(os.Stdout, nil)
	logger = slog.New(logHandler)
	// logger = logger.With("id", consumerName)
	slog.SetDefault(logger)

	// Connect to Kafka
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	kafkaClient := kafka.New([]string{brokerUrl1})
	err := kafkaClient.CreateConsumer(config)
	if err != nil {
		panic(err)
	}
	defer kafkaClient.Close()

	rabbitClient, err := rabbitmq.New(rabbitURI)
	if err != nil {
		panic(err)
	}
	err = rabbitClient.CreateConsumer(TopicMediaLog, consumerName, rabbitmq.Config{})
	if err != nil {
		panic(err)
	}
	defer rabbitClient.Close()

	consumeCtx, consumeCancel := context.WithCancel(context.Background())

	// Multiple consumer in one service
	mediaDelivery, err := kafkaClient.Consume(consumeCtx, pubsub.ConLoad{
		Topic:     TopicMediaLog,
		Partition: PartTopicMediaLog1,
		Offset:    kafka.OffsetOldest,
	})
	if err != nil {
		slog.Error("Failed to consume media logs", "err", err)
	}

	orderDelivery, err := kafkaClient.Consume(consumeCtx, pubsub.ConLoad{
		Topic:     TopicOrderLog,
		Partition: PartitionOrderLog1,
		Offset:    kafka.OffsetOldest,
	})
	if err != nil {
		slog.Error("Failed to consume order logs", "err", err)
	}

	rabbitMediaDelivery, err := rabbitClient.Consume(consumeCtx, pubsub.ConLoad{
		Topic:        TopicMediaLog,
		ConsumerName: consumerName,
	})
	if err != nil {
		slog.Error("Failed to consume media logs from rabbitmq", "err", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < workerPoolSize; i++ {

		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case err := <-mediaDelivery.Errors():
					slog.Info("mediaDelivery error", "err", err)
					return
				case msg := <-mediaDelivery.Messages():
					workMedia(consumeCtx, msg)
				case <-consumeCtx.Done():
					slog.Info("Receive exit signal", "main", "mediaDelivery")
					return
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case err := <-orderDelivery.Errors():
					slog.Info("orderDelivery error", "err", err)
					return
				case msg := <-orderDelivery.Messages():
					workOrder(consumeCtx, msg)
				case <-consumeCtx.Done():
					slog.Info("Receive exit signal", "main", "orderDelivery")
					return
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case err := <-rabbitMediaDelivery.Errors():
					slog.Info("mediaDelivery error", "err", err)
					return
				case msg := <-rabbitMediaDelivery.Messages():
					rabbitWorkMedia(consumeCtx, msg)
				case <-consumeCtx.Done():
					slog.Info("Receive exit signal", "main", "rabbitMediaDelivery")
					return
				}
			}
		}()
	}

	// Wait for signal to stop
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()

	slog.Info("Cleaning up...")

	consumeCancel()

	mediaDelivery.Close()
	orderDelivery.Close()
	rabbitMediaDelivery.Close()

	// Wait for all worker to finish their processing
	wg.Wait()

	kafkaClient.Stop()
	rabbitClient.Stop()
}

func workMedia(_ context.Context, msg *pubsub.ConsumerMessage) {
	if msg == nil {
		return
	}
	if msg.Offset <= LastMediaProcessedOffset {
		slog.Info("Skip media message", "offset", msg.Offset)
		return
	}

	logAttr := slog.Group("work_media", slog.Int64("offset", msg.Offset))

	// select {
	// case <-ctx.Done():
	// 	logger.Info("Exit signal received", logAttr)
	// default:
	slog.Info("Media received", logAttr)
	slog.Info("Media processing...", logAttr)
	time.Sleep(1 * time.Second)
	slog.Info("Media processed!", logAttr)
	// }
}

func workOrder(_ context.Context, msg *pubsub.ConsumerMessage) {
	if msg == nil {
		return
	}

	if msg.Offset <= LastOrderProcessedOffset {
		slog.Info("Skip order message", "offset", msg.Offset)
		return
	}

	logAttr := slog.Group("work_order", slog.Int64("offset", msg.Offset))

	// select {
	// case <-ctx.Done():
	// 	logger.Info("Exit signal received", logAttr)
	// default:
	slog.Info("Order received", logAttr)
	slog.Info("Order processing...", logAttr)
	time.Sleep(1 * time.Second)
	slog.Info("Order processed!", logAttr)
	// }
}

func rabbitWorkMedia(_ context.Context, msg *pubsub.ConsumerMessage) {
	if msg == nil {
		return
	}

	logAttr := slog.Group("work_rabbit_media")

	slog.Info("Rabbit media received", "data", string(msg.Data), logAttr)
	slog.Info("Rabbit media processing...", logAttr)
	time.Sleep(3 * time.Second)
	slog.Info("Rabbit media processed!", logAttr)
}
