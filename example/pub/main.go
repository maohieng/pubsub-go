package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ekhom.com/example/kafka/sub/example/pub/handler"
	"ekhom.com/example/kafka/sub/pubsub"
	"ekhom.com/example/kafka/sub/pubsub/kafka"
	"ekhom.com/example/kafka/sub/pubsub/rabbitmq"
	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

const (
	brokerUrl1 = "localhost:9092"
)

var kafkaClient pubsub.Client
var rabbitClient pubsub.Client

func main() {
	// Setup logger
	slogHandler := slog.NewTextHandler(os.Stdout, nil)

	logger := slog.New(slogHandler)

	// Set the default logger so that we can use elsewhere
	// slog.Info() OR log.Println() in code
	slog.SetDefault(logger)

	// Create a logger for server's ErrorLog from slog
	errorLog := slog.NewLogLogger(slogHandler, slog.LevelError)

	// Setup Gin
	gin.SetMode(gin.ReleaseMode)
	gin.ForceConsoleColor()
	router := gin.New()

	router.Use(loggerMiddlware())
	router.Use(gin.Recovery())

	// Connect to Kafka
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	// Kafka
	client := kafka.New([]string{brokerUrl1})
	err := client.CreateProducer(config)
	if err != nil {
		panic(err)
	}

	kafkaClient = client
	defer kafkaClient.Close()

	// RabbitMQ
	rbbClient, err := rabbitmq.New("amqp://admin:5b3H9mTVb51C@localhost:9004")
	if err != nil {
		panic(err)
	}

	rbbClient.CreateProducer(handler.TopicMediaLog, rabbitmq.Config{})
	rabbitClient = rbbClient
	defer rbbClient.Close()

	router.POST("/order", handler.OrderHandler(kafkaClient))
	router.POST("/media", handler.MediaHandler(kafkaClient, rabbitClient))
	router.POST("all", handler.AllHandler(kafkaClient, rabbitClient))

	server := &http.Server{
		Addr:     ":8101",
		Handler:  router,
		ErrorLog: errorLog,
	}
	log.Println("Starting server on", server.Addr)

	errChan := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	select {
	case <-ctx.Done():
		log.Println("Cleaning up...")

		kafkaClient.Stop()
		rabbitClient.Stop()

		sdCtx, sdCancel := context.WithTimeout(context.Background(), 5*time.Second)
		server.Shutdown(sdCtx)
		sdCancel()
	case err := <-errChan:
		log.Fatalf("Error: %v", err)
	}
}

func loggerMiddlware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Start timer
		start := time.Now()
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery

		// Process request
		c.Next()

		var now = time.Now()
		var latency = now.Sub(start).Milliseconds()
		var lcientIP = c.ClientIP()
		var method = c.Request.Method
		var statusCode = c.Writer.Status()
		var errorMessage = c.Errors.ByType(gin.ErrorTypePrivate).String()

		if raw != "" {
			path = path + "?" + raw
		}

		if errorMessage != "" {
			slog.Error(errorMessage,
				"client_ip", lcientIP,
				"user_agent", c.Request.UserAgent(),
				"method", method,
				"path", path,
				slog.Int("code", statusCode),
				slog.Int64("latency", int64(latency)),
			)
		} else {
			slog.Info("success",
				"client_ip", lcientIP,
				"user_agent", c.Request.UserAgent(),
				"method", method,
				"path", path,
				slog.Int("code", statusCode),
				slog.Int64("latency", int64(latency)),
			)
		}
	}

}
