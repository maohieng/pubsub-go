package handler

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/gin-gonic/gin"
	"github.com/maohieng/pubsub"
)

const (
	TopicMediaLog      = "media-logs"
	TopicOrderLog      = "order-logs"
	PartTopicMediaLog1 = 0
	PartitionOrderLog1 = 0
)

type orderStatus struct {
	OrderId string `json:"orderId"`
	Status  string `json:"status"`
}

func OrderHandler(pubsubClient pubsub.Client) gin.HandlerFunc {
	return func(c *gin.Context) {

		var order orderStatus
		c.Bind(&order)

		// Send message to Kafka
		err := publishOrder(c.Request.Context(), pubsubClient, order)

		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{
			"message": "Message sent to Kafka",
			"err":     err,
		})
	}
}

func publishOrder(ctx context.Context, pubsubClient pubsub.Client, order orderStatus) error {
	data, _ := json.Marshal(order)

	// Send message to Kafka
	return pubsubClient.Publish(ctx, &pubsub.PubLoad{
		Topic:     TopicOrderLog,
		Data:      data,
		Partition: PartitionOrderLog1,
	})
}

type mediaStatus struct {
	MediaId string `json:"mediaId"`
	Status  string `json:"status"`
}

func MediaHandler(kafkaClient, rabbitClient pubsub.Client) gin.HandlerFunc {
	return func(c *gin.Context) {

		var media mediaStatus
		c.Bind(&media)

		// Send message to Kafka
		err := publishMedia(c.Request.Context(), kafkaClient, rabbitClient, media)

		c.JSON(200, gin.H{
			"message": "Message are sent",
			"err":     err,
		})
	}
}

func publishMedia(ctx context.Context, kafkaClient, rabbitClient pubsub.Client, media mediaStatus) (err error) {
	data, _ := json.Marshal(media)

	if kafkaClient != nil {
		err1 := kafkaClient.Publish(ctx, &pubsub.PubLoad{
			Topic:     TopicMediaLog,
			Data:      data,
			Partition: PartTopicMediaLog1,
		})

		err = errors.Join(err, err1)
	}

	if rabbitClient != nil {
		err2 := rabbitClient.Publish(ctx, &pubsub.PubLoad{
			Topic: TopicMediaLog,
			Data:  data,
		})

		err = errors.Join(err, err2)
	}

	return
}

func AllHandler(kafkaClient, rabbitClient pubsub.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		type allStatus struct {
			Media mediaStatus `json:"media"`
			Order orderStatus `json:"order"`
		}

		var status allStatus
		c.Bind(&status)

		// Send message to Kafka
		mErr := publishMedia(c.Request.Context(), kafkaClient, rabbitClient, status.Media)
		oErr := publishOrder(c.Request.Context(), kafkaClient, status.Order)

		var allErr error = errors.Join(mErr, oErr)

		c.JSON(200, gin.H{
			"message": "Message sent to pubsub",
			"errors":  allErr,
		})
	}
}
