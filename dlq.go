// DLQ (dead letter queue) implementation for messages which can't be parsed by kafka consumer

package main

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

// asyncDLQWriter runs as a background goroutine and writes messages to DLQ.
func asyncDLQWriter(ctx context.Context, producer *kafka.Producer, db *pgxpool.Pool, dlqChan <-chan DLQMessage) {
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("DLQ writer shutting down")
			return

		case msg := <-dlqChan:
			// ✅ Send to Kafka DLQ topic
			cfg := currentConfig.Load().(*Config)

			err := producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &cfg.DLQ.Topic,
					Partition: kafka.PartitionAny,
				},
				Key:   []byte(msg.Key),
				Value: msg.Value,
				Headers: []kafka.Header{
					{Key: "error", Value: []byte(msg.Reason)},
					{Key: "ts", Value: []byte(time.Now().Format(time.RFC3339))},
				},
			}, nil)

			if err != nil {
				log.Error().
					Err(err).
					Str("key", msg.Key).
					Msg("Failed to write DLQ message to Kafka")
			}

			// ✅ Always insert into DB as a fallback
			insertSensorErrorRow(ctx, db, msg.Key, msg.Reason, msg.Value, msg.Partition, msg.Offset)
		}
	}
}
