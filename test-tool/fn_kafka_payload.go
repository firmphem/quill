package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Metric struct {
	Name      string      `json:"name"`
	Timestamp int64       `json:"timestamp"`
	DataType  string      `json:"dataType"`
	Value     interface{} `json:"value"`
}

type Payload struct {
	Timestamp int64    `json:"timestamp"`
	Metrics   []Metric `json:"metrics"`
	Seq       int      `json:"seq"`
}

type Topic struct {
	Namespace          string `json:"namespace"`
	EdgeNodeDescriptor string `json:"edgeNodeDescriptor"`
	GroupID            string `json:"groupId"`
	EdgeNodeID         string `json:"edgeNodeId"`
	DeviceID           string `json:"deviceId"`
	Type               string `json:"type"`
}

type KafkaMessage struct {
	Topic   Topic   `json:"topic"`
	Payload Payload `json:"payload"`
}

var globalKafkaPayloadSeq int = 1

// -----------------------------------------------------------------------------
//
// todo:
// - probably need to split generating valid and invalid messages and have different types on invalidd msgs
func fnProducePayload(numPayloads int, numMetrics int, partition int, brokenPayloads bool, brokenMetrics bool) (any, error) {
	if len(cfg.Kafka.Brokers) == 0 {
		return nil, fmt.Errorf("no kafka brokers configured")
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Kafka.Brokers[0],

		"compression.type":           "gzip",
		"message.max.bytes":          400000000,
		"batch.size":                 400000000,
		"socket.keepalive.enable":    true,
		"queue.buffering.max.kbytes": 4000000,
		"socket.timeout.ms":          300000,
	})
	if err != nil {
		return nil, fmt.Errorf("cannot create kafka producer: %w", err)
	}
	defer producer.Close()

	for i := 0; i < numPayloads; i++ {
		var raw []byte

		if brokenPayloads {
			msg := `{"f1": "", "f2": 0}`
			raw, err = json.Marshal(msg)
		} else {
			msg := KafkaMessage{
				Topic: Topic{
					// hardcoding for easy checks
					Namespace:          "namespace",
					EdgeNodeDescriptor: "EdgeNodeDescriptor",
					GroupID:            "group_id",
					EdgeNodeID:         "edge_id",
					DeviceID:           "device_id",
					Type:               "13",
				},
				Payload: Payload{
					Timestamp: time.Now().UnixMilli(),
					Seq:       nextSeq(),
					Metrics:   buildMetrics(numMetrics, brokenMetrics),
				},
			}

			raw, err = json.Marshal(msg)
		}

		if err != nil {
			return nil, fmt.Errorf("cannot marshal kafka payload: %w", err)
		}

		record := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &cfg.Kafka.Topic,
				Partition: int32(partition),
			},
			Value: raw,
		}

		if partition < 0 {
			record.TopicPartition.Partition = kafka.PartitionAny
		}

		if err := producer.Produce(record, nil); err != nil {
			return nil, fmt.Errorf("produce failed: %w", err)
		}

		e := <-producer.Events()
		m, ok := e.(*kafka.Message)
		if !ok {
			return nil, fmt.Errorf("failed to read delivery report event")
		}
		if m.TopicPartition.Error != nil {
			return nil, fmt.Errorf("delivery error: %w", m.TopicPartition.Error)
		}
	}

	return nil, nil
}

// -----------------------------------------------------------------------------
func nextSeq() int {
	v := globalKafkaPayloadSeq
	globalKafkaPayloadSeq++
	return v
}

// -----------------------------------------------------------------------------
func buildMetrics(n int, brokenMetrics bool) []Metric {
	results := []Metric{}
	now := time.Now().UnixMilli()

	// valid metrics (int, float, bool)
	results = append(results,
		Metric{
			Name:      "valid-int-1",
			DataType:  "int",
			Timestamp: now,
			Value:     123,
		},
		Metric{
			Name:      "valid-float-2",
			DataType:  "float",
			Timestamp: now,
			Value:     123.456,
		},
		Metric{
			Name:      "valid-bool-true-3",
			DataType:  "bool",
			Timestamp: now,
			Value:     true,
		},
		Metric{
			Name:      "valid-bool-false-4",
			DataType:  "bool",
			Timestamp: now,
			Value:     false,
		},
	)

	if brokenMetrics {
		results = append(results,
			Metric{
				Name:      "invalid-string-1",
				DataType:  "string",
				Timestamp: now,
				Value:     "not-a-number",
			},
			Metric{
				Name:      "invalid-datetime-2",
				DataType:  "datetime",
				Timestamp: now,
				Value:     "2025-01-01T12:34:56Z",
			},
		)
	}

	value := len(results)
	for len(results) < n {
		results = append(results, Metric{
			Name:      fmt.Sprintf("valid-int-%d", value),
			DataType:  "int",
			Timestamp: now,
			Value:     value,
		})
		value++
	}

	return results
}
