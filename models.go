// stores different models (except config which is not the model), i.e. structures used by the quill

package main

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ------------------- Models -------------------
type KafkaMessage struct {
	Topic   TopicInfo   `json:"topic"`
	Payload PayloadInfo `json:"payload"`
}

type TopicInfo struct {
	Namespace          string `json:"namespace"`
	EdgeNodeDescriptor string `json:"edgeNodeDescriptor"`
	GroupID            string `json:"groupId"`
	EdgeNodeID         string `json:"edgeNodeId"`
	DeviceID           string `json:"deviceId"`
	Type               string `json:"type"`
}

type PayloadInfo struct {
	Timestamp int64    `json:"timestamp"`
	Metrics   []Metric `json:"metrics"`
	Seq       int      `json:"seq"`
}

type DLQMessage struct {
	Key       string
	Value     []byte
	Reason    string
	Partition int
	Offset    int64
}

type Metric struct {
	Name      string      `json:"name"`
	Timestamp int64       `json:"timestamp"`
	DataType  string      `json:"dataType"`
	Value     interface{} `json:"value"`
}

// Represents one row ready to insert into the `metric` table.
type MetricRow struct {
	MetricTimestamp  time.Time // derived from metric.timestamp
	PayloadTimestamp time.Time // derived from payload.timestamp
	//DataType         string    // "Float", "Int32", etc.
	Value float64

	// Foreign-key integer references (no actual DB FKs for speed)
	MetricNameNo int
	DeviceIDNo   int
	NodeIDNo     int
	GroupIDNo    int
	TypeNo       int

	// Kafka tracking
	Partition int
	Offset    int64
	Msg       kafka.Message // ✅ keep the original Kafka message
	DedupKey  string
}

// NBIRTH: node-level, no deviceId
type NBirthMessage struct {
	Topic struct {
		Namespace  string `json:"namespace"`
		GroupID    string `json:"groupId"`
		EdgeNodeID string `json:"edgeNodeId"`
		Type       string `json:"type"` // always "NBIRTH"
	} `json:"topic"`
	Payload struct {
		Timestamp int64    `json:"timestamp"`
		Metrics   []Metric `json:"metrics"`
		Seq       int      `json:"seq"`
	} `json:"payload"`
}

// DBIRTH: device-level, requires deviceId
type DBirthMessage struct {
	Topic struct {
		Namespace  string `json:"namespace"`
		GroupID    string `json:"groupId"`
		EdgeNodeID string `json:"edgeNodeId"`
		DeviceID   string `json:"deviceId"`
		Type       string `json:"type"` // always "DBIRTH"
	} `json:"topic"`
	Payload struct {
		Timestamp int64    `json:"timestamp"`
		Metrics   []Metric `json:"metrics"`
		Seq       int      `json:"seq"`
	} `json:"payload"`
}
