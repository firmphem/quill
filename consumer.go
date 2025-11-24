// Kafka consumer implementation used to read data from Kafka topic and process it

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

// -----------------------------------------------------------------------------
func commitAfterError(consumer *kafka.Consumer, m *kafka.Message, reason string, db *pgxpool.Pool) {
	insertSensorErrorRow(context.Background(), db, "", reason, m.Value,
		int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))

	tp := kafka.TopicPartition{
		Topic:     m.TopicPartition.Topic,
		Partition: m.TopicPartition.Partition,
		Offset:    m.TopicPartition.Offset + 1,
	}
	if _, err := consumer.CommitOffsets([]kafka.TopicPartition{tp}); err != nil {
		log.Error().Err(err).Str("reason", reason).Interface("tp", tp).Msg("Commit failed after error")
	} else {
		offsetsCommittedTotal.Inc()
		log.Debug().Str("reason", reason).Interface("tp", tp).Msg("Committed after error")
	}
}

// ------------------- Kafka Message Processing --------------------------------
func processMessage(
	ctx context.Context,
	m *kafka.Message,
	db *pgxpool.Pool,
	_ *kafka.Producer,
	dlqChan chan<- DLQMessage,
	out chan<- MetricRow,
	consumer *kafka.Consumer,
	_ int,

) {
	messagesReceivedTotal.Inc()

	// 1️⃣ Parse JSON into new KafkaMessage struct
	var km KafkaMessage
	if err := json.Unmarshal(m.Value, &km); err != nil {
		// Insert into sensor_errors
		insertSensorErrorRow(ctx, db, "", "json_unmarshal_failed", m.Value,
			int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))
		log.Info().Msg("Inserted sensor error row reason=json_unmarshal_failed")
		// ✅ Commit offset even for unrecoverable errors
		tp := kafka.TopicPartition{
			Topic:     m.TopicPartition.Topic,
			Partition: m.TopicPartition.Partition,
			Offset:    m.TopicPartition.Offset + 1, // commit next offset
		}
		_, commitErr := consumer.CommitOffsets([]kafka.TopicPartition{tp})
		if commitErr != nil {
			log.Error().Err(commitErr).Msg("Failed to commit offset for JSON error")
		} else {
			log.Info().Msg("Committed offset for JSON error")
		}

		return
	}

	switch strings.ToUpper(km.Topic.Type) {
	case "NBIRTH":
		log.Info().
			Str("edgeNodeId", km.Topic.EdgeNodeID).
			Str("groupId", km.Topic.GroupID).
			Msg("Processing NBIRTH message")

		// Ensure NBIRTH table exists (lazy creation)
		_, err := db.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS nbirth (
				id SERIAL PRIMARY KEY,
				edge_node_id TEXT NOT NULL,
				metric_name TEXT NOT NULL,
				metric_timestamp BIGINT NOT NULL,
				created_at TIMESTAMP DEFAULT now()
			);
		`)
		if err != nil {
			insertSensorErrorRow(ctx, db, "", fmt.Sprintf("nbirth_table_create_failed: %v", err), m.Value,
				int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))
			// Commit on unrecoverable error
			tp := kafka.TopicPartition{Topic: m.TopicPartition.Topic, Partition: m.TopicPartition.Partition, Offset: m.TopicPartition.Offset + 1}
			_, _ = consumer.CommitOffsets([]kafka.TopicPartition{tp})
			return
		}

		// Minimal NBIRTH insertion (name + timestamp + edgeNodeId)
		for _, metric := range km.Payload.Metrics {
			// Ignore extra fields; only store name + timestamp
			if strings.TrimSpace(metric.Name) == "" || metric.Timestamp <= 0 {
				insertSensorErrorRow(ctx, db, "", "nbirth_metric_invalid", m.Value,
					int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))
				// continue to next metric; we still commit the message after the loop
				continue
			}

			_, err := db.Exec(ctx, `
				INSERT INTO nbirth (edge_node_id, metric_name, metric_timestamp)
				VALUES ($1, $2, $3);
			`, km.Topic.EdgeNodeID, metric.Name, metric.Timestamp)
			if err != nil {
				insertSensorErrorRow(ctx, db, "", fmt.Sprintf("nbirth_insert_failed: %v", err), m.Value,
					int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))
				// continue to next metric; we still commit the message after the loop
				continue
			}
		}

		// ✅ Commit after NBIRTH handling
		tp := kafka.TopicPartition{Topic: m.TopicPartition.Topic, Partition: m.TopicPartition.Partition, Offset: m.TopicPartition.Offset + 1}
		_, commitErr := consumer.CommitOffsets([]kafka.TopicPartition{tp})
		if commitErr != nil {
			log.Error().Err(commitErr).Msg("Failed to commit offset for NBIRTH")
		} else {
			log.Info().Msg("Committed offset for NBIRTH")
		}
		return

	case "DBIRTH":
		log.Info().
			Str("edgeNodeId", km.Topic.EdgeNodeID).
			Str("groupId", km.Topic.GroupID).
			Msg("Processing DBIRTH message")

		// Validate presence of deviceId
		if strings.TrimSpace(km.Topic.DeviceID) == "" {
			insertSensorErrorRow(ctx, db, "", "dbirth_missing_device_id", m.Value,
				int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))
			// Commit on unrecoverable error
			tp := kafka.TopicPartition{Topic: m.TopicPartition.Topic, Partition: m.TopicPartition.Partition, Offset: m.TopicPartition.Offset + 1}
			_, _ = consumer.CommitOffsets([]kafka.TopicPartition{tp})
			return
		}

		// Ensure DBIRTH table exists (lazy creation)
		_, err := db.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS dbirth (
				id SERIAL PRIMARY KEY,
				edge_node_id TEXT NOT NULL,
				device_id TEXT NOT NULL,
				metric_name TEXT NOT NULL,
				metric_timestamp BIGINT NOT NULL,
				data_type TEXT NOT NULL,
				created_at TIMESTAMP DEFAULT now()
			);
		`)
		if err != nil {
			insertSensorErrorRow(ctx, db, "", fmt.Sprintf("dbirth_table_create_failed: %v", err), m.Value,
				int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))
			tp := kafka.TopicPartition{Topic: m.TopicPartition.Topic, Partition: m.TopicPartition.Partition, Offset: m.TopicPartition.Offset + 1}
			_, _ = consumer.CommitOffsets([]kafka.TopicPartition{tp})
			return
		}

		// Minimal DBIRTH insertion (name + timestamp + datatype + edgeNodeId + deviceId)
		for _, metric := range km.Payload.Metrics {
			if strings.TrimSpace(metric.Name) == "" || metric.Timestamp <= 0 || strings.TrimSpace(metric.DataType) == "" {
				insertSensorErrorRow(ctx, db, "", "dbirth_metric_invalid", m.Value,
					int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))
				continue
			}

			_, err := db.Exec(ctx, `
				INSERT INTO dbirth (edge_node_id, device_id, metric_name, metric_timestamp, data_type)
				VALUES ($1, $2, $3, $4, $5);
			`, km.Topic.EdgeNodeID, km.Topic.DeviceID, metric.Name, metric.Timestamp, metric.DataType)
			if err != nil {
				insertSensorErrorRow(ctx, db, "", fmt.Sprintf("dbirth_insert_failed: %v", err), m.Value,
					int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))
				continue
			}
		}

		// ✅ Commit after DBIRTH handling
		tp := kafka.TopicPartition{Topic: m.TopicPartition.Topic, Partition: m.TopicPartition.Partition, Offset: m.TopicPartition.Offset + 1}
		_, commitErr := consumer.CommitOffsets([]kafka.TopicPartition{tp})
		if commitErr != nil {
			log.Error().Err(commitErr).Msg("Failed to commit offset for DBIRTH")
		} else {
			log.Info().Msg("Committed offset for DBIRTH")
		}
		return
	} // ⬅️ Close the switch

	// 2️⃣ Lookup or create all reference IDs (cached in memory)
	groupID, err := lookupOrCreateRefID(ctx, db, groupCache, &refMu,
		"group_ref", "group_id", km.Topic.GroupID)
	if err != nil {
		log.Error().Err(err).Msg("Failed to resolve group_ref")
		return
	}

	nodeID, err := lookupOrCreateRefID(ctx, db, nodeCache, &refMu,
		"edge_node", "edge_node_id", km.Topic.EdgeNodeID, "group_id_no", groupID)
	if err != nil {
		log.Error().Err(err).Msg("Failed to resolve edge_node")
		return
	}

	deviceID, err := lookupOrCreateRefID(ctx, db, deviceCache, &refMu,
		"device", "device_id", km.Topic.DeviceID, "node_id_no", nodeID)
	if err != nil {
		log.Error().Err(err).Msg("Failed to resolve device")
		return
	}

	typeID, err := lookupOrCreateRefID(ctx, db, typeCache, &refMu,
		"data_class", "type", km.Topic.Type)
	if err != nil {
		log.Error().Err(err).Msg("Failed to resolve data_class")
		return
	}

	payloadTs := time.UnixMilli(km.Payload.Timestamp)

	// 3️⃣ Process each metric in the payload
	for _, metric := range km.Payload.Metrics {
		// (a) numeric value conversion
		//val, ok := toFloat64(metric.Value)
		var val float64
		var ok bool

		switch v := metric.Value.(type) {
		case bool:
			val = 0.0
			if v {
				val = 1.0
			}
			ok = true
		default:
			val, ok = toFloat64(v)
		}

		if !ok {
			select {
			case dlqChan <- DLQMessage{
				Key:       metric.Name,
				Value:     m.Value,
				Reason:    "non_numeric_value",
				Partition: int(m.TopicPartition.Partition),
				Offset:    int64(m.TopicPartition.Offset),
			}:
			default:
				log.Error().Str("metric", metric.Name).Msg("DLQ channel full — dropped non-numeric metric")
			}
			continue
		}

		// (b) metric name ID lookup

		metricNameID, err := lookupOrCreateMetricName(ctx, db, metricNameCache, &refMu,
			metric.Name, metric.DataType, deviceID)
		if err != nil {
			select {
			case dlqChan <- DLQMessage{
				Key:       metric.Name,
				Value:     m.Value,
				Reason:    fmt.Sprintf("metric_name_lookup_failed: %v", err),
				Partition: int(m.TopicPartition.Partition),
				Offset:    int64(m.TopicPartition.Offset),
			}:
			default:
				log.Error().Str("metric", metric.Name).Msg("DLQ channel full — dropped lookup error")
			}
			continue
		}

		// (c) Build the MetricRow for DB insertion
		row := MetricRow{
			MetricTimestamp:  time.UnixMilli(metric.Timestamp),
			PayloadTimestamp: payloadTs,
			Value:            val,

			MetricNameNo: metricNameID,
			DeviceIDNo:   deviceID,
			NodeIDNo:     nodeID,
			GroupIDNo:    groupID,
			TypeNo:       typeID,

			Partition: int(m.TopicPartition.Partition),
			Offset:    int64(m.TopicPartition.Offset),

			Msg: *m,
		}

		// (d) Send row to batch processor
		select {
		case out <- row:
			rowsEnqueued.Inc()
			rowsReceivedTotal.Inc()
		case <-ctx.Done():
			return
		}
	}

	datapointsReceivedTotal.Add(float64(len(km.Payload.Metrics)))
}
