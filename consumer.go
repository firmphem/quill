// Kafka consumer implementation used to read data from Kafka topic and process it

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

// -----------------------------------------------------------------------------
func (pw *PartitionWorker) closeMsgCh() {
	pw.closeOnce.Do(func() {
		close(pw.msgCh)
	})
}

// -----------------------------------------------------------------------------
func newPartitionWorker(partition int32, topic string, wg *sync.WaitGroup, parentCtx context.Context) *PartitionWorker {
	cfg := currentConfig.Load().(*Config)
	ctx, cancel := context.WithCancel(parentCtx)
	return &PartitionWorker{
		partition: partition,
		topic:     topic,
		msgCh:     make(chan *kafka.Message, cfg.Quill.ChannelSize), // need to tune it on later stage
		wg:        wg,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// -----------------------------------------------------------------------------
func (pw *PartitionWorker) start(db *pgxpool.Pool, ackCh chan<- Ack) {
	pw.wg.Add(1)
	go func() {
		defer pw.wg.Done()
		log.Info().Int32("partition", pw.partition).Msg("worker started")

		for {
			select {
			case <-pw.ctx.Done():
				log.Info().Int32("partition", pw.partition).Msg("ctx cancelled, exiting")
				return
			case m, ok := <-pw.msgCh:
				if !ok {
					log.Info().Int32("partition", pw.partition).Msg("msgCh closed, exiting")
					return
				}

				err := pw.processMessageNew(m, db)
				if err != nil {
					insertSensorErrorRow(pw.ctx, db, err.Error(), m)
					log.Error().
						Int32("partition", m.TopicPartition.Partition).
						Int64("offset", int64(m.TopicPartition.Offset)).
						Err(err).
						Msg("Failed to process kafka payload. See sensor_error table for specific message ")
				}

				// if we are here then we must commit -> send ack
				ackCh <- Ack{
					Partition: m.TopicPartition.Partition,
					Offset:    m.TopicPartition.Offset,
					CommitNow: false,
				}
			}
		}
	}()
}

// -----------------------------------------------------------------------------
func (pw *PartitionWorker) stop() {
	pw.cancel()
}

// -----------------------------------------------------------------------------
func (pw *PartitionWorker) processMessageNew(m *kafka.Message, db *pgxpool.Pool) error {
	// generic logic:
	// - we will process as much metrics as we could;
	// - all fails will be reported as errors;
	// - after the exit from this functtion message will be committed in anyway
	// - we print here only errors which "accumulated", i.e. we continue after them

	var customError error
	messagesReceivedTotal.Inc()

	var km KafkaMessage
	if err := json.Unmarshal(m.Value, &km); err != nil {
		return fmt.Errorf("failed to process payload due to json_unmarshal_failed: %w", err)
	}

	switch strings.ToUpper(km.Topic.Type) {
	case "NBIRTH":
		log.Debug().Str("edgeNodeId", km.Topic.EdgeNodeID).Str("groupId", km.Topic.GroupID).Msg("Processing NBIRTH message")

		// Minimal NBIRTH insertion (name + timestamp + edgeNodeId)
		for _, metric := range km.Payload.Metrics {
			if strings.TrimSpace(metric.Name) == "" || metric.Timestamp <= 0 {
				customError = errors.New("failed to process payload due to nbirth_metric_invalid")
				log.Error().
					Int32("partition", m.TopicPartition.Partition).
					Int64("offset", int64(m.TopicPartition.Offset)).
					Msg("failed to process payload due to nbirth_metric_invalid")
				continue
			}

			_, err := db.Exec(pw.ctx, `
				INSERT INTO nbirth (edge_node_id, metric_name, metric_timestamp)
				VALUES ($1, $2, $3);
			`, km.Topic.EdgeNodeID, metric.Name, metric.Timestamp)
			if err != nil {
				customError = errors.New("failed to process payload due to nbirth_insert_failed")
				log.Error().
					Int32("partition", m.TopicPartition.Partition).
					Int64("offset", int64(m.TopicPartition.Offset)).
					Msg("failed to process payload due to nbirth_insert_failed")
				continue
			}
		}
		return customError

	case "DBIRTH":
		log.Info().Str("edgeNodeId", km.Topic.EdgeNodeID).Str("groupId", km.Topic.GroupID).Msg("Processing DBIRTH message")

		// Validate presence of deviceId
		if strings.TrimSpace(km.Topic.DeviceID) == "" {
			return errors.New("failed to process payload due to dbirth_missing_device_id")
		}

		// Minimal DBIRTH insertion (name + timestamp + datatype + edgeNodeId + deviceId)
		for _, metric := range km.Payload.Metrics {
			if strings.TrimSpace(metric.Name) == "" || metric.Timestamp <= 0 || strings.TrimSpace(metric.DataType) == "" {
				customError = errors.New("failed to process payload due to dbirth_missing_device_id")
				log.Error().
					Int32("partition", m.TopicPartition.Partition).
					Int64("offset", int64(m.TopicPartition.Offset)).
					Msg("failed to process payload due to dbirth_metric_invalid")

				continue
			}

			_, err := db.Exec(pw.ctx, `
				INSERT INTO dbirth (edge_node_id, device_id, metric_name, metric_timestamp, data_type)
				VALUES ($1, $2, $3, $4, $5);
			`, km.Topic.EdgeNodeID, km.Topic.DeviceID, metric.Name, metric.Timestamp, metric.DataType)
			if err != nil {
				customError = errors.New("failed to process payload due to dbirth_missing_device_id")
				log.Error().
					Int32("partition", m.TopicPartition.Partition).
					Int64("offset", int64(m.TopicPartition.Offset)).
					Msg("failed to process payload due to dbirth_insert_failed")
				continue
			}
		}

		return customError
	} // close the switch NBIRTH, DBIRTH

	s := tracker.startStage("enrichment")
	defer s.end()

	// lookups or create all reference IDs (cached in memory)
	groupID, err := lookupOrCreateRefID(pw.ctx, db, groupCache, &refMu, "group_ref", "group_id", km.Topic.GroupID)
	if err != nil {
		return fmt.Errorf("failed to resolve group_ref: %w", err)
	}

	nodeID, err := lookupOrCreateRefID(pw.ctx, db, nodeCache, &refMu, "edge_node", "edge_node_id", km.Topic.EdgeNodeID, "group_id_no", groupID)
	if err != nil {
		return fmt.Errorf("failed to resolve group_ref: %w", err)
	}

	deviceID, err := lookupOrCreateRefID(pw.ctx, db, deviceCache, &refMu, "device", "device_id", km.Topic.DeviceID, "node_id_no", nodeID)
	if err != nil {
		return fmt.Errorf("failed to resolve device: %w", err)
	}

	typeID, err := lookupOrCreateRefID(pw.ctx, db, typeCache, &refMu, "data_class", "type", km.Topic.Type)
	if err != nil {
		return fmt.Errorf("failed to resolve data_class: %w", err)
	}

	// payloadTs := time.UnixMilli(km.Payload.Timestamp)

	// process each metric in the payload
	rows := make([]MetricRow, 0)
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
			customError = fmt.Errorf("failed to process payload due to metric value: %v", metric.Value)
			log.Error().
				Int32("partition", m.TopicPartition.Partition).
				Int64("offset", int64(m.TopicPartition.Offset)).
				Err(customError).
				Msg(customError.Error())
			continue
		}

		//  metric name ID lookup
		metricNameID, err := lookupOrCreateMetricName(pw.ctx, db, metricNameCache, &refMu, metric.Name, metric.DataType, deviceID)
		if err != nil {
			customError = fmt.Errorf("failed to process payload due to failed lookup metricNameID by name: %v", metric.Name)
			log.Error().
				Int32("partition", m.TopicPartition.Partition).
				Int64("offset", int64(m.TopicPartition.Offset)).
				Err(customError).
				Msg(customError.Error())
			continue
		}

		// build the MetricRow for DB insertion
		row := MetricRow{
			MetricTimestamp: time.UnixMilli(metric.Timestamp),
			Value:           val,

			MetricNameNo: metricNameID,
			DeviceIDNo:   deviceID,
			NodeIDNo:     nodeID,
			GroupIDNo:    groupID,
			TypeNo:       typeID,

			Partition: int(m.TopicPartition.Partition),
			Offset:    int64(m.TopicPartition.Offset),

			Msg: *m,
		}

		rows = append(rows, row)
		rowsReceivedTotal.Inc()
	}
	s.end()

	insertErr := pw.insertMetricsToPostgresWithRetries(db, rows, int(pw.partition))
	// log.Info().Int("payload size", len(km.Payload.Metrics)).Msg("metrics size")

	if insertErr != nil {
		customError = fmt.Errorf("insert into the postgres failed: %w", insertErr)
	} else {
		datapointsReceivedTotal.Add(float64(len(km.Payload.Metrics)))
		datapointsReceivedAtomic.Add(uint64(len(km.Payload.Metrics)))
	}

	return customError
}
