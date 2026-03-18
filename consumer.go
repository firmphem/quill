package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

var pollingPaused atomic.Bool

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
		msgCh:     make(chan *kafka.Message, cfg.Quill.ChannelSize),
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
		defer log.Info().Int32("partition", pw.partition).Msg("worker exit")

		firstMessageReceived := false

		for {
			select {
			case m, ok := <-pw.msgCh:
				if !ok {
					log.Info().Int32("partition", pw.partition).Msg("msgCh closed, exiting")
					return
				}

				if !firstMessageReceived {
					log.Info().
						Int32("partition", m.TopicPartition.Partition).
						Int64("offset", int64(m.TopicPartition.Offset)).
						Msg("first kafka message received")
					firstMessageReceived = true
				}

				err := pw.processMessageNew(m, db)

				if err != nil {
					retryInsertSensorErrorRow(pw.ctx, db, err.Error(), m)
					log.Error().
						Int32("partition", m.TopicPartition.Partition).
						Int64("offset", int64(m.TopicPartition.Offset)).
						Err(err).
						Msg("Failed to process kafka payload")
				}

				ackCh <- Ack{
					Partition: m.TopicPartition.Partition,
					Offset:    m.TopicPartition.Offset,
					CommitNow: false,
				}

			case <-pw.ctx.Done():
				for {
					select {
					case m, ok := <-pw.msgCh:
						if !ok {
							log.Info().Int32("partition", pw.partition).Msg("worker drained")
							return
						}

						err := pw.processMessageNew(m, db)

						if err != nil {
							retryInsertSensorErrorRow(pw.ctx, db, err.Error(), m)
						}

						ackCh <- Ack{
							Partition: m.TopicPartition.Partition,
							Offset:    m.TopicPartition.Offset,
						}

					default:
						log.Info().Int32("partition", pw.partition).Msg("worker drained")
						return
					}
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
	if len(m.Value) == 0 {
		log.Trace().
			Int32("partition", m.TopicPartition.Partition).
			Int64("offset", int64(m.TopicPartition.Offset)).
			Msg("got empty payload. will commit it")
		return nil
	}

	var customError error

	var km KafkaMessage
	if err := json.Unmarshal(m.Value, &km); err != nil {
		return fmt.Errorf("failed to process payload due to json_unmarshal_failed: %w", err)
	}

	partitionLabel := strconv.FormatInt(int64(m.TopicPartition.Partition), 10)
	if km.Payload.Timestamp > 0 {
		lastMessageTimestampPerPartition.WithLabelValues(partitionLabel).Set(float64(km.Payload.Timestamp) / 1000)
	} else {
		log.Warn().Str("partition", partitionLabel).Msg("skipping staleness update: payload timestamp is zero or missing")
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

	// process each metric in the payload
	rows := make([]MetricRow, 0)
	for _, metric := range km.Payload.Metrics {
		// (a) numeric value conversion
		//val, ok := toFloat64(metric.Value)
		valF, valI, ok := parseMetricValue(metric.Value)

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
			ValueF:          valF,
			ValueI:          valI,

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
	}
	s.end()
	dpCountPerMessageHistogram.WithLabelValues().Observe(float64(len(km.Payload.Metrics)))

	insertErr := pw.insertMetricsToPostgresWithRetries(db, rows, int(pw.partition))

	if insertErr != nil {
		customError = fmt.Errorf("insert into the postgres failed: %w", insertErr)
	} else {
		messagesSavedToDBTotal.WithLabelValues().Inc()
		dpWrittenIntoDBTotal.WithLabelValues().Add(float64(len(km.Payload.Metrics)))
		datapointsReceivedAtomic.Add(uint64(len(km.Payload.Metrics)))
	}

	return customError
}

// -----------------------------------------------------------------------------
func runConsumer(globalCtx context.Context, cfg *Config, dbPool *pgxpool.Pool) {
	mainConsumer := initKafkaConsumer(cfg)

	workers := make(map[int32]*PartitionWorker)
	partitions := make(map[int32]*partitionState)

	var workersMu sync.Mutex
	var partitionsMu sync.Mutex
	var workerWG sync.WaitGroup

	ackCh := make(chan Ack, cfg.Quill.ChannelSize)
	go monitorChannel(globalCtx, "ack", ackCh)

	topic := cfg.Kafka.Topic

	ensureWorker := func(partition int32) {
		workersMu.Lock()
		defer workersMu.Unlock()

		if _, ok := workers[partition]; ok {
			return
		}

		w := newPartitionWorker(partition, topic, &workerWG, globalCtx)
		workers[partition] = w
		partitionsMu.Lock()

		if _, ok := partitions[partition]; !ok {
			partitions[partition] = &partitionState{
				lastProcessed: -1,
				lastCommitted: -1,
			}
		}

		partitionsMu.Unlock()
		w.start(dbPool, ackCh)
	}

	err := mainConsumer.Subscribe(topic, func(consumer *kafka.Consumer, ev kafka.Event) error {
		switch e := ev.(type) {
		case kafka.AssignedPartitions:
			log.Info().Interface("partitions", e.Partitions).Msg("assigned")
			if err := consumer.Assign(e.Partitions); err != nil {
				return err
			}

			for _, tp := range e.Partitions {
				ensureWorker(tp.Partition)
			}
			pollingPaused.Store(false)

			return nil

		case kafka.RevokedPartitions:
			groupConsumerRebalanceTotal.WithLabelValues().Inc()

			log.Info().Msg("revoking partitions")
			pollingPaused.Store(true)
			workersMu.Lock()

			for _, tp := range e.Partitions {
				if w, ok := workers[tp.Partition]; ok {
					w.stop()
				}
			}

			workersMu.Unlock()
			workerWG.Wait()

			partitionsMu.Lock()

			var offsets []kafka.TopicPartition

			for _, tp := range e.Partitions {
				ps := partitions[tp.Partition]
				if ps != nil && ps.lastProcessed >= 0 {
					offsets = append(offsets, kafka.TopicPartition{
						Topic:     &topic,
						Partition: tp.Partition,
						Offset:    ps.lastProcessed + 1,
					})
				}
			}

			partitionsMu.Unlock()

			if len(offsets) > 0 {
				log.Info().Interface("offsets", offsets).Msg("committing final offsets")
				err := retryUntilDone(
					globalCtx,
					"commit_offset_on_rebalance",
					func(ctx context.Context) error {
						_, err := consumer.CommitOffsets(offsets)
						incKafkaOffsetCommitMetric(err)
						return err
					},
					isRetriableErr,
					"failure",
					func(finalErr error) error {
						return finalErr
					},
				)
				// _, err := consumer.CommitOffsets(offsets)
				if err != nil {
					log.Error().Err(err).Msg("commit on revoke failed")
				}
			}

			// cleanup workers
			workersMu.Lock()

			for _, tp := range e.Partitions {
				if w, ok := workers[tp.Partition]; ok {
					w.closeMsgCh()

					delete(workers, tp.Partition)
				}

				partitionsMu.Lock()
				delete(partitions, tp.Partition)
				partitionsMu.Unlock()
			}

			workersMu.Unlock()

			return consumer.Unassign()
		}

		return nil
	})

	if err != nil {
		log.Fatal().Err(err).Msg("subscribe failed")
	}

	go func() {
		for {
			if pollingPaused.Load() {
				time.Sleep(10 * time.Millisecond)
				continue
			}

			ev := mainConsumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				p := e.TopicPartition.Partition
				ensureWorker(p)
				workersMu.Lock()
				w := workers[p]
				workersMu.Unlock()

				if w == nil {
					log.Fatal().Int32("partition", p).Msg("worker missing")
				}

				delivered := false
				for !delivered {
					select {
					case w.msgCh <- e:
						delivered = true
					case <-globalCtx.Done():
						return
					default:
						time.Sleep(time.Millisecond)
					}
				}

			case kafka.Error:
				if e.IsFatal() {
					log.Fatal().Err(e).Msg("kafka fatal error")
				}
				kafkaErrorsByErrorTotal.WithLabelValues(e.Error()).Inc()
				log.Warn().Err(e).Msg("kafka transient error")
			}
		}
	}()

	commitDone := make(chan struct{})
	go commitManager(globalCtx, &partitionsMu, partitions, topic, mainConsumer, ackCh, commitDone)

	<-globalCtx.Done()

	log.Info().Msg("consumer shutting down")

	mainConsumer.Close()

	<-commitDone
}

// -----------------------------------------------------------------------------
func parseMetricValue(v interface{}) (*float64, *int, bool) {
	if v == nil {
		return nil, nil, false
	}

	if b, ok := v.(bool); ok {
		var i int
		if b {
			i = 1
		}
		return nil, &i, true
	}

	f, ok := v.(float64)
	if !ok {
		return nil, nil, false
	}

	if f == math.Trunc(f) {
		i := int(f)
		return nil, &i, true
	}

	return &f, nil, true
}
