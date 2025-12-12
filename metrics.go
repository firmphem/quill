package main

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

// insertSensorErrorRow inserts a malformed or rejected Kafka message into the sensor_errors table.
// raw_payload is stored as BYTEA, so it can contain invalid UTF-8 or malformed JSON safely.
func insertSensorErrorRow(
	ctx context.Context,
	db *pgxpool.Pool,
	reason string,
	m *kafka.Message,
) {
	rawValue := m.Value
	partition := m.TopicPartition.Partition
	offset := int64(m.TopicPartition.Offset)

	log.Error().Int32("partition", partition).Int64("offset", offset).Msg("going to update update sensor_errors table")

	if rawValue == nil {
		rawValue = []byte("{}") // minimal placeholder
	}

	_, err := db.Exec(ctx, `
		INSERT INTO sensor_errors (created_at, sensor_name, reason, raw_payload, partition, kafka_offset)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, time.Now(), "n/a", reason, rawValue, partition, offset)

	if err != nil {
		log.Error().Int32("partition", partition).Int64("offset", offset).Err(err).Msg("Failed to insert payload into sensor_error")
		// TODO: dump kafkaMessage into a file in the special folder.
	}
}

// func insertBatchFallback(ctx context.Context, db *pgxpool.Pool, batch []MetricRow, workerID int) error {
// 	if len(batch) == 0 {
// 		return nil
// 	}
//
// 	// Prepare insert SQL for metric table
// 	sql := `
// 		INSERT INTO metric_unclogged (
// 			metric_timestamp,
// 			payload_timestamp,
// 			data_type,
// 			value,
// 			metric_name_no,
// 			device_id_no,
// 			node_id_no,
// 			group_id_no,
// 			type_no
// 		)
// 		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
// 		ON CONFLICT DO NOTHING;
// 	`
//
// 	var inserted int
// 	for _, r := range batch {
// 		_, err := db.Exec(
// 			ctx,
// 			sql,
// 			r.MetricTimestamp.UnixMilli(),
// 			//r.PayloadTimestamp.UnixMilli(),
// 			r.Value,
// 			r.MetricNameNo,
// 			r.DeviceIDNo,
// 			r.NodeIDNo,
// 			r.GroupIDNo,
// 			r.TypeNo,
// 		)
//
// 		if err != nil {
// 			log.Error().
// 				Err(err).
// 				Int("worker", workerID).
// 				Int("metric_name_no", r.MetricNameNo).
// 				Msg("Failed to insert row in fallback mode")
// 			continue
// 		}
// 		inserted++
// 	}
//
// 	log.Warn().
// 		Int("worker", workerID).
// 		Int("inserted", inserted).
// 		Int("skipped", len(batch)-inserted).
// 		Msg("Fallback insert completed with ON CONFLICT DO NOTHING")
//
// 	rowsInsertedTotal.Add(float64(inserted))
// 	return nil
// }

// retryInsertBatch tries to insert a batch with exponential backoff.
// it retries only on transient errors (like network or timeout issues).
// func retryInsertBatch(ctx context.Context, db *pgxpool.Pool, batch []MetricRow, workerID int) error {
// 	const maxRetries = 3
// 	baseDelay := time.Second
// 	var lastErr error
//
// 	for attempt := 1; attempt <= maxRetries; attempt++ {
// 		lastErr = insertBatchFn(ctx, db, batch, workerID)
// 		if lastErr == nil {
// 			return nil
// 		}
//
// 		// check if it's a Postgres error
// 		var pgErr *pgconn.PgError
// 		isTransient := false
//
// 		if errors.As(lastErr, &pgErr) {
// 			code := pgErr.Code
//
// 			switch {
// 			// connection-level errors
// 			case strings.HasPrefix(code, "08"):
// 				isTransient = true
// 			// serialization failure or deadlock
// 			case code == "40001" || code == "40P01":
// 				isTransient = true
// 			// timeout
// 			case code == "57014":
// 				isTransient = true
// 			default:
// 				isTransient = false
// 			}
// 		} else {
// 			// fallback: look for common transient patterns in plain errors
// 			msg := lastErr.Error()
// 			isTransient = strings.Contains(msg, "timeout") ||
// 				strings.Contains(msg, "connection reset") ||
// 				strings.Contains(msg, "broken pipe")
// 		}
//
// 		if !isTransient {
// 			return lastErr
// 		}
//
// 		// log and backoff
// 		wait := baseDelay * time.Duration(1<<(attempt-1))
// 		log.Warn().
// 			Err(lastErr).
// 			Str("sqlstate", pgErr.Code).
// 			Int("worker", workerID).
// 			Int("attempt", attempt).
// 			Dur("backoff", wait).
// 			Msg("Transient DB error, retrying batch")
//
// 		select {
// 		case <-ctx.Done():
// 			return ctx.Err()
// 		case <-time.After(wait):
// 		}
// 	}
//
// 	return fmt.Errorf("insert batch failed after %d retries: %w", maxRetries, lastErr)
// }

// // batchProcessor buffers rows and flushes them to TimescaleDB in batches.
// // ------------------- Batch Processor -------------------
// func batchProcessor(
// 	ctx context.Context,
// 	db *pgxpool.Pool,
// 	input <-chan MetricRow,
// 	consumer *kafka.Consumer,
// 	workerID int,
// 	flushBarrier <-chan struct{},
//
// ) {
// 	cfg := currentConfig.Load().(*Config)
// 	ticker := time.NewTicker(cfg.Quill.BatchTimeout)
// 	idleFlushTimeout := 2 * time.Second
// 	idleTimer := time.NewTimer(idleFlushTimeout)
// 	defer idleTimer.Stop()
//
// 	defer ticker.Stop()
//
// 	buffer := make([]MetricRow, 0, cfg.Quill.BatchSize)
//
// 	flush := func(reason string) {
// 		if len(buffer) == 0 {
// 			return
// 		}
// 		log.Debug().
// 			Int("worker", workerID).
// 			Str("reason", reason).
// 			Int("row_count", len(buffer)).
// 			Msg("Flushing batch")
//
// 		start := time.Now()
// 		if err := retryInsertBatch(ctx, db, buffer, workerID); err != nil {
// 			log.Error().
// 				Err(err).
// 				Int("worker", workerID).
// 				Int("count", len(buffer)).
// 				Msg("Failed to insert batch")
// 			errorsTotal.Inc()
// 			rowsFailedTotal.Add(float64(len(buffer)))
// 			// ❌ Do not commit offsets if DB failed
// 		} else {
// 			log.Info().
// 				Int("worker", workerID).
// 				Int("count", len(buffer)).
// 				Msg("Batch inserted successfully")
//
// 			messagesTotal.Add(float64(len(buffer)))
// 			batchesTotal.Inc()
// 			batchSizeHist.Observe(float64(len(buffer)))
//
// 			// ✅ Commit Kafka offsets after DB success
// 			latestByPartition := make(map[int]*kafka.Message)
// 			for _, r := range buffer {
// 				if msg, ok := latestByPartition[r.Partition]; !ok || r.Offset > int64(msg.TopicPartition.Offset) {
// 					latestByPartition[r.Partition] = &r.Msg
// 				}
// 			}
//
// 			commits := make([]kafka.Message, 0, len(latestByPartition))
// 			for _, msg := range latestByPartition {
// 				commits = append(commits, *msg)
// 			}
// 			if len(commits) > 0 {
// 				// Build TopicPartition list
// 				tps := make([]kafka.TopicPartition, 0, len(latestByPartition))
// 				for _, msg := range latestByPartition {
// 					tp := kafka.TopicPartition{
// 						Topic:     msg.TopicPartition.Topic,
// 						Partition: msg.TopicPartition.Partition,
// 						Offset:    msg.TopicPartition.Offset + 1, // commit next offset
// 					}
// 					tps = append(tps, tp)
// 				}
// 				_, err := consumer.CommitOffsets(tps)
//
// 				if err != nil {
// 					log.Error().Err(err).
// 						Int("worker", workerID).
// 						Int("commits", len(tps)).
// 						Msg("Failed to commit offsets")
// 				} else {
// 					offsetsCommittedTotal.Add(float64(len(tps)))
// 					log.Debug().
// 						Int("worker", workerID).
// 						Int("commits", len(tps)).
// 						Msg("Offsets committed successfully")
// 				}
// 			}
//
// 		}
//
// 		log.Debug().
// 			Dur("took", time.Since(start)).
// 			Int("worker", workerID).
// 			Msg("Batch flush completed")
//
// 		buffer = buffer[:0]
// 	}
//
// 	for {
// 		select {
// 		case <-flushBarrier:
// 			log.Info().Int("worker", workerID).Msg("Received shutdown flush signal")
// 			flush("shutdown")
// 			return
//
// 		case <-ctx.Done():
// 			log.Info().Int("worker", workerID).Msg("Context canceled, exiting")
// 			return
//
// 		case row := <-input:
// 			cfg = currentConfig.Load().(*Config) // reload config
// 			buffer = append(buffer, row)
// 			idleTimer.Reset(idleFlushTimeout)
// 			if len(buffer) >= cfg.Quill.BatchSize {
// 				flush("batch size")
// 			}
//
// 		case <-ticker.C:
// 			flush("batch timeout")
// 			cfg = currentConfig.Load().(*Config)
// 			ticker.Reset(cfg.Quill.BatchTimeout)
//
// 		case <-idleTimer.C:
// 			if len(buffer) > 0 {
// 				log.Info().Int("worker", workerID).Msg("Idle flush triggered")
// 				flush("idle timeout")
// 			}
// 			idleTimer.Reset(idleFlushTimeout)
//
// 		}
// 	}
// }

// ------------------- Deduplication Globals -------------------
// TODO: seems to be not in the use??
// func isDuplicateKeyError(err error) bool {
// 	if err == nil {
// 		return false
// 	}
// 	// PostgreSQL duplicate error code is 23505
// 	return strings.Contains(err.Error(), "duplicate key value violates unique constraint") ||
// 		strings.Contains(err.Error(), "SQLSTATE 23505")
// }
