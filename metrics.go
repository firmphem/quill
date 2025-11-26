package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

// insertSensorErrorRow inserts a malformed or rejected Kafka message into the sensor_errors table.
// raw_payload is stored as BYTEA, so it can contain invalid UTF-8 or malformed JSON safely.
func insertSensorErrorRow(
	ctx context.Context,
	db *pgxpool.Pool,
	sensorName string,
	reason string,
	rawValue []byte,
	partition int,
	offset int64,
) {
	if rawValue == nil {
		rawValue = []byte("{}") // minimal placeholder
	}

	_, err := db.Exec(ctx, `
		INSERT INTO sensor_errors (created_at, sensor_name, reason, raw_payload, partition, kafka_offset)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, time.Now(), sensorName, reason, rawValue, partition, offset)

	if err != nil {
		log.Error().
			Err(err).
			Str("sensor", sensorName).
			Msg("Failed to insert sensor error row")
	} else {
		log.Info().
			Str("sensor", sensorName).
			Str("reason", reason).
			Msg("Inserted sensor error row")
	}
}

// ------------------- DB Insert (COPY with Dedup) -------------------
func insertBatchCopy(ctx context.Context, db *pgxpool.Pool, batch []MetricRow, workerID int) error {
	if len(batch) == 0 {
		return nil
	}

	trackToPostgresUsingCopyMethod := tracker.Start("toPostgresUsingCopyMethod")
	defer trackToPostgresUsingCopyMethod()
	// Prepare rows for COPY
	rows := make([][]interface{}, len(batch))
	for i, r := range batch {
		rows[i] = []interface{}{
			r.MetricTimestamp.UnixMilli(),  // metric_timestamp (BIGINT)
			r.PayloadTimestamp.UnixMilli(), // payload_timestamp (BIGINT)
			r.Value,                        // value
			r.MetricNameNo,                 // metric_name_no
			r.DeviceIDNo,                   // device_id_no
			r.NodeIDNo,                     // node_id_no
			r.GroupIDNo,                    // group_id_no
			r.TypeNo,                       // type_no
		}
	}

	log.Debug().
		Int("worker", workerID).
		Int("row_count", len(batch)).
		Msg("COPYing batch into metric table")
	start := time.Now()
	ct, err := db.CopyFrom(
		ctx,
		pgx.Identifier{"metric"},
		[]string{
			"metric_timestamp",
			"payload_timestamp",
			"value",
			"metric_name_no",
			"device_id_no",
			"node_id_no",
			"group_id_no",
			"type_no",
		},
		pgx.CopyFromRows(rows),
	)
	duration := time.Since(start)
	if err != nil {
		return fmt.Errorf("copy into metric failed: %w", err)
	}

	if int(ct) != len(batch) {
		log.Warn().
			Int("worker", workerID).
			Int("expected", len(batch)).
			Int64("inserted", ct).
			Msg("COPY inserted fewer rows than expected")
	}

	rowsInsertedTotal.Add(float64(ct))
	batchesTotal.Inc()

	trackToPostgresUsingCopyMethod()
	// ⏱️ 8️⃣ Log duration and throughput

	// ✅ Log with realistic duration and throughput
	log.Debug().
		Int("worker", workerID).
		Int("batchSize", len(batch)).
		Int64("rowsInserted", ct).
		Str("duration", duration.String()). // <-- use String() for human-readable
		Float64("rowsPerSec", float64(ct)/duration.Seconds()).
		Msg("Batch inserted with COPY")

	return nil

}

func insertBatchFallback(ctx context.Context, db *pgxpool.Pool, batch []MetricRow, workerID int) error {
	if len(batch) == 0 {
		return nil
	}

	trackToPostgresUsingRowByRow := tracker.Start("toPostgresUsingRowByRow")
	defer trackToPostgresUsingRowByRow()
	// Prepare insert SQL for metric table
	sql := `
		INSERT INTO metric_unclogged (
			metric_timestamp,
			payload_timestamp,
			data_type,
			value,
			metric_name_no,
			device_id_no,
			node_id_no,
			group_id_no,
			type_no
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT DO NOTHING;
	`

	var inserted int
	for _, r := range batch {
		_, err := db.Exec(
			ctx,
			sql,
			r.MetricTimestamp.UnixMilli(),
			r.PayloadTimestamp.UnixMilli(),
			r.Value,
			r.MetricNameNo,
			r.DeviceIDNo,
			r.NodeIDNo,
			r.GroupIDNo,
			r.TypeNo,
		)

		if err != nil {
			log.Error().
				Err(err).
				Int("worker", workerID).
				Int("metric_name_no", r.MetricNameNo).
				Msg("Failed to insert row in fallback mode")
			continue
		}
		inserted++
	}

	log.Warn().
		Int("worker", workerID).
		Int("inserted", inserted).
		Int("skipped", len(batch)-inserted).
		Msg("Fallback insert completed with ON CONFLICT DO NOTHING")

	rowsInsertedTotal.Add(float64(inserted))
	trackToPostgresUsingRowByRow()

	return nil
}

// retryInsertBatch tries to insert a batch with exponential backoff.
// It retries only on transient errors (like network or timeout issues).
func retryInsertBatch(ctx context.Context, db *pgxpool.Pool, batch []MetricRow, workerID int) error {
	const maxRetries = 3
	baseDelay := time.Second
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		lastErr = insertBatchFn(ctx, db, batch, workerID)
		if lastErr == nil {
			return nil // ✅ success
		}

		// check if it's a Postgres error
		var pgErr *pgconn.PgError
		isTransient := false

		if errors.As(lastErr, &pgErr) {
			code := pgErr.Code

			switch {
			// Connection-level errors
			case strings.HasPrefix(code, "08"):
				isTransient = true
			// Serialization failure or deadlock
			case code == "40001" || code == "40P01":
				isTransient = true
			// Query canceled / timeout
			case code == "57014":
				isTransient = true
			// Everything else — not retryable
			default:
				isTransient = false
			}
		} else {
			// Fallback: look for common transient patterns in plain errors
			msg := lastErr.Error()
			isTransient = strings.Contains(msg, "timeout") ||
				strings.Contains(msg, "connection reset") ||
				strings.Contains(msg, "broken pipe")
		}

		if !isTransient {
			// ❌ non-retryable
			return lastErr
		}

		// log and backoff
		wait := baseDelay * time.Duration(1<<(attempt-1))
		log.Warn().
			Err(lastErr).
			Str("sqlstate", pgErr.Code).
			Int("worker", workerID).
			Int("attempt", attempt).
			Dur("backoff", wait).
			Msg("Transient DB error, retrying batch")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}

	return fmt.Errorf("insert batch failed after %d retries: %w", maxRetries, lastErr)
}

// batchProcessor buffers rows and flushes them to TimescaleDB in batches.
// ------------------- Batch Processor -------------------
func batchProcessor(
	ctx context.Context,
	db *pgxpool.Pool,
	input <-chan MetricRow,
	consumer *kafka.Consumer,
	workerID int,
	flushBarrier <-chan struct{},

) {
	cfg := currentConfig.Load().(*Config)
	ticker := time.NewTicker(cfg.Quill.BatchTimeout)
	idleFlushTimeout := 2 * time.Second
	idleTimer := time.NewTimer(idleFlushTimeout)
	defer idleTimer.Stop()

	defer ticker.Stop()

	buffer := make([]MetricRow, 0, cfg.Quill.BatchSize)

	flush := func(reason string) {
		if len(buffer) == 0 {
			return
		}
		log.Debug().
			Int("worker", workerID).
			Str("reason", reason).
			Int("row_count", len(buffer)).
			Msg("Flushing batch")

		start := time.Now()
		if err := retryInsertBatch(ctx, db, buffer, workerID); err != nil {
			log.Error().
				Err(err).
				Int("worker", workerID).
				Int("count", len(buffer)).
				Msg("Failed to insert batch")
			errorsTotal.Inc()
			rowsFailedTotal.Add(float64(len(buffer)))
			// ❌ Do not commit offsets if DB failed
		} else {
			log.Debug().
				Int("worker", workerID).
				Int("count", len(buffer)).
				Msg("Batch inserted successfully")

			messagesTotal.Add(float64(len(buffer)))
			batchesTotal.Inc()
			batchSizeHist.Observe(float64(len(buffer)))

			// ✅ Commit Kafka offsets after DB success
			latestByPartition := make(map[int]*kafka.Message)
			for _, r := range buffer {
				if msg, ok := latestByPartition[r.Partition]; !ok || r.Offset > int64(msg.TopicPartition.Offset) {
					latestByPartition[r.Partition] = &r.Msg
				}
			}

			commits := make([]kafka.Message, 0, len(latestByPartition))
			for _, msg := range latestByPartition {
				commits = append(commits, *msg)
			}
			if len(commits) > 0 {
				// Build TopicPartition list
				tps := make([]kafka.TopicPartition, 0, len(latestByPartition))
				for _, msg := range latestByPartition {
					tp := kafka.TopicPartition{
						Topic:     msg.TopicPartition.Topic,
						Partition: msg.TopicPartition.Partition,
						Offset:    msg.TopicPartition.Offset + 1, // commit next offset
					}
					tps = append(tps, tp)
				}
				_, err := consumer.CommitOffsets(tps)

				if err != nil {
					log.Error().Err(err).
						Int("worker", workerID).
						Int("commits", len(tps)).
						Msg("Failed to commit offsets")
				} else {
					offsetsCommittedTotal.Add(float64(len(tps)))
					log.Debug().
						Int("worker", workerID).
						Int("commits", len(tps)).
						Msg("Offsets committed successfully")
				}
			}

		}

		log.Debug().
			Dur("took", time.Since(start)).
			Int("worker", workerID).
			Msg("Batch flush completed")

		buffer = buffer[:0]
	}

	for {
		select {
		case <-flushBarrier:
			log.Info().Int("worker", workerID).Msg("Received shutdown flush signal")
			flush("shutdown")
			return

		case <-ctx.Done():
			log.Info().Int("worker", workerID).Msg("Context canceled, exiting")
			return

		case row := <-input:
			cfg = currentConfig.Load().(*Config) // reload config
			buffer = append(buffer, row)
			idleTimer.Reset(idleFlushTimeout)
			if len(buffer) >= cfg.Quill.BatchSize {
				flush("batch size")
			}

		case <-ticker.C:
			flush("batch timeout")
			cfg = currentConfig.Load().(*Config)
			ticker.Reset(cfg.Quill.BatchTimeout)

		case <-idleTimer.C:
			if len(buffer) > 0 {
				log.Info().Int("worker", workerID).Msg("Idle flush triggered")
				flush("idle timeout")
			}
			idleTimer.Reset(idleFlushTimeout)

		}
	}
}

// ------------------- Deduplication Globals -------------------
// TODO: seems to be not in the use??
func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	// PostgreSQL duplicate error code is 23505
	return strings.Contains(err.Error(), "duplicate key value violates unique constraint") ||
		strings.Contains(err.Error(), "SQLSTATE 23505")
}
