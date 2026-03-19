// create db connection pool to the backend storage

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

const (
	DbInsertMethodCopy   = "copy"
	DbInsertMethodInsert = "insert"
)

// -----------------------------------------------------------------------------
func createDBPool(cfg *Config) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.Database.User, cfg.Database.Password, cfg.Database.Host,
		cfg.Database.Port, cfg.Database.Name)
	return pgxpool.New(context.Background(), dsn)
}

// -----------------------------------------------------------------------------
func initDatabase(cfg *Config) *pgxpool.Pool {
	dbPool, err := createDBPool(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("DB connection failed")
	}
	return dbPool
}

// -----------------------------------------------------------------------------
func (pw *PartitionWorker) insertMetricsToPostgresWithRetries(db *pgxpool.Pool, batch []MetricRow, partitionID int) error {
	s := tracker.startStage("pg")
	defer s.end()
	copyTry := func(ctx context.Context) error {
		e := insertBatchCopy(pw.ctx, db, batch, partitionID)
		if e != nil {
			dbInsertRetryTotal.WithLabelValues(DbInsertMethodCopy).Inc()
			dbOpRetryTotal.WithLabelValues().Inc()
		}
		return e
	}

	batchTry := func(ctx context.Context) error {
		e := insertBatchTransactional(pw.ctx, db, batch, partitionID)
		if e != nil {
			dbInsertRetryTotal.WithLabelValues(DbInsertMethodInsert).Inc()
			dbOpRetryTotal.WithLabelValues().Inc()
		}
		return e
	}

	fallbackToBatch := func(copyErr error) error {
		return retryUntilDone(
			pw.ctx,
			"batch_insert_to_postgres",
			batchTry,
			isRetriableErr,
			"failure",
			func(batchErr error) error {
				return batchErr
			},
		)
	}

	return retryUntilDone(
		pw.ctx,
		"copy_to_postgres",
		copyTry,
		isRetriableErr,
		"batch_insert_to_postgres",
		fallbackToBatch,
	)
}

// -----------------------------------------------------------------------------
func insertBatchCopy(ctx context.Context, db *pgxpool.Pool, batch []MetricRow, partitionID int) error {
	if len(batch) == 0 {
		return nil
	}

	s := tracker.startStage("pg-copy-transform")
	rows := make([][]interface{}, len(batch))
	for i, r := range batch {
		rows[i] = []interface{}{
			r.MetricTimestamp.UnixMilli(), // metric_timestamp (BIGINT)
			r.ValueF,                      // valueF
			r.ValueI,                      // valueI
			r.MetricNameNo,                // metric_name_no
			r.DeviceIDNo,                  // device_id_no
			r.NodeIDNo,                    // node_id_no
			r.GroupIDNo,                   // group_id_no
			r.TypeNo,                      // type_no
		}
	}
	s.end()

	s = tracker.startStage("pg-copy-exec")
	defer s.end()
	start := time.Now()
	ct, err := db.CopyFrom(
		ctx,
		pgx.Identifier{"metric"},
		[]string{
			"metric_timestamp",
			"value_f",
			"value_i",
			"metric_name_no",
			"device_id_no",
			"node_id_no",
			"group_id_no",
			"type_no",
		},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		log.Error().Int("partition", partitionID).Err(err).Msg("COPY failed")
		return fmt.Errorf("copy into metric failed: %w", err)
	}

	dbInsertDuration.WithLabelValues(DbInsertMethodCopy).Observe(time.Since(start).Seconds())

	if int(ct) != len(batch) {
		log.Debug().Int("rows_expected", len(batch)).Int64("rows_inserted", ct).Int("partition", partitionID).Msg("COPY inserted fewer rows than expected")
	}

	log.Debug().Int("rows_inserted", len(batch)).Int("partition", partitionID).Msg("Batch was inserted with COPY")

	return nil
}

// -----------------------------------------------------------------------------
func insertBatchTransactional(ctx context.Context, db *pgxpool.Pool, batch []MetricRow, partitionID int) error {
	if len(batch) == 0 {
		return nil
	}
	s := tracker.startStage("pg-batch")
	defer s.end()

	cfg := currentConfig.Load().(*Config)

	tx, err := db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		log.Error().Err(err).Msg("begin tx failed")
		return fmt.Errorf("begin tx failed: %w", err)
	}

	defer func() {
		if err != nil {
			log.Error().Int("partition", partitionID).Msg("BATCH failed")
			_ = tx.Rollback(ctx)
		}
	}()

	insertSQL := `
        INSERT INTO metric (
            metric_timestamp,
            value_f,
            value_i,
            metric_name_no,
            device_id_no,
            node_id_no,
            group_id_no,
            type_no
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        ON CONFLICT DO NOTHING
    `

	start := time.Now()

	for i := 0; i < len(batch); i += cfg.Quill.BatchSize {
		end := i + cfg.Quill.BatchSize
		if end > len(batch) {
			end = len(batch)
		}

		rows := batch[i:end]
		b := &pgx.Batch{}

		for _, r := range rows {
			b.Queue(insertSQL,
				r.MetricTimestamp.UnixMilli(),
				r.ValueF,
				r.ValueI,
				r.MetricNameNo,
				r.DeviceIDNo,
				r.NodeIDNo,
				r.GroupIDNo,
				r.TypeNo,
			)
		}

		br := tx.SendBatch(ctx, b)
		if _, err2 := br.Exec(); err2 != nil {
			br.Close()
			log.Error().Int("partition", partitionID).Err(err2).Msg("batch insert failed")
			return fmt.Errorf("batch insert failed: %w", err2)
		}
		br.Close()
	}

	if err = tx.Commit(ctx); err != nil {
		log.Error().Int("partition", partitionID).Err(err).Msg("transaction commit failed")
		return fmt.Errorf("transaction commit failed: %w", err)
	}
	dbInsertDuration.WithLabelValues(DbInsertMethodInsert).Observe(time.Since(start).Seconds())

	log.Debug().Int("rows_inserted", len(batch)).Int("partition", partitionID).Msg("Batch was inserted with INSERTs")

	return nil
}
