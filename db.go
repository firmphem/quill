// create db connection pool to the backend storage

package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

type TryFunc func(ctx context.Context) error
type IsRetriableFunc func(error) bool

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
func isRetriablePostgreSQLErr(err error) bool {
	if err == nil {
		return false
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// TODO: review them carefully again
		// https://www.postgresql.org/docs/current/errcodes-appendix.html
		switch pgErr.Code {
		case "40001": // serialization failure
		case "40P01": // deadlock
		case "53300": // too_many_connections
		case "53400": // configuration limit exceeded
		case "55P03": // lock not available
		case "57014": // statement timeout (all class 57 ?)
		case "08000", "08003", "08006", "08001", "08004", "08007": // connection errors (all class 08 ?)
			return true

		default:
			return false
		}
	}

	// network from the network layer errors
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}

	return false
}

// TODO:
// below methods needs to be reworked. no references to workers we should have. only partition/offset
//

// -----------------------------------------------------------------------------
func insertBatchCopy(ctx context.Context, db *pgxpool.Pool, batch []MetricRow, workerID int) error {
	if len(batch) == 0 {
		return nil
	}

	s := tracker.startStage("pg-copy-transform")
	rows := make([][]interface{}, len(batch))
	for i, r := range batch {
		rows[i] = []interface{}{
			r.MetricTimestamp.UnixMilli(), // metric_timestamp (BIGINT)
			r.Value,                       // value
			r.MetricNameNo,                // metric_name_no
			r.DeviceIDNo,                  // device_id_no
			r.NodeIDNo,                    // node_id_no
			r.GroupIDNo,                   // group_id_no
			r.TypeNo,                      // type_no
		}
	}
	s.end()

	log.Debug().Int("worker", workerID).Int("row_count", len(batch)).Msg("COPYing batch into metric table")

	s = tracker.startStage("pg-copy-exec")
	defer s.end()
	ct, err := db.CopyFrom(
		ctx,
		pgx.Identifier{"metric"},
		[]string{
			"metric_timestamp",
			"value",
			"metric_name_no",
			"device_id_no",
			"node_id_no",
			"group_id_no",
			"type_no",
		},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		log.Error().Int("worker", workerID).Msg("COPY failed")
		return fmt.Errorf("copy into metric failed: %w", err)
	}

	if int(ct) != len(batch) {
		log.Debug().Int("worker", workerID).Int("expected", len(batch)).Int64("inserted", ct).Msg("COPY inserted fewer rows than expected")
	}

	rowsInsertedTotal.Add(float64(ct))
	batchesTotal.Inc()

	log.Debug().Int("worker", workerID).Int("batchSize", len(batch)).Int64("rowsInserted", ct).Msg("Batch inserted with COPY")

	return nil
}

// -----------------------------------------------------------------------------
func (pw *PartitionWorker) insertMetricsToPostgresWithRetries(db *pgxpool.Pool, batch []MetricRow, workerID int) error {
	s := tracker.startStage("pg")
	defer s.end()
	copyTry := func(ctx context.Context) error {
		return insertBatchCopy(pw.ctx, db, batch, workerID)
	}

	batchTry := func(ctx context.Context) error {
		return insertBatchTransactional(pw.ctx, db, batch, workerID)
	}

	fallbackToBatch := func(copyErr error) error {
		return retryUntilDone(
			pw.ctx,
			batchTry,
			isRetriablePostgreSQLErr,
			func(batchErr error) error {
				return batchErr
			},
		)
	}

	return retryUntilDone(
		pw.ctx,
		copyTry,
		isRetriablePostgreSQLErr,
		fallbackToBatch,
	)
}

// -----------------------------------------------------------------------------
func insertBatchTransactional(ctx context.Context, db *pgxpool.Pool, batch []MetricRow, workerID int) error {
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
			log.Error().Int("worker", workerID).Msg("BATCH failed")
			_ = tx.Rollback(ctx)
		}
	}()

	insertSQL := `
        INSERT INTO metric (
            metric_timestamp,
            value,
            metric_name_no,
            device_id_no,
            node_id_no,
            group_id_no,
            type_no
        ) VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT DO NOTHING
    `

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
				r.Value,
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
			log.Error().Err(err2).Msg("batch insert failed")
			return fmt.Errorf("batch insert failed: %w", err2)
		}
		br.Close()
	}

	if err = tx.Commit(ctx); err != nil {
		log.Error().Err(err).Msg("transaction commit failed")
		return fmt.Errorf("transaction commit failed: %w", err)
	}

	log.Debug().Int("worker", workerID).Int("batch", len(batch)).Msg("Batch inserted data")

	return nil
}

// -----------------------------------------------------------------------------
// retry and error is not retriable fallback to another method if any OR fail
func retryUntilDone(ctx context.Context, tryFunc TryFunc, isRetriable IsRetriableFunc, onNonRetriable func(error) error) error {
	backoff := time.Second

	for {
		err := tryFunc(ctx)

		if err == nil {
			return nil
		}

		if !isRetriable(err) {
			return onNonRetriable(err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(backoff)
			if backoff <= 32*time.Second {
				backoff *= 2
			}
		}
	}
}
