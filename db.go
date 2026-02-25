// create db connection pool to the backend storage

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
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
func getPostgresErrorCode(err error) (string, bool) {
	var pgErr *pgconn.PgError

	if err != nil && errors.As(err, &pgErr) {
		return pgErr.Code, true
	}

	return "", false
}

// -----------------------------------------------------------------------------
func isRetriableErr(err error) bool {
	if err == nil {
		return false
	}

	// postgres error
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", // serialization failure
			"40P01",                                              // deadlock
			"53300",                                              // too_many_connections
			"53400",                                              // configuration limit exceeded
			"55P03",                                              // lock not available
			"57014",                                              // statement timeout (all class 57 ?)
			"57P03",                                              // cannot  connect now
			"57P01",                                              // terminating connection due to administrator command
			"08000", "08003", "08006", "08001", "08004", "08007": // connection errors (all class 08 ?)
			return true

		default:
			return false
		}
	}

	// network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// syscalls: connection reset/refused or broken pipe
	if errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE) {
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

	log.Debug().Int("worker", workerID).Int("row_count", len(batch)).Msg("COPYing batch into metric table")

	s = tracker.startStage("pg-copy-exec")
	defer s.end()
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
		code, isPG := getPostgresErrorCode(err)
		log.Error().Int("worker", workerID).Err(err).Str("code", code).Bool("isPG", isPG).Msg("COPY failed")
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
func retryUntilDone(ctx context.Context,
	tryFuncLabel string,
	tryFunc TryFunc,
	isRetriable IsRetriableFunc,
	onNonRetriableLabel string,
	onNonRetriable func(error) error,
) error {

	defer func() {
		if r := recover(); r != nil {
			log.Error().Err(fmt.Errorf("pgx panic recovered: %v", r)).Msg("recovered from pgx panic")
		}
	}()

	backoff := time.Second
	retryCount := 0

	for {
		err := tryFunc(ctx)

		if err == nil {
			if retryCount > 0 {
				log.Info().Str("op", tryFuncLabel).Int("retries", retryCount).Msg("operation succeeded after previous failures")
			}
			return nil
		}
		retryCount++

		if !isRetriable(err) {
			log.Info().Err(err).Str("op", tryFuncLabel).Str("next op", onNonRetriableLabel).Msg("error is not retriable so we will try the next method if available.")
			return onNonRetriable(err)
		} else {
			log.Info().Err(err).Str("op", tryFuncLabel).Str("next op", onNonRetriableLabel).Int("retries", retryCount).Msg("error is retriable so we will try it again after the sleep")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(backoff)
			if backoff <= 16*time.Second {
				backoff *= 2
			}
		}
	}
}
