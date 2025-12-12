package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

// -----------------------------------------------------------------------------
func fnTruncateDatabase() (any, error) {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, cfg.getDbDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to create pool: %w", err)
	}
	defer pool.Close()

	rows, err := pool.Query(ctx, "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = 'public'")
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var fullNames []string
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return nil, err
		}
		fullNames = append(fullNames, fmt.Sprintf("%s.%s", schema, table))
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	if len(fullNames) == 0 {
		return nil, nil
	}

	sql := "truncate " + join(fullNames, ", ") + " cascade"
	_, err = pool.Exec(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("truncate failed: %w", err)
	}

	return nil, nil
}

// -----------------------------------------------------------------------------
func fnSqlExecutor(query string) (any, error) {
	dbpool, err := pgxpool.New(context.Background(), cfg.getDbDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres pool: %w", err)
	}
	defer dbpool.Close()

	// run sql
	rows, err := dbpool.Query(context.Background(), query)
	if err != nil {
		log.Err(err).Msg("error while running pg query")
		return nil, fmt.Errorf("sql execution failed: %w", err)
	}
	defer rows.Close()

	// expected 1 row with 1 column. otherwise - fail
	if !rows.Next() {
		return nil, nil
	}

	// get the value as a string so we don't care about exact type
	var val any
	err = rows.Scan(&val)
	if err != nil {
		return nil, fmt.Errorf("failed to scan sql result: %w", err)
	}

	return val, nil
}
