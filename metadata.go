package main

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const consumerMetadataTable = `
   create table if not exists metadata(
      variable_name text primary key,
      variable_value text
   )`

const topicName = "kafka-topic"

// -----------------------------------------------------------------------------
func createConsmumerMetadataTable(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, consumerMetadataTable)
	return err
}

// -----------------------------------------------------------------------------
func geCurrentMetadataValue(ctx context.Context, pool *pgxpool.Pool, name string) (string, error) {
	var value string

	query := `select variable_value from metadata where variable_name = $1`

	err := pool.QueryRow(ctx, query, name).Scan(&value)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}

	return value, nil
}

// -----------------------------------------------------------------------------
func setMetadataValue(ctx context.Context, pool *pgxpool.Pool, name string, value string) error {
	query := `
		insert into metadata (variable_name, variable_value) values ($1, $2)
		on conflict (variable_name)
		do update
         set variable_value = EXCLUDED.variable_value	`

	_, err := pool.Exec(ctx, query, name, value)
	if err != nil {
		return err
	}

	return nil
}
