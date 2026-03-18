// cache implementation for basic entities like devices, metrics and so on

package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

// sensor cache
// Reference caches for fast lookups of small tables
var (
	refMu           sync.RWMutex
	groupCache      = make(map[string]int)
	nodeCache       = make(map[string]int)
	deviceCache     = make(map[string]int)
	typeCache       = make(map[string]int)
	metricNameCache = make(map[string]int)
)

// ------------------- Helpers -------------------
// lookupOrCreateRef is a generic helper for reference tables.
// tableName: e.g. "device", "edge_node"
// columnName: e.g. "device_id", "edge_node_id"
// lookupOrCreateRef inserts or fetches an integer ID for a reference value.
// It supports all lookup tables: group_ref, edge_node, device, data_class, metric_name.
func lookupOrCreateRef(
	ctx context.Context,
	db *pgxpool.Pool,
	tableName string, // e.g. "device"
	columnName string, // e.g. "device_id"
	value string, // e.g. "Puc68"
	fkColumn string, // e.g. "node_id_no" or "" if none
	fkValue int, // e.g. 42 or 0 if none
) (int, error) {

	if value == "" {
		return 0, fmt.Errorf("empty value for %s", tableName)
	}

	// Map table → id column
	idCols := map[string]string{
		"group_ref":   "group_id_no",
		"edge_node":   "node_id_no",
		"device":      "device_id_no",
		"data_class":  "type_no",
		"metric_name": "metric_name_no",
	}

	idCol, ok := idCols[tableName]
	if !ok {
		return 0, fmt.Errorf("unknown table %s", tableName)
	}

	var sql string
	var id int

	if fkColumn != "" && fkValue != 0 {
		sql = fmt.Sprintf(`
            INSERT INTO %s (%s, %s)
            VALUES ($1, $2)
            ON CONFLICT (%s) DO UPDATE SET %s = EXCLUDED.%s
            RETURNING %s;
        `, tableName, columnName, fkColumn, columnName, fkColumn, fkColumn, idCol)

		if err := db.QueryRow(ctx, sql, value, fkValue).Scan(&id); err != nil {
			return 0, err
		}
	} else {
		sql = fmt.Sprintf(`
            INSERT INTO %s (%s)
            VALUES ($1)
            ON CONFLICT (%s) DO UPDATE SET %s = EXCLUDED.%s
            RETURNING %s;
        `, tableName, columnName, columnName, columnName, columnName, idCol)

		if err := db.QueryRow(ctx, sql, value).Scan(&id); err != nil {
			return 0, err
		}
	}

	return id, nil
}

// lookupOrCreateRefID uses cache + DB lookup
func lookupOrCreateRefID(
	ctx context.Context,
	db *pgxpool.Pool,
	cache map[string]int,
	mu *sync.RWMutex,
	tableName, columnName, value string,
	extra ...interface{}, // optional: fkColumn string, fkValue int
) (int, error) {

	mu.RLock()
	if id, ok := cache[value]; ok {
		mu.RUnlock()
		return id, nil
	}
	mu.RUnlock()

	var fkColumn string
	var fkValue int

	if len(extra) == 2 {
		fkColumn, _ = extra[0].(string)
		fkValue, _ = extra[1].(int)
	}

	var id int

	// run 'lookupOrCreateRef' with retry
	err := retryUntilDone(
		ctx,
		"lookupOrCreateRef",
		func(innerCtx context.Context) (innerErr error) {
			var lookupErr error
			id, lookupErr = lookupOrCreateRef(innerCtx, db, tableName, columnName, value, fkColumn, fkValue)
			if lookupErr != nil {
				dbOpRetryTotal.WithLabelValues().Inc()
			}
			return lookupErr
		},
		isRetriableErr,
		"failure",
		func(finalErr error) error {
			return finalErr
		},
	)

	if err != nil {
		return 0, err
	}

	mu.Lock()
	cache[value] = id
	mu.Unlock()

	return id, nil
}

func lookupOrCreateMetricName(
	ctx context.Context,
	db *pgxpool.Pool,
	cache map[string]int,
	mu *sync.RWMutex,
	name string,
	dataType string,
	deviceIDNo int,
) (int, error) {

	if name == "" {
		return 0, fmt.Errorf("empty metric name")
	}

	// Check cache first
	mu.RLock()
	if id, ok := cache[name]; ok {
		mu.RUnlock()
		return id, nil
	}
	mu.RUnlock()

	// insert or update metric_name with data_type
	sql := `
        INSERT INTO metric_name (name, data_type, device_id_no)
        VALUES ($1, $2, $3)
        ON CONFLICT (name) DO UPDATE SET data_type = EXCLUDED.data_type
        RETURNING metric_name_no;
    `

	var id int

	// wrap it in the retry
	err := retryUntilDone(
		ctx,
		"lookupOrCreateMetricName",
		func(innerCtx context.Context) (innerErr error) {
			e := db.QueryRow(innerCtx, sql, name, dataType, deviceIDNo).Scan(&id)
			if e != nil {
				dbOpRetryTotal.WithLabelValues().Inc()
			}

			return e
		},
		isRetriableErr,
		"failure",
		func(finalErr error) error {
			return fmt.Errorf("insert metric_name failed after retries: %w", finalErr)
		},
	)

	if err != nil {
		return 0, err
	}

	// update cache
	mu.Lock()
	cache[name] = id
	mu.Unlock()

	return id, nil
}

// -----------------------------------------------------------------------------
func preloadReferenceCaches(ctx context.Context, db *pgxpool.Pool) error {
	log.Info().Msg("Preloading reference caches from database...")

	type preloadTarget struct {
		table  string
		idCol  string
		valCol string
		cache  map[string]int
	}

	targets := []preloadTarget{
		{"group_ref", "group_id_no", "group_id", groupCache},
		{"edge_node", "node_id_no", "edge_node_id", nodeCache},
		{"device", "device_id_no", "device_id", deviceCache},
		{"data_class", "type_no", "type", typeCache},
		{"metric_name", "metric_name_no", "name", metricNameCache},
	}

	for _, t := range targets {
		query := fmt.Sprintf("SELECT %s, %s FROM %s", t.idCol, t.valCol, t.table)

		rows, err := db.Query(ctx, query)
		if err != nil {
			return fmt.Errorf("preload %s failed: %w", t.table, err)
		}

		count := 0
		for rows.Next() {
			var id int
			var val string
			if err := rows.Scan(&id, &val); err != nil {
				rows.Close()
				return fmt.Errorf("scan failed for %s: %w", t.table, err)
			}

			refMu.Lock()
			t.cache[val] = id
			refMu.Unlock()
			count++
		}
		rows.Close()

		log.Info().Str("table", t.table).Int("entries", count).Msg("Cache preloaded")
	}

	log.Info().Msg("All reference caches preloaded successfully")
	return nil
}
