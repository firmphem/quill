// cache implementation for basic entities like devices, metrics and so on

package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

type MetricNameEntry struct {
	ID        int
	DataType  string
	CreatedAt int64
}

// sensor cache
// Reference caches for fast lookups of small tables
var (
	refMu       sync.RWMutex
	groupCache  = make(map[string]int)
	nodeCache   = make(map[string]int)
	deviceCache = make(map[string]int)
	typeCache   = make(map[string]int)
	// metricNameCache = make(map[string]int)
	metricNameCache = make(map[string]MetricNameEntry) // key: "name:device_id_no"
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
	tableName string,
	columnName string, // The unique text column (e.g., 'device_id')
	value string,
	fkColumn string, // The FK column (e.g., 'node_id_no')
	fkValue int,
) (int, error) {
	if value == "" {
		return 0, fmt.Errorf("empty value for %s", tableName)
	}

	type tableSchema struct {
		IDCol      string
		UniqueCols string
	}

	schemas := map[string]tableSchema{
		"group_ref":  {IDCol: "group_id_no", UniqueCols: "group_id"},
		"edge_node":  {IDCol: "node_id_no", UniqueCols: "edge_node_id"},
		"device":     {IDCol: "device_id_no", UniqueCols: "device_id, node_id_no"},
		"data_class": {IDCol: "type_no", UniqueCols: "type"},
	}

	schema, ok := schemas[tableName]
	if !ok {
		return 0, fmt.Errorf("lookupOrCreateRef: unknown table metadata for %s", tableName)
	}

	var id int
	var insertQuery string

	if fkColumn != "" && fkValue != 0 {
		insertQuery = fmt.Sprintf(`
			INSERT INTO %s (%s, %s)
			VALUES ($1, $2)
			ON CONFLICT (%s) DO NOTHING
			RETURNING %s`, tableName, columnName, fkColumn, schema.UniqueCols, schema.IDCol)

		err := db.QueryRow(ctx, insertQuery, value, fkValue).Scan(&id)

		// got ID -> done
		if err == nil {
			return id, nil
		}

		// if it's NOT a "no rows" error, it's a real DB error
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("insert failed on %s: %w", tableName, err)
		}
	} else {
		insertQuery = fmt.Sprintf(`
			INSERT INTO %s (%s)
			VALUES ($1)
			ON CONFLICT (%s) DO NOTHING
			RETURNING %s`, tableName, columnName, schema.UniqueCols, schema.IDCol)

		err := db.QueryRow(ctx, insertQuery, value).Scan(&id)
		if err == nil {
			return id, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("insert failed on %s: %w", tableName, err)
		}
	}

	// due to concurrency. we already had mertic_name inserted in another thread. let's get it from the db (not from the cache)
	var selectQuery string
	var selectErr error

	if fkColumn != "" && fkValue != 0 {
		selectQuery = fmt.Sprintf("select %s from %s where %s = $1 and %s = $2",
			schema.IDCol, tableName, columnName, fkColumn)
		selectErr = db.QueryRow(ctx, selectQuery, value, fkValue).Scan(&id)
	} else {
		selectQuery = fmt.Sprintf("select %s from %s where %s = $1",
			schema.IDCol, tableName, columnName)
		selectErr = db.QueryRow(ctx, selectQuery, value).Scan(&id)
	}

	if selectErr != nil {
		return 0, fmt.Errorf("lookup failed after 'empty' insert on %s: %w", tableName, selectErr)
	}

	return id, nil
}

// ------------------------------------------------------------------------------
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

	err := retryUntilDone(
		ctx,
		fmt.Sprintf("lookupOrCreateRef-%s", tableName),
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

// ------------------------------------------------------------------------------
func retireAndCreateMetricTX(
	ctx context.Context,
	db *pgxpool.Pool,
	oldDataType string,
	name string,
	dataType string,
	deviceIDNo int,
	ts int64,
	retireID int,
) (int, error) {

	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	if retireID != 0 {
		_, err := tx.Exec(ctx, `
			UPDATE metric_name
			SET retired_at = $1
			WHERE metric_name_no = $2 AND retired_at IS NULL`,
			ts, retireID)
		if err != nil {
			return 0, fmt.Errorf("retire current metric name failed: %w", err)
		}
		log.Trace().Str("name", name).Int("device_no_id", deviceIDNo).Str("old data_type", oldDataType).Str("new data_type", dataType).Msg("metric was retired")
	}

	var newID int
	err = tx.QueryRow(ctx, `
		INSERT INTO metric_name (name, data_type, device_id_no, created_at, retired_at)
		VALUES ($1, $2, $3, $4, NULL)
      ON CONFLICT (name, device_id_no) WHERE retired_at IS NULL
      DO UPDATE SET name = EXCLUDED.name, device_id_no = EXCLUDED.device_id_no
		RETURNING metric_name_no`,
		name, dataType, deviceIDNo, ts).Scan(&newID)

	if err != nil {
		return 0, fmt.Errorf("insert new metric name failed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit while retiring metric name failed: %w", err)
	}

	return newID, nil
}

// -----------------------------------------------------------------------------
// this is the entrypoint for DBIRTH and DDATA messages
func lookupOrCreateMetricName(
	ctx context.Context,
	db *pgxpool.Pool,
	cache map[string]MetricNameEntry,
	mu *sync.RWMutex,
	name string,
	dataType string,
	deviceIDNo int,
	ts int64,
) (int, error) {
	cfg := currentConfig.Load().(*Config)

	cacheKey := fmt.Sprintf("%s:%d", name, deviceIDNo)

	if name == "" {
		return 0, fmt.Errorf("empty metric name")
	}

	metricNameNoIDToBeRetired := 0
	oldDataType := ""

	// check cache first
	mu.RLock()
	if entry, ok := cache[cacheKey]; ok {
		// found same metric
		if entry.DataType == dataType {
			mu.RUnlock()
			return entry.ID, nil
		}
		// metric was just created. do not retire it
		if time.Now().UnixMilli()-entry.CreatedAt < cfg.Quill.DoNotRetireTooYoungMetricThresholdMilliseconds {
			log.Trace().Str("name", name).Int("device_no_id", deviceIDNo).Msg("metric too young to be retired")
			mu.RUnlock()
			return entry.ID, nil
		}

		log.Trace().Int("id to be retired", entry.ID).Msg("following id will be retired")
		metricNameNoIDToBeRetired = entry.ID
		oldDataType = entry.DataType

		// it is a cache hit but data type is wrong or metric is too old
		// this will cause retiring the old metric and creating the new one
	} else {
		log.Trace().Str("name", name).Str("data type", dataType).Int("device", deviceIDNo).Msg("cache miss")
	}
	mu.RUnlock()

	var id int
	err := retryUntilDone(
		ctx,
		"retireAndCreateMetricName",
		func(innerCtx context.Context) error {
			var lookupErr error
			id, lookupErr = retireAndCreateMetricTX(
				innerCtx,
				db,
				oldDataType,
				name,
				dataType,
				deviceIDNo,
				ts,
				metricNameNoIDToBeRetired,
			)
			if lookupErr != nil {
				dbOpRetryTotal.WithLabelValues().Inc()
			}
			return lookupErr
		},
		isRetriableErr,
		"failure updating metric_name",
		func(finalErr error) error {
			return finalErr
		},
	)

	if err != nil {
		return 0, err
	}

	refMu.Lock()
	metricNameCache[cacheKey] = MetricNameEntry{ID: id, DataType: dataType, CreatedAt: ts}
	refMu.Unlock()

	return id, nil
}

// -----------------------------------------------------------------------------
// func preloadReferenceCachesX(ctx context.Context, db *pgxpool.Pool) error {
// 	log.Info().Msg("Preloading reference caches from database...")
//
// 	type preloadTarget struct {
// 		table  string
// 		idCol  string
// 		valCol string
// 		cache  map[string]int
// 	}
//
// 	targets := []preloadTarget{
// 		{"group_ref", "group_id_no", "group_id", groupCache},
// 		{"edge_node", "node_id_no", "edge_node_id", nodeCache},
// 		{"device", "device_id_no", "device_id", deviceCache},
// 		{"data_class", "type_no", "type", typeCache},
// 		{"metric_name", "metric_name_no", "name", metricNameCache},
// 	}
//
// 	for _, t := range targets {
// 		query := fmt.Sprintf("SELECT %s, %s FROM %s", t.idCol, t.valCol, t.table)
//
// 		rows, err := db.Query(ctx, query)
// 		if err != nil {
// 			return fmt.Errorf("preload %s failed: %w", t.table, err)
// 		}
//
// 		count := 0
// 		for rows.Next() {
// 			var id int
// 			var val string
// 			if err := rows.Scan(&id, &val); err != nil {
// 				rows.Close()
// 				return fmt.Errorf("scan failed for %s: %w", t.table, err)
// 			}
//
// 			refMu.Lock()
// 			t.cache[val] = id
// 			refMu.Unlock()
// 			count++
// 		}
// 		rows.Close()
//
// 		log.Info().Str("table", t.table).Int("entries", count).Msg("Cache preloaded")
// 	}
//
// 	log.Info().Msg("All reference caches preloaded successfully")
// 	return nil
// }

// -----------------------------------------------------------------------------
func defaultCacheReadFunc(cache map[string]int) func(pgx.Rows) error {
	return func(rows pgx.Rows) error {
		var id int
		var val string
		if err := rows.Scan(&id, &val); err != nil {
			return err
		}

		refMu.Lock()
		cache[val] = id
		refMu.Unlock()
		return nil
	}
}

func preloadReferenceCaches(ctx context.Context, db *pgxpool.Pool) error {
	log.Info().Msg("Preloading reference caches from database...")

	type preloadTarget struct {
		table   string
		query   string
		process func(pgx.Rows) error
	}

	targets := []preloadTarget{
		{
			table:   "group_ref",
			query:   "SELECT group_id_no, group_id FROM group_ref",
			process: defaultCacheReadFunc(groupCache),
		},
		{
			table:   "edge_node",
			query:   "SELECT node_id_no, edge_node_id FROM edge_node",
			process: defaultCacheReadFunc(nodeCache),
		},
		{
			table:   "device",
			query:   "SELECT device_id_no, device_id FROM device",
			process: defaultCacheReadFunc(deviceCache),
		},
		{
			table:   "data_class",
			query:   "SELECT type_no, type FROM data_class",
			process: defaultCacheReadFunc(typeCache),
		},
		{
			table: "metric_name",
			query: "SELECT metric_name_no, name, data_type, device_id_no, created_at FROM metric_name",
			process: func(rows pgx.Rows) error {
				var id int
				var name, dataType string
				var deviceIDNo *int
				var createdAt int64

				if err := rows.Scan(&id, &name, &dataType, &deviceIDNo, &createdAt); err != nil {
					return err
				}

				devID := 0
				if deviceIDNo != nil {
					devID = *deviceIDNo
				}

				key := fmt.Sprintf("%s:%d", name, devID)

				refMu.Lock()
				metricNameCache[key] = MetricNameEntry{ID: id, DataType: dataType, CreatedAt: createdAt}
				refMu.Unlock()
				return nil
			},
		},
	}

	for _, t := range targets {
		rows, err := db.Query(ctx, t.query)
		if err != nil {
			return fmt.Errorf("preload %s failed: %w", t.table, err)
		}

		count := 0
		for rows.Next() {
			if err := t.process(rows); err != nil {
				rows.Close()
				return fmt.Errorf("scan failed for %s: %w", t.table, err)
			}
			count++
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			return fmt.Errorf("row iteration error for %s: %w", t.table, err)
		}

		log.Info().Str("table", t.table).Int("entries", count).Msg("Cache preloaded")
	}

	log.Info().Msg("all reference caches preloaded successfully")
	return nil
}

// -----------------------------------------------------------------------------
func debug_dumpCaches() {
	fmt.Println("groupCache:", toString(groupCache))
	fmt.Println("nodeCache:", toString(nodeCache))
	fmt.Println("deviceCache:", toString(deviceCache))
	fmt.Println("typeCache:", toString(typeCache))
	fmt.Println("metricNameCache:", toString(metricNameCache))
}
