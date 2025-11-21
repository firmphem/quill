package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"gopkg.in/yaml.v3"
)

// ------------------- Config -------------------
type Config struct {
	Kafka struct {
		Brokers []string `yaml:"brokers"`
		Topic   string   `yaml:"topic"`
		GroupID string   `yaml:"group_id"`
	} `yaml:"kafka"`
	Database struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		User     string `yaml:"user"`
		Password string `yaml:"-"`
		Name     string `yaml:"name"`
	} `yaml:"database"`
	Quill struct {
		BatchSize    int           `yaml:"batch_size"`
		BatchTimeout time.Duration `yaml:"batch_timeout_seconds"`
		Workers      int           `yaml:"workers"`
		Consumers    int           `yaml:"consumers"`
	} `yaml:"quill"`
	DLQ struct {
		Topic string `yaml:"topic"`
	} `yaml:"dlq"`
	Logging struct {
		Level string `yaml:"level"`
	} `yaml:"logging"`

	HTTP struct {
		MetricsPort int `yaml:"metrics_port"`
	} `yaml:"http"`
}

// ------------------- Models -------------------

type KafkaMessage struct {
	Topic   TopicInfo   `json:"topic"`
	Payload PayloadInfo `json:"payload"`
}

type TopicInfo struct {
	Namespace          string `json:"namespace"`
	EdgeNodeDescriptor string `json:"edgeNodeDescriptor"`
	GroupID            string `json:"groupId"`
	EdgeNodeID         string `json:"edgeNodeId"`
	DeviceID           string `json:"deviceId"`
	Type               string `json:"type"`
}

type PayloadInfo struct {
	Timestamp int64    `json:"timestamp"`
	Metrics   []Metric `json:"metrics"`
	Seq       int      `json:"seq"`
}

type Metric struct {
	Name      string      `json:"name"`
	Timestamp int64       `json:"timestamp"`
	DataType  string      `json:"dataType"`
	Value     interface{} `json:"value"`
}

// NBIRTH: node-level, no deviceId
type NBirthMessage struct {
	Topic struct {
		Namespace  string `json:"namespace"`
		GroupID    string `json:"groupId"`
		EdgeNodeID string `json:"edgeNodeId"`
		Type       string `json:"type"` // always "NBIRTH"
	} `json:"topic"`
	Payload struct {
		Timestamp int64    `json:"timestamp"`
		Metrics   []Metric `json:"metrics"`
		Seq       int      `json:"seq"`
	} `json:"payload"`
}

// DBIRTH: device-level, requires deviceId
type DBirthMessage struct {
	Topic struct {
		Namespace  string `json:"namespace"`
		GroupID    string `json:"groupId"`
		EdgeNodeID string `json:"edgeNodeId"`
		DeviceID   string `json:"deviceId"`
		Type       string `json:"type"` // always "DBIRTH"
	} `json:"topic"`
	Payload struct {
		Timestamp int64    `json:"timestamp"`
		Metrics   []Metric `json:"metrics"`
		Seq       int      `json:"seq"`
	} `json:"payload"`
}

// Represents one row ready to insert into the `metric` table.
type MetricRow struct {
	MetricTimestamp  time.Time // derived from metric.timestamp
	PayloadTimestamp time.Time // derived from payload.timestamp
	//DataType         string    // "Float", "Int32", etc.
	Value float64

	// Foreign-key integer references (no actual DB FKs for speed)
	MetricNameNo int
	DeviceIDNo   int
	NodeIDNo     int
	GroupIDNo    int
	TypeNo       int

	// Kafka tracking
	Partition int
	Offset    int64
	Msg       kafka.Message // ✅ keep the original Kafka message
	DedupKey  string
}

// ------------------- DLQ Message -------------------
type DLQMessage struct {
	Key       string
	Value     []byte
	Reason    string
	Partition int
	Offset    int64
}

// ------------------- Globals -------------------
var currentConfig atomic.Value // stores *Config
var insertBatchFn = insertBatchCopy

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

var (
	// key = "timestamp|sensorID"
	dedupCache sync.Map // key: string, value: time.Time
	// TTL for cache entries
	dedupTTL = 10 * time.Minute
)

var (
	dedupMu sync.Mutex
)

// ------------------- Prometheus -------------------
var (
	messagesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quill_messages_total",
		Help: "Total number of sensor datapoints processed",
	})
	batchesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quill_batches_total",
		Help: "Total number of batches inserted",
	})
	errorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quill_errors_total",
		Help: "Total number of DB insert errors",
	})
	batchSizeHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "quill_batch_size",
		Help:    "Distribution of batch sizes inserted",
		Buckets: prometheus.LinearBuckets(100, 500, 10),
	})
	retriesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quill_retries_total",
		Help: "Total retries due to DB failures",
	})
	rowsInsertedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quill_rows_inserted_total",
		Help: "Rows successfully inserted",
	})
	rowsFailedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quill_rows_failed_total",
		Help: "Rows failed during insert",
	})
	rowsEnqueued = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quill_rows_enqueued_total",
		Help: "Rows enqueued from Kafka",
	})
	messagesReceivedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quill_messages_received_total",
		Help: "Kafka messages consumed",
	})
	rowsReceivedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quill_rows_received_total",
		Help: "Rows received from Kafka messages",
	})
	datapointsReceivedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "quill_datapoints_received_total",
		Help: "Datapoints received from Kafka",
	})

	offsetsCommittedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "quill_offsets_committed_total",
			Help: "Total number of Kafka offsets successfully committed",
		})
)

// ------------------- Helpers -------------------
func loadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	config := &Config{}
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, err
	}

	config.Database.Password = os.Getenv("QUILL_DB_PASSWORD")
	if config.Database.Password == "" {
		return nil, fmt.Errorf("QUILL_DB_PASSWORD not set")
	}
	return config, nil
}

func validateConfig(cfg *Config) error {
	if len(cfg.Kafka.Brokers) == 0 || cfg.Kafka.Topic == "" || cfg.Kafka.GroupID == "" {
		return fmt.Errorf("kafka config invalid")
	}
	if cfg.Database.Host == "" || cfg.Database.User == "" || cfg.Database.Name == "" {
		return fmt.Errorf("database config invalid")
	}
	if cfg.Quill.BatchSize <= 0 || cfg.Quill.BatchTimeout <= 0 || cfg.Quill.Consumers <= 0 {
		return fmt.Errorf("quill config invalid")
	}
	return nil
}

func applyLogLevel(cfg *Config) {
	switch strings.ToLower(cfg.Logging.Level) {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
		log.Warn().Str("level", cfg.Logging.Level).Msg("Unknown log level, default INFO")
	}
}

func toFloat64(v interface{}) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int, int32, int64:
		return float64(reflect.ValueOf(t).Int()), true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func commitAfterError(consumer *kafka.Consumer, m *kafka.Message, reason string, db *pgxpool.Pool) {
	insertSensorErrorRow(context.Background(), db, "", reason, m.Value,
		int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))

	tp := kafka.TopicPartition{
		Topic:     m.TopicPartition.Topic,
		Partition: m.TopicPartition.Partition,
		Offset:    m.TopicPartition.Offset + 1,
	}
	if _, err := consumer.CommitOffsets([]kafka.TopicPartition{tp}); err != nil {
		log.Error().Err(err).Str("reason", reason).Interface("tp", tp).Msg("Commit failed after error")
	} else {
		offsetsCommittedTotal.Inc()
		log.Debug().Str("reason", reason).Interface("tp", tp).Msg("Committed after error")
	}
}

// ------------------- Kafka Message Processing -------------------
func processMessage(
	ctx context.Context,
	m *kafka.Message,
	db *pgxpool.Pool,
	_ *kafka.Producer,
	dlqChan chan<- DLQMessage,
	out chan<- MetricRow,
	consumer *kafka.Consumer,
	_ int,

) {
	messagesReceivedTotal.Inc()

	// 1️⃣ Parse JSON into new KafkaMessage struct
	var km KafkaMessage
	if err := json.Unmarshal(m.Value, &km); err != nil {
		// Insert into sensor_errors
		insertSensorErrorRow(ctx, db, "", "json_unmarshal_failed", m.Value,
			int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))
		log.Info().Msg("Inserted sensor error row reason=json_unmarshal_failed")
		// ✅ Commit offset even for unrecoverable errors
		tp := kafka.TopicPartition{
			Topic:     m.TopicPartition.Topic,
			Partition: m.TopicPartition.Partition,
			Offset:    m.TopicPartition.Offset + 1, // commit next offset
		}
		_, commitErr := consumer.CommitOffsets([]kafka.TopicPartition{tp})
		if commitErr != nil {
			log.Error().Err(commitErr).Msg("Failed to commit offset for JSON error")
		} else {
			log.Info().Msg("Committed offset for JSON error")
		}

		return
	}

	switch strings.ToUpper(km.Topic.Type) {
	case "NBIRTH":
		log.Info().
			Str("edgeNodeId", km.Topic.EdgeNodeID).
			Str("groupId", km.Topic.GroupID).
			Msg("Processing NBIRTH message")

		// Ensure NBIRTH table exists (lazy creation)
		_, err := db.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS nbirth (
				id SERIAL PRIMARY KEY,
				edge_node_id TEXT NOT NULL,
				metric_name TEXT NOT NULL,
				metric_timestamp BIGINT NOT NULL,
				created_at TIMESTAMP DEFAULT now()
			);
		`)
		if err != nil {
			insertSensorErrorRow(ctx, db, "", fmt.Sprintf("nbirth_table_create_failed: %v", err), m.Value,
				int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))
			// Commit on unrecoverable error
			tp := kafka.TopicPartition{Topic: m.TopicPartition.Topic, Partition: m.TopicPartition.Partition, Offset: m.TopicPartition.Offset + 1}
			_, _ = consumer.CommitOffsets([]kafka.TopicPartition{tp})
			return
		}

		// Minimal NBIRTH insertion (name + timestamp + edgeNodeId)
		for _, metric := range km.Payload.Metrics {
			// Ignore extra fields; only store name + timestamp
			if strings.TrimSpace(metric.Name) == "" || metric.Timestamp <= 0 {
				insertSensorErrorRow(ctx, db, "", "nbirth_metric_invalid", m.Value,
					int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))
				// continue to next metric; we still commit the message after the loop
				continue
			}

			_, err := db.Exec(ctx, `
				INSERT INTO nbirth (edge_node_id, metric_name, metric_timestamp)
				VALUES ($1, $2, $3);
			`, km.Topic.EdgeNodeID, metric.Name, metric.Timestamp)
			if err != nil {
				insertSensorErrorRow(ctx, db, "", fmt.Sprintf("nbirth_insert_failed: %v", err), m.Value,
					int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))
				// continue to next metric; we still commit the message after the loop
				continue
			}
		}

		// ✅ Commit after NBIRTH handling
		tp := kafka.TopicPartition{Topic: m.TopicPartition.Topic, Partition: m.TopicPartition.Partition, Offset: m.TopicPartition.Offset + 1}
		_, commitErr := consumer.CommitOffsets([]kafka.TopicPartition{tp})
		if commitErr != nil {
			log.Error().Err(commitErr).Msg("Failed to commit offset for NBIRTH")
		} else {
			log.Info().Msg("Committed offset for NBIRTH")
		}
		return

	case "DBIRTH":
		log.Info().
			Str("edgeNodeId", km.Topic.EdgeNodeID).
			Str("groupId", km.Topic.GroupID).
			Msg("Processing DBIRTH message")

		// Validate presence of deviceId
		if strings.TrimSpace(km.Topic.DeviceID) == "" {
			insertSensorErrorRow(ctx, db, "", "dbirth_missing_device_id", m.Value,
				int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))
			// Commit on unrecoverable error
			tp := kafka.TopicPartition{Topic: m.TopicPartition.Topic, Partition: m.TopicPartition.Partition, Offset: m.TopicPartition.Offset + 1}
			_, _ = consumer.CommitOffsets([]kafka.TopicPartition{tp})
			return
		}

		// Ensure DBIRTH table exists (lazy creation)
		_, err := db.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS dbirth (
				id SERIAL PRIMARY KEY,
				edge_node_id TEXT NOT NULL,
				device_id TEXT NOT NULL,
				metric_name TEXT NOT NULL,
				metric_timestamp BIGINT NOT NULL,
				data_type TEXT NOT NULL,
				created_at TIMESTAMP DEFAULT now()
			);
		`)
		if err != nil {
			insertSensorErrorRow(ctx, db, "", fmt.Sprintf("dbirth_table_create_failed: %v", err), m.Value,
				int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))
			tp := kafka.TopicPartition{Topic: m.TopicPartition.Topic, Partition: m.TopicPartition.Partition, Offset: m.TopicPartition.Offset + 1}
			_, _ = consumer.CommitOffsets([]kafka.TopicPartition{tp})
			return
		}

		// Minimal DBIRTH insertion (name + timestamp + datatype + edgeNodeId + deviceId)
		for _, metric := range km.Payload.Metrics {
			if strings.TrimSpace(metric.Name) == "" || metric.Timestamp <= 0 || strings.TrimSpace(metric.DataType) == "" {
				insertSensorErrorRow(ctx, db, "", "dbirth_metric_invalid", m.Value,
					int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))
				continue
			}

			_, err := db.Exec(ctx, `
				INSERT INTO dbirth (edge_node_id, device_id, metric_name, metric_timestamp, data_type)
				VALUES ($1, $2, $3, $4, $5);
			`, km.Topic.EdgeNodeID, km.Topic.DeviceID, metric.Name, metric.Timestamp, metric.DataType)
			if err != nil {
				insertSensorErrorRow(ctx, db, "", fmt.Sprintf("dbirth_insert_failed: %v", err), m.Value,
					int(m.TopicPartition.Partition), int64(m.TopicPartition.Offset))
				continue
			}
		}

		// ✅ Commit after DBIRTH handling
		tp := kafka.TopicPartition{Topic: m.TopicPartition.Topic, Partition: m.TopicPartition.Partition, Offset: m.TopicPartition.Offset + 1}
		_, commitErr := consumer.CommitOffsets([]kafka.TopicPartition{tp})
		if commitErr != nil {
			log.Error().Err(commitErr).Msg("Failed to commit offset for DBIRTH")
		} else {
			log.Info().Msg("Committed offset for DBIRTH")
		}
		return
	} // ⬅️ Close the switch

	// 2️⃣ Lookup or create all reference IDs (cached in memory)
	groupID, err := lookupOrCreateRefID(ctx, db, groupCache, &refMu,
		"group_ref", "group_id", km.Topic.GroupID)
	if err != nil {
		log.Error().Err(err).Msg("Failed to resolve group_ref")
		return
	}

	nodeID, err := lookupOrCreateRefID(ctx, db, nodeCache, &refMu,
		"edge_node", "edge_node_id", km.Topic.EdgeNodeID, "group_id_no", groupID)
	if err != nil {
		log.Error().Err(err).Msg("Failed to resolve edge_node")
		return
	}

	deviceID, err := lookupOrCreateRefID(ctx, db, deviceCache, &refMu,
		"device", "device_id", km.Topic.DeviceID, "node_id_no", nodeID)
	if err != nil {
		log.Error().Err(err).Msg("Failed to resolve device")
		return
	}

	typeID, err := lookupOrCreateRefID(ctx, db, typeCache, &refMu,
		"data_class", "type", km.Topic.Type)
	if err != nil {
		log.Error().Err(err).Msg("Failed to resolve data_class")
		return
	}

	payloadTs := time.UnixMilli(km.Payload.Timestamp)

	// 3️⃣ Process each metric in the payload
	for _, metric := range km.Payload.Metrics {
		// (a) numeric value conversion
		//val, ok := toFloat64(metric.Value)
		var val float64
		var ok bool

		switch v := metric.Value.(type) {
		case bool:
			val = 0.0
			if v {
				val = 1.0
			}
			ok = true
		default:
			val, ok = toFloat64(v)
		}

		if !ok {
			select {
			case dlqChan <- DLQMessage{
				Key:       metric.Name,
				Value:     m.Value,
				Reason:    "non_numeric_value",
				Partition: int(m.TopicPartition.Partition),
				Offset:    int64(m.TopicPartition.Offset),
			}:
			default:
				log.Error().Str("metric", metric.Name).Msg("DLQ channel full — dropped non-numeric metric")
			}
			continue
		}

		// (b) metric name ID lookup

		metricNameID, err := lookupOrCreateMetricName(ctx, db, metricNameCache, &refMu,
			metric.Name, metric.DataType, deviceID)
		if err != nil {
			select {
			case dlqChan <- DLQMessage{
				Key:       metric.Name,
				Value:     m.Value,
				Reason:    fmt.Sprintf("metric_name_lookup_failed: %v", err),
				Partition: int(m.TopicPartition.Partition),
				Offset:    int64(m.TopicPartition.Offset),
			}:
			default:
				log.Error().Str("metric", metric.Name).Msg("DLQ channel full — dropped lookup error")
			}
			continue
		}

		// (c) Build the MetricRow for DB insertion
		row := MetricRow{
			MetricTimestamp:  time.UnixMilli(metric.Timestamp),
			PayloadTimestamp: payloadTs,
			Value:            val,

			MetricNameNo: metricNameID,
			DeviceIDNo:   deviceID,
			NodeIDNo:     nodeID,
			GroupIDNo:    groupID,
			TypeNo:       typeID,

			Partition: int(m.TopicPartition.Partition),
			Offset:    int64(m.TopicPartition.Offset),

			Msg: *m,
		}

		// (d) Send row to batch processor
		select {
		case out <- row:
			rowsEnqueued.Inc()
			rowsReceivedTotal.Inc()
		case <-ctx.Done():
			return
		}
	}

	datapointsReceivedTotal.Add(float64(len(km.Payload.Metrics)))
}

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

	id, err := lookupOrCreateRef(ctx, db, tableName, columnName, value, fkColumn, fkValue)
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

	// Insert or update metric_name with data_type
	sql := `
        INSERT INTO metric_name (name, data_type, device_id_no)
        VALUES ($1, $2, $3)
        ON CONFLICT (name) DO UPDATE SET data_type = EXCLUDED.data_type
        RETURNING metric_name_no;
    `

	var id int
	if err := db.QueryRow(ctx, sql, name, dataType, deviceIDNo).Scan(&id); err != nil {
		return 0, fmt.Errorf("insert metric_name failed: %w", err)
	}

	// Update cache
	mu.Lock()
	cache[name] = id
	mu.Unlock()

	return id, nil
}

// ------------------- DB -------------------
func createDBPool(cfg *Config) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.Database.User, cfg.Database.Password, cfg.Database.Host,
		cfg.Database.Port, cfg.Database.Name)
	return pgxpool.New(context.Background(), dsn)
}

// preloadReferenceCaches loads existing reference data from DB into memory.
// This reduces insert overhead during ingestion.
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

	log.Info().Msg("✅ All reference caches preloaded successfully")
	return nil
}

// ------------------- Deduplication Globals -------------------

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	// PostgreSQL duplicate error code is 23505
	return strings.Contains(err.Error(), "duplicate key value violates unique constraint") ||
		strings.Contains(err.Error(), "SQLSTATE 23505")
}

// ------------------- DB Insert (COPY with Dedup) -------------------
func insertBatchCopy(ctx context.Context, db *pgxpool.Pool, batch []MetricRow, workerID int) error {
	if len(batch) == 0 {
		return nil
	}

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

	// ⏱️ 8️⃣ Log duration and throughput

	// ✅ Log with realistic duration and throughput
	log.Info().
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
	return nil
}

// asyncDLQWriter runs as a background goroutine and writes messages to DLQ.
func asyncDLQWriter(ctx context.Context, producer *kafka.Producer, db *pgxpool.Pool, dlqChan <-chan DLQMessage) {
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("DLQ writer shutting down")
			return

		case msg := <-dlqChan:
			// ✅ Send to Kafka DLQ topic
			cfg := currentConfig.Load().(*Config)

			err := producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &cfg.DLQ.Topic,
					Partition: kafka.PartitionAny,
				},
				Key:   []byte(msg.Key),
				Value: msg.Value,
				Headers: []kafka.Header{
					{Key: "error", Value: []byte(msg.Reason)},
					{Key: "ts", Value: []byte(time.Now().Format(time.RFC3339))},
				},
			}, nil)

			if err != nil {
				log.Error().
					Err(err).
					Str("key", msg.Key).
					Msg("Failed to write DLQ message to Kafka")
			}

			// ✅ Always insert into DB as a fallback
			insertSensorErrorRow(ctx, db, msg.Key, msg.Reason, msg.Value, msg.Partition, msg.Offset)
		}
	}
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
			log.Info().
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

// ------------------- Main -------------------
func main() {
	zerolog.TimeFieldFormat = time.RFC3339
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()

	cfg, err := loadConfig("config.yaml")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load config")
	}
	if err := validateConfig(cfg); err != nil {
		log.Fatal().Err(err).Msg("Invalid config")
	}
	currentConfig.Store(cfg)
	applyLogLevel(cfg)

	// DB pool
	dbPool, err := createDBPool(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("DB connection failed")
	}
	defer dbPool.Close()
	if err := preloadReferenceCaches(ctx, dbPool); err != nil {
		log.Fatal().Err(err).Msg("Failed to preload sensor cache")
	}

	// Prometheus
	prometheus.MustRegister(
		messagesTotal, batchesTotal, errorsTotal, batchSizeHist, retriesTotal,
		rowsInsertedTotal, rowsFailedTotal, rowsEnqueued,
		messagesReceivedTotal, rowsReceivedTotal, datapointsReceivedTotal, offsetsCommittedTotal,
	)
	// ------------------- HTTP Metrics Server -------------------
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := dbPool.Ping(ctx); err != nil {
			http.Error(w, "DB not ready", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok"))
	})

	httpAddr := fmt.Sprintf("0.0.0.0:%d", cfg.HTTP.MetricsPort)
	httpServer := &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	// Start HTTP server in background
	go func() {
		log.Info().Msgf("HTTP endpoints: /metrics /readyz at %s", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("HTTP server failed")
		}
	}()

	// ✅ Confluent Kafka DLQ Producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":            strings.Join(cfg.Kafka.Brokers, ","),
		"queue.buffering.max.messages": 1000000,
		"linger.ms":                    10,
		"batch.num.messages":           10000,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Confluent DLQ producer")
	}
	defer func() {
		log.Info().Msg("Flushing and closing DLQ producer...")
		producer.Flush(5000) // wait up to 5s for pending messages
		producer.Close()
	}()

	// ✅ Background delivery-report handler
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Error().
						Str("topic", *ev.TopicPartition.Topic).
						Err(ev.TopicPartition.Error).
						Msg("DLQ delivery failed")
				}
			}
		}
	}()

	// ✅ Channel for DLQ messages
	dlqChan := make(chan DLQMessage, 1000)
	go asyncDLQWriter(ctx, producer, dbPool, dlqChan)

	// Confluent Kafka
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  strings.Join(cfg.Kafka.Brokers, ","),
		"group.id":           cfg.Kafka.GroupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false, // we’ll commit manually after DB success
		"session.timeout.ms": 6000,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Kafka consumer")
	}
	defer func() {
		log.Info().Msg("Closing Kafka consumer...")
		consumer.Close()
	}()

	// ✅ Subscribe to the topic
	if err := consumer.SubscribeTopics([]string{cfg.Kafka.Topic}, nil); err != nil {
		log.Fatal().Err(err).Msg("Failed to subscribe to topic")
	}

	msgChan := make(chan kafka.Message, cfg.Quill.BatchSize*2)
	rowChan := make(chan MetricRow, cfg.Quill.BatchSize*2)

	// Barrier channel to signal workers to flush and exit
	flushBarrier := make(chan struct{})

	// Batch workers
	for i := 0; i < cfg.Quill.Workers; i++ {
		go batchProcessor(ctx, dbPool, rowChan, consumer, i, flushBarrier)
	}

	// Consumer workers
	for i := 0; i < cfg.Quill.Consumers; i++ {
		go func(id int) {
			for {
				select {
				case <-ctx.Done():
					return
				case m := <-msgChan:
					processMessage(ctx, &m, dbPool, producer, dlqChan, rowChan, consumer, id)
				}
			}
		}(i)
	}

	// Reader loop with backpressure
	// Kafka consumer loop
	go func() {
		log.Info().Msg("Starting Confluent Kafka consumer poll loop")

		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("Kafka consumer exiting (context canceled)")
				return
			default:
				ev := consumer.Poll(100) // poll every 100 ms
				if ev == nil {
					continue
				}

				switch e := ev.(type) {
				case *kafka.Message:
					// ✅ forward message to channel
					select {
					case msgChan <- *e:
					case <-ctx.Done():
						return
					}

				case kafka.Error:
					log.Error().Err(e).Msg("Kafka error event")
				default:
					// ignore other events (logs/stats)
				}
			}
		}
	}()

	// Wait for termination signal
	<-ctx.Done()
	log.Warn().Msg("🛑 Shutdown signal received, flushing in-flight data...")

	// Step 1: stop reading new Kafka messages
	log.Info().Msg("Stopping Kafka reader...")
	log.Info().Msg("Closing Kafka consumer...")
	consumer.Close()

	// Step 2: signal all batch workers to flush and exit
	close(flushBarrier)
	log.Info().Msg("Waiting for batch workers to finish final flush...")
	time.Sleep(3 * time.Second) // optional small grace delay

	// Graceful HTTP shutdown
	log.Info().Msg("Stopping HTTP metrics server...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("Failed to shut down HTTP server cleanly")
	} else {
		log.Info().Msg("HTTP metrics server stopped")
	}

	// Step 3: close DB pool
	log.Info().Msg("Closing DB connection pool...")
	dbPool.Close()

	log.Info().Msg("✅ QUILL shutdown complete — all data flushed")

}
