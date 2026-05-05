// Maintains the quill config and different helper functions around config and logging

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

const (
	ErrorMatchEquals   = "equals"
	ErrorMatchContains = "contains"

	kafkaTopicPattern = "%KAFKA_TOPIC%"
)

type ErrorMatcher struct {
	Value   string `yaml:"value"`
	Matched string `yaml:"matched"`
}

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
		DoNotRetireTooYoungMetricThresholdMilliseconds int64         `yaml:"do_not_retire_too_young_metric_threshold_ms"`
		ServiceName                                    string        `yaml:"service_name"`
		BatchSize                                      int           `yaml:"batch_size"`
		BatchTimeout                                   time.Duration `yaml:"batch_timeout_seconds"`
		ChannelSize                                    int           `yaml:"channel_size"`
		KafkaOffsetCommit                              struct {
			EveryMessages     int `yaml:"every_messages"`
			EveryMilliseconds int `yaml:"every_milliseconds"`
		} `yaml:"kafka_offset_commit"`
	} `yaml:"quill"`
	Logging struct {
		Level string `yaml:"level"`
	} `yaml:"logging"`
	HTTP struct {
		MetricsPort int `yaml:"metrics_port"`
	} `yaml:"http"`
	Tracker struct {
		Enabled bool `yaml:"enabled"`
	} `yaml:"tracker"`
	Etcd struct {
		Endpoints       []string `yaml:"endpoints"`
		LeaseTTLSeconds int      `yaml:"leaseTTLSeconds"`
		EtcdLeaderKey   string   `yaml:"etcdLeaderKey"`
	} `yaml:"etcd"`
	NonRetriableErrors   []ErrorMatcher `yaml:"non_retriable_errors"`
	CausingFailureErrors []ErrorMatcher `yaml:"causing_failure_errors"`
}

var configFile string

// -----------------------------------------------------------------------------
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
	if os.Getenv("QUILL_DB_USER") != "" {
		log.Info().Msg("overwrite db user with env var")
		config.Database.User = os.Getenv("QUILL_DB_USER")
	}
	if os.Getenv("QUILL_DB_HOST") != "" {
		log.Info().Msg("overwrite db host with env var")
		config.Database.Host = os.Getenv("QUILL_DB_HOST")
	}
	if os.Getenv("QUILL_DB_NAME") != "" {
		log.Info().Msg("overwrite db name with env var")
		config.Kafka.Brokers = strings.Split(os.Getenv("QUILL_DB_NAME"), ",")
	}
	if os.Getenv("KAFKA_TOPIC") != "" {
		log.Info().Msg("overwrite kafka topic with env var")
		config.Kafka.Topic = os.Getenv("KAFKA_TOPIC")
	}
	if os.Getenv("KAFKA_BROKERS") != "" {
		log.Info().Msg("overwrite kafka topic with env var")
		config.Kafka.Brokers = strings.Split(strings.ReplaceAll(os.Getenv("KAFKA_BROKERS"), ",", " "), " ")
	}

	if config.Quill.ServiceName == "" {
		serviceNameDefault := "consumer-" + config.Kafka.Topic
		log.Info().Msgf("service name is empty will use the default %s", serviceNameDefault)
		config.Quill.ServiceName = serviceNameDefault
	}

	if exitAfterNMessages > 0 {
		config.Quill.KafkaOffsetCommit.EveryMessages = 1
		config.Quill.KafkaOffsetCommit.EveryMilliseconds = 500000
	}

	if strings.Index(config.Etcd.EtcdLeaderKey, kafkaTopicPattern) > -1 {
		config.Etcd.EtcdLeaderKey = strings.Replace(config.Etcd.EtcdLeaderKey, kafkaTopicPattern, config.Kafka.Topic, -1)
	}
	if strings.Index(config.Kafka.GroupID, kafkaTopicPattern) > -1 {
		config.Kafka.GroupID = strings.Replace(config.Kafka.GroupID, kafkaTopicPattern, config.Kafka.Topic, -1)
	}

	return config, nil
}

// -----------------------------------------------------------------------------
func validateConfig(cfg *Config) error {
	// return fmt.Errorf("invalid config: kafka config invalid")
	hasError := false

	if len(cfg.Kafka.Brokers) == 0 || cfg.Kafka.Topic == "" || cfg.Kafka.GroupID == "" {
		hasError = true
		log.Error().Msg("invalid config: kafka config invalid")
	}
	if cfg.Database.Host == "" || cfg.Database.User == "" || cfg.Database.Name == "" {
		hasError = true
		log.Error().Msg("invalid config: database config invalid")
	}
	if cfg.Database.Password == "" {
		hasError = true
		log.Error().Msg("invalid config: QUILL_DB_PASSWORD not set")
	}
	if cfg.Database.User == "" {
		hasError = true
		log.Error().Msg("invalid config: db user not set")
	}
	if cfg.Quill.BatchSize <= 0 {
		hasError = true
		log.Error().Msg("invalid config: batch_size must be greater than 0")
	}
	if cfg.Quill.BatchTimeout <= 0 {
		hasError = true
		log.Error().Msg("invalid config: batch_timeout_seconds must be greater than 0")
	}
	if cfg.Quill.ChannelSize <= 0 {
		hasError = true
		log.Error().Msg("invalid config: channel_size must be greater than 0")
	}
	if cfg.Quill.KafkaOffsetCommit.EveryMessages <= 0 {
		hasError = true
		log.Error().Msg("invalid config: every_messages must be greater than 0")
	}
	if cfg.Quill.KafkaOffsetCommit.EveryMilliseconds <= 0 {
		hasError = true
		log.Error().Msg("invalid config: every_milliseconds must be greater than 0")
	}
	if cfg.Quill.DoNotRetireTooYoungMetricThresholdMilliseconds < 0 {
		hasError = true
		log.Error().Msg("invalid config: do_not_retire_too_young_metric_threshold_ms must be greater or equal to 0")
	}

	if checkErrors(cfg.NonRetriableErrors) {
		hasError = true
	}
	if checkErrors(cfg.CausingFailureErrors) {
		hasError = true
	}

	if hasError {
		return fmt.Errorf("invalid config")
	}

	return nil
}

// -----------------------------------------------------------------------------
func checkErrors(e []ErrorMatcher) bool {
	hasError := false
	for _, errConfig := range e {
		if errConfig.Value == "" {
			hasError = true
			log.Error().Msg("invalid config: error value/string cannot be empty")
		}

		switch errConfig.Matched {
		case ErrorMatchEquals, ErrorMatchContains:
			continue
		default:
			hasError = true
			log.Error().Msgf("invalid match type '%s' for error '%s': must be '%s' or '%s'",
				errConfig.Matched, errConfig.Value, ErrorMatchEquals, ErrorMatchContains)
		}
	}

	return hasError
}

// -----------------------------------------------------------------------------
func applyLogLevel(cfg *Config) {
	switch strings.ToLower(cfg.Logging.Level) {
	case "trace":
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
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
		log.Warn().Msg("unrecognised log level, defaulting to INFO")
	}
}

// -----------------------------------------------------------------------------
func validateKafkaTopicAgainstPrevConsumed(ctx context.Context, dbPool *pgxpool.Pool, metaKey string, expectedTopic string) string {
	storedTopic, err := geCurrentMetadataValue(ctx, dbPool, metaKey)
	if err != nil {
		log.Fatal().Err(err).Msg("we cannot check if run consumer for the same topic or not. exiting...")
	}

	if storedTopic != "" && storedTopic != expectedTopic {
		log.Fatal().Msgf("previously this database was used to consume data from topic '%s' while wanted to consume from '%s'. exiting...", storedTopic, expectedTopic)
	}

	return storedTopic
}
