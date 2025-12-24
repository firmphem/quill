// Maintains the quill config and different helper functions around config and logging

package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

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
		BatchSize         int           `yaml:"batch_size"`
		BatchTimeout      time.Duration `yaml:"batch_timeout_seconds"`
		ChannelSize       int           `yaml:"channel_size"`
		KafkaOffsetCommit struct {
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
	if config.Database.Password == "" {
		return nil, fmt.Errorf("QUILL_DB_PASSWORD not set")
	}

	if exitAfterNMessages > 0 {
		config.Quill.KafkaOffsetCommit.EveryMessages = 1
		config.Quill.KafkaOffsetCommit.EveryMilliseconds = 500000
	}

	return config, nil
}

// -----------------------------------------------------------------------------
func validateConfig(cfg *Config) error {
	if len(cfg.Kafka.Brokers) == 0 || cfg.Kafka.Topic == "" || cfg.Kafka.GroupID == "" {
		return fmt.Errorf("kafka config invalid")
	}
	if cfg.Database.Host == "" || cfg.Database.User == "" || cfg.Database.Name == "" {
		return fmt.Errorf("database config invalid")
	}
	if cfg.Quill.BatchSize <= 0 || cfg.Quill.BatchTimeout <= 0 {
		return fmt.Errorf("quill config invalid")
	}
	return nil
}

// -----------------------------------------------------------------------------
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
	log.WithLevel(zerolog.NoLevel).
		Msgf("log level is currently set to: %s", zerolog.GlobalLevel().String())
}
