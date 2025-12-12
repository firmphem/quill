package main

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// -----------------------------------------------------------------------------
type testCase struct {
	FileName string     `yaml:"-"`
	Name     string     `yaml:"name"`
	Steps    []testStep `yaml:"steps"`
}

type testStep struct {
	Name         string                 `yaml:"name"`
	Type         string                 `yaml:"type"` // SQL, golang, bash
	Func         string                 `yaml:"func"`
	Args         map[string]interface{} `yaml:"args"`
	Pos          []interface{}          `yaml:"positional_args"`
	IgnoreErrors bool                   `yaml:"ignore_errors"`
	PrintOutput  bool                   `yaml:"print_output"`
	Assert       Assert                 `yaml:"assert"`
}

type Assert struct {
	Condition string `yaml:"condition"`
	Expected  any    `yaml:"expected"`
}

// -----------------------------------------------------------------------------
type KafkaCurrentOffsetInfo struct {
	Topic     string
	Partition int
	Offset    int64
}

// -----------------------------------------------------------------------------
func loadConfig(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("cannot read config file: %w", err)
	}
	var c config
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return fmt.Errorf("invalid yaml in config: %w", err)
	}

	if len(c.Kafka.Brokers) == 0 || c.Kafka.Topic == "" {
		return errors.New("kafka configuration incomplete: brokers and topic are required")
	}
	if c.Database.Host == "" || c.Database.Name == "" {
		return errors.New("database configuration incomplete: host and name are required")
	}

	c.Database.Password = os.Getenv("QUILL_DB_PASSWORD")
	if c.Database.Password == "" {
		return fmt.Errorf("QUILL_DB_PASSWORD not set")
	}

	cfg = c
	return nil
}
