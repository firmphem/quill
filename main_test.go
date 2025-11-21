package main

import (
	"os"
	"testing"
	"time"
)

// ✅ Test config validation
func TestValidateConfig(t *testing.T) {
	cfg := &Config{}
	err := validateConfig(cfg)
	if err == nil {
		t.Errorf("expected error for empty config, got nil")
	}

	// Fix required fields
	cfg.Kafka.Brokers = []string{"localhost:9092"}
	cfg.Kafka.Topic = "test"
	cfg.Kafka.GroupID = "group1"
	cfg.Database.Host = "localhost"
	cfg.Database.Port = 5432
	cfg.Database.User = "user"
	cfg.Database.Name = "db"
	cfg.Database.Password = "pass"
	cfg.Quill.BatchSize = 10
	cfg.Quill.BatchTimeout = time.Second
	cfg.Quill.Consumers = 1

	if err := validateConfig(cfg); err != nil {
		t.Errorf("expected valid config, got %v", err)
	}
}

// ✅ Test toFloat64
func TestToFloat64(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected float64
		ok       bool
	}{
		{42, 42, true},
		{int64(99), 99, true},
		{3.14, 3.14, true},
		{"bad", 0, false},
	}

	for _, tt := range tests {
		got, ok := toFloat64(tt.input)
		if ok != tt.ok || (ok && got != tt.expected) {
			t.Errorf("toFloat64(%v) = (%v,%v), want (%v,%v)",
				tt.input, got, ok, tt.expected, tt.ok)
		}
	}
}

// ✅ Test loadConfig with env var
func TestLoadConfig(t *testing.T) {
	os.Setenv("QUILL_DB_PASSWORD", "secret")
	defer os.Unsetenv("QUILL_DB_PASSWORD")

	// Write a temporary config file
	content := `
kafka:
  brokers: ["localhost:9092"]
  topic: "test"
  group_id: "group1"
database:
  host: "localhost"
  port: 5432
  user: "user"
  name: "db"
quill:
  batch_size: 100
  batch_timeout_seconds: 5s
  workers: 2
  consumers: 1
dlq:
  topic: "dlq"
logging:
  level: "debug"
`
	tmpfile := "test_config.yaml"
	os.WriteFile(tmpfile, []byte(content), 0644)
	defer os.Remove(tmpfile)

	cfg, err := loadConfig(tmpfile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.Database.Password != "secret" {
		t.Errorf("expected db password from env, got %s", cfg.Database.Password)
	}
}
