package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ------------------- Globals -------------------
var currentConfig atomic.Value // stores *Config
var insertBatchFn = insertBatchCopy

var exitAfterNMessages int32

var (
	// key = "timestamp|sensorID"
	dedupCache sync.Map // key: string, value: time.Time
	// TTL for cache entries
	dedupTTL = 10 * time.Minute
)

var (
	dedupMu sync.Mutex
)

// -----------------------------------------------------------------------------
func init() {
	var x int
	log.Logger = setupLogging()

	flag.StringVar(&configFile, "config", "", "path to config yaml")
	flag.IntVar(&x, "exit-after-n-messages", 0, "exit after processing N messages. this is for debug mode only")
	flag.Parse()
	exitAfterNMessages = int32(x)

	if configFile == "" {
		log.Info().Msg("config file was not provided. going to try a default one 'config.yaml'")
		configFile = "config.yaml"
	}
	log.Info().Msg(fmt.Sprintf("config file was set to '%v'", configFile))

}

// -----------------------------------------------------------------------------
func setupLogging() zerolog.Logger {
	zerolog.TimeFieldFormat = time.RFC3339

	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		fn := runtime.FuncForPC(pc)
		if fn == nil {
			return "unknown"
		}
		return filepath.Base(fn.Name())
	}

	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}

	return zerolog.New(output).
		With().
		Timestamp().
		Caller().
		Logger()
}

// -----------------------------------------------------------------------------
func loadAndInitConfig() *Config {
	cfg, err := loadConfig(configFile)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load config")
	}
	if err := validateConfig(cfg); err != nil {
		log.Fatal().Err(err).Msg("Invalid config")
	}
	currentConfig.Store(cfg)
	applyLogLevel(cfg)
	return cfg
}

// -----------------------------------------------------------------------------
func initKafkaConsumer(cfg *Config) *kafka.Consumer {
	kcfg := &kafka.ConfigMap{
		"bootstrap.servers":  strings.Join(cfg.Kafka.Brokers, ","),
		"group.id":           cfg.Kafka.GroupID,
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
	}

	consumer, err := kafka.NewConsumer(kcfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kafka consumer create: %v\n", err)
		os.Exit(2)
	}
	return consumer
}

// -----------------------------------------------------------------------------
func main() {
	cfg := loadAndInitConfig()

	globalCtx, globalCancel := context.WithCancel(context.Background())
	defer globalCancel()

	httpServer := startPrometheusEndpoint(globalCtx)

	dbPool, err := createDBPool(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("DB connection failed")
	}
	defer dbPool.Close()

	err = retryUntilDone(globalCtx, "createConsumerMetadataTable",
		func(ctx context.Context) error {
			e := createConsmumerMetadataTable(ctx, dbPool)
			if e != nil {
				dbOpRetryTotal.WithLabelValues().Inc()
			}
			return e
		},
		isRetriableErr,
		"failure",
		func(finalErr error) error {
			log.Fatal().Err(finalErr).Msg("cannot create metadata table, exiting")
			return nil
		})
	if err != nil {
		return
	}

	validateKafkaTopicAgainstPrevConsumed(globalCtx, dbPool, topicName, cfg.Kafka.Topic)
	waitForLeadership(globalCtx, globalCancel)

	tracker = newTracker(globalCtx, 10*time.Second, log.Logger)
	if cfg.Tracker.Enabled {
		tracker.enable()
	} else {
		log.Info().Msg("tracker disabled")
	}
	defer tracker.stop()

	go runConsumer(globalCtx, cfg, dbPool)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	select {
	case s := <-sig:
		log.Info().Str("signal", s.String()).Msg("shutdown requested via OS signal")
	case <-globalCtx.Done():
		log.Info().Msg("shutdown requested internally")
	}

	globalCancel()

	gracefulHTTPShutdown(httpServer)
}
