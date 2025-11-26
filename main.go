package main

import (
	"context"
	"os"
	"os/signal"
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

var (
	// key = "timestamp|sensorID"
	dedupCache sync.Map // key: string, value: time.Time
	// TTL for cache entries
	dedupTTL = 10 * time.Minute
)

var (
	dedupMu sync.Mutex
	tracker *Tracker
)

func init() {
	tracker = NewTracker(3*time.Second, 12)
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

	httpServer := startPrometheusEndpoint(ctx, dbPool)

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
