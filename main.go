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

	// global context to cancel everything
	globalCtx, globalCancel := context.WithCancel(context.Background())
	defer globalCancel()

	httpServer := startPrometheusEndpoint(globalCtx)

	dbPool, err := createDBPool(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("DB connection failed")
	}
	defer dbPool.Close()

	if err := createConsmumerMetadataTable(globalCtx, dbPool); err != nil {
		log.Error().Err(err).Msg("cannot create a metadata table for consumer. exiting...")
		os.Exit(1)
	}

	// this is just a pre-check if we consume from the correct topic
	validateKafkaTopicAgainstPrevConsumed(globalCtx, dbPool, topicName, cfg.Kafka.Topic)

	waitForLeadership()

	// this is a post-check in order to make sure that new leader will consume from the right topic
	currentStoredTopic := validateKafkaTopicAgainstPrevConsumed(globalCtx, dbPool, topicName, cfg.Kafka.Topic)
	if currentStoredTopic == "" {
		if err := setMetadataValue(globalCtx, dbPool, topicName, cfg.Kafka.Topic); err != nil {
			log.Fatal().Err(err).Msg("cannot update the metadata table")
		}
	}

	tracker = newTracker(globalCtx, time.Second*10, log.Logger)
	if cfg.Tracker.Enabled {
		log.Info().Msg("tracker was enabled")
		tracker.enable()
	} else {
		log.Info().Msg("tracker was disabled")
	}
	defer tracker.stop()

	if err := preloadReferenceCaches(globalCtx, dbPool); err != nil {
		log.Fatal().Err(err).Msg("Failed to preload sensor cache")
	}

	mainConsumer := initKafkaConsumer(cfg)

	// worker map & sync
	workers := make(map[int32]*PartitionWorker)
	var workersMu sync.Mutex
	var workerWG sync.WaitGroup

	// partitions state & guard
	partitions := make(map[int32]*partitionState)
	var partitionsMu sync.Mutex

	ackCh := make(chan Ack, cfg.Quill.ChannelSize) // tune it
	topic := cfg.Kafka.Topic

	ensureWorker := func(partition int32) {
		workersMu.Lock()
		defer workersMu.Unlock()
		if _, ok := workers[partition]; ok {
			return
		}
		w := newPartitionWorker(partition, topic, &workerWG, globalCtx)
		workers[partition] = w
		partitionsMu.Lock()
		if _, ok := partitions[partition]; !ok {
			partitions[partition] = &partitionState{
				lastProcessed: -1,
				lastCommitted: -1,
				msgCount:      0,
			}
		}
		partitionsMu.Unlock()
		w.start(dbPool, ackCh)
	}

	// some rebalance magic happens here
	err = mainConsumer.Subscribe(topic, func(consumer *kafka.Consumer, ev kafka.Event) error {
		switch e := ev.(type) {
		case kafka.AssignedPartitions:
			log.Info().Interface("partitions", e.Partitions).Msg("assigned")
			if err := consumer.Assign(e.Partitions); err != nil {
				log.Error().Err(err).Msg("paratition assigning error")
			}

			/////////////////////////////////
			// fmt.Println("initial offsets:")
			// for _, p := range e.Partitions {
			// 	earliest, latest, err := mainConsumer.QueryWatermarkOffsets(*p.Topic, p.Partition, 5_000)
			// 	if err != nil {
			// 		fmt.Println("  error:", err)
			// 		continue
			// 	}
			//
			// 	fmt.Printf("  %s[%d] earliest=%d highest=%d)\n", *p.Topic, p.Partition, earliest, highest)
			// }
			// /////////////////////////////////

			// create workers for assigned partitions
			for _, tp := range e.Partitions {
				ensureWorker(tp.Partition)
			}
			return nil

		case kafka.RevokedPartitions:
			log.Info().Interface("partitions", e.Partitions).Msg("revoked")

			// commit last processed offsets for revoked partitions (from partition state)
			partitionsMu.Lock()
			var offsets []kafka.TopicPartition
			for _, tp := range e.Partitions {
				ps := partitions[tp.Partition]
				if ps != nil && ps.lastProcessed >= 0 && ps.lastProcessed != ps.lastCommitted {
					offsets = append(offsets, kafka.TopicPartition{
						Topic:     &topic,
						Partition: tp.Partition,
						Offset:    ps.lastProcessed + 1,
					})
				}
			}
			partitionsMu.Unlock()

			if len(offsets) > 0 {
				log.Debug().Interface("offsets", offsets).Msg("committing offsets for revoked partitions")
				if _, err := consumer.CommitOffsets(offsets); err != nil {
					log.Error().Err(err).Msg("commit on revoke failed")
				}
			}

			// stop workers and remove partition state
			workersMu.Lock()
			for _, tp := range e.Partitions {
				if w, ok := workers[tp.Partition]; ok {
					w.stop()
					w.closeMsgCh()
					delete(workers, tp.Partition)
				}
				partitionsMu.Lock()
				delete(partitions, tp.Partition)
				partitionsMu.Unlock()
			}
			workersMu.Unlock()

			if err := consumer.Unassign(); err != nil {
				log.Error().Err(err).Msg("partition unassigning error")
			}
			return nil

		}
		return nil
	})
	if err != nil {
		log.Fatal().Err(err).Msg("cant subscribe topic error")
	}

	// main poll loop
	pollCtx, pollCancel := context.WithCancel(context.Background())
	var pollWG sync.WaitGroup
	pollWG.Add(1)

	// todo:
	// need to monitor channel if they (+ack) are full
	// add exp backup off if required (will see perf test)

	// non-blocking but cpu hungry when is full
	go func() {
		defer pollWG.Done()

		for {
			select {
			case <-pollCtx.Done():
				return
			default:
			}

			s := tracker.startStage("poll")
			ev := mainConsumer.Poll(100)
			if ev == nil {
				s.end()
				continue
			}
			s.end()

			switch e := ev.(type) {
			case *kafka.Message:
				s := tracker.startStage("routing")
				p := e.TopicPartition.Partition
				ensureWorker(p)
				workersMu.Lock()
				w := workers[p]
				workersMu.Unlock()
				if w == nil {
					log.Fatal().Int32("partition", p).Msg("no worker for partition found")
				}

				delivered := false
				for !delivered {
					select {
					case <-pollCtx.Done():
						return
					case w.msgCh <- e:
						delivered = true
					default:
						// exp backup off vs high cpu usage????
						time.Sleep(1 * time.Millisecond)
					}
				}
				s.end()

			case kafka.Error:
				log.Fatal().Err(e).Msg("main consumer error. failing the application. check kafka connection")
			default:
			}
		}
	}()

	// Commit manager: receive acks and commit offsets by batch/time.
	commitDone := make(chan struct{})
	go commitManager(globalCtx, &partitionsMu, partitions, topic, mainConsumer, ackCh, commitDone)

	// everything below is about graceful shutdown of all things
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	<-sig
	log.Info().Msg("shutdown requested...")

	globalCancel()

	pollCancel()
	pollWG.Wait()

	workersMu.Lock()
	for _, w := range workers {
		w.stop()
		w.closeMsgCh()
	}
	workersMu.Unlock()

	workerWG.Wait()

	<-commitDone

	mainConsumer.Close()
	log.Info().Msg("main consumer was closed...")

	gracefulHTTPShutdown(httpServer)

	log.Info().Msg("graceful shutdownn completed...")
}
