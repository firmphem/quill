package main

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

// just for debugging datapointsReceivedTotal
var datapointsReceivedAtomic atomic.Uint64

var (
	isInstanceLeader *prometheus.GaugeVec

	// db related
	messagesSavedToDBTotal          *prometheus.CounterVec
	messagesSavedToSensorErrorTotal *prometheus.CounterVec
	dpWrittenIntoDBTotal            *prometheus.CounterVec
	dpCountPerMessageHistogram      *prometheus.HistogramVec
	dbInsertDuration                *prometheus.HistogramVec

	// kafka
	groupConsumerRebalanceTotal *prometheus.CounterVec
	kafkaErrorsByErrorTotal     *prometheus.CounterVec

	// db related
	dbInsertRetryTotal *prometheus.CounterVec
	dbOpRetryTotal     *prometheus.CounterVec

	// offsets
	kafkaOffsetCommitTotal           *prometheus.CounterVec
	lastMessageTimestampPerPartition *prometheus.GaugeVec

	serviceUptimeSeconds *prometheus.GaugeVec

	ackChannelUtilizationRatio *prometheus.GaugeVec
)

// -----------------------------------------------------------------------------
func monitorChannel(ctx context.Context, name string, ch chan Ack) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	capacity := cap(ch)
	log.Info().Str("channel", name).Int("cap", capacity).Str("channel", name).Msg("started channel monitoring")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fillRatio := float64(len(ch)) / float64(capacity)
			ackChannelUtilizationRatio.WithLabelValues(name).Set(fillRatio)
		}
	}
}

// -----------------------------------------------------------------------------
func initMetrics() {
	labelsPerDatabaseInsertMethod := []string{"insert_method"}
	labelsEmpty := []string{}

	cfg := currentConfig.Load().(*Config)
	constLabels := prometheus.Labels{"service": cfg.Quill.ServiceName}

	isInstanceLeader = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "quill_is_leader",
		Help:        "Whether this instance is currently the leader (1 = leader, 0 = not leader)",
		ConstLabels: constLabels,
	}, labelsEmpty)

	messagesSavedToDBTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "quill_messages_processed_total",
		Help:        "Total number of payload saved to DB",
		ConstLabels: constLabels,
	}, labelsEmpty)

	messagesSavedToSensorErrorTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "quill_messages_failed_total",
		Help:        "Total number of messages routed to sensor_errors",
		ConstLabels: constLabels,
	}, labelsEmpty)

	dpWrittenIntoDBTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "quill_datapoints_written_total",
		Help:        "Total number of messages routed to sensor_errors",
		ConstLabels: constLabels,
	}, labelsEmpty)

	dpCountPerMessageHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:        "quill_batch_size_histogram",
			Help:        "histogram of actual batch sizes flushed",
			Buckets:     []float64{10, 100, 1000, 10000, 100000, 1000000},
			ConstLabels: constLabels,
		}, labelsEmpty)

	dbInsertDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:        "quill_db_operation_duration_seconds",
			Help:        "Latency of database insert operations in seconds",
			Buckets:     []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			ConstLabels: constLabels,
		},
		labelsPerDatabaseInsertMethod,
	)

	groupConsumerRebalanceTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "quill_kafka_rebalances_total",
		Help:        "Total number of rebalance event counter",
		ConstLabels: constLabels,
	}, labelsEmpty)

	kafkaErrorsByErrorTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "quill_kafka_errors_total",
		Help:        "Total number kafka errors labelled by error",
		ConstLabels: constLabels,
	}, []string{"kafka_error"})

	dbInsertRetryTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "quill_db_insert_failures_total",
		Help:        "Total number db insert retries",
		ConstLabels: constLabels,
	}, labelsPerDatabaseInsertMethod)

	dbOpRetryTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "quill_db_retries_total",
		Help:        "Total number DB retry counter",
		ConstLabels: constLabels,
	}, labelsEmpty)

	kafkaOffsetCommitTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:        "quill_offset_commits_total",
		Help:        "Total number of offset commits by status",
		ConstLabels: constLabels,
	}, []string{"status"})

	serviceUptimeSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "quill_uptime_seconds",
		Help:        "Uptime",
		ConstLabels: constLabels,
	}, labelsEmpty)

	ackChannelUtilizationRatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "quill_channel_utilization_ratio",
		Help:        "channel fill level vs configured channel_size",
		ConstLabels: constLabels,
	}, []string{"channel_name"})

	lastMessageTimestampPerPartition = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:        "quill_last_message_received_timestamp",
		Help:        "The Unix timestamp of the last message received per partition",
		ConstLabels: constLabels,
	}, []string{"partition"})

	isInstanceLeader.WithLabelValues().Set(0)
	serviceUptimeSeconds.WithLabelValues().Set(0)

	messagesSavedToDBTotal.WithLabelValues().Add(0)
	messagesSavedToSensorErrorTotal.WithLabelValues().Add(0)
	dpWrittenIntoDBTotal.WithLabelValues().Add(0)
	groupConsumerRebalanceTotal.WithLabelValues().Add(0)
	// kafkaErrorsByErrorTotal.WithLabelValues()
	dbInsertRetryTotal.WithLabelValues(DbInsertMethodCopy).Add(0)
	dbOpRetryTotal.WithLabelValues().Add(0)
	kafkaOffsetCommitTotal.WithLabelValues(KafkaOffsetSuccessful).Add(0)
	kafkaOffsetCommitTotal.WithLabelValues(KafkaOffsetFailed).Add(0)

	dpCountPerMessageHistogram.WithLabelValues()
	dbInsertDuration.WithLabelValues(DbInsertMethodCopy)
	dbInsertDuration.WithLabelValues(DbInsertMethodInsert)

	ackChannelUtilizationRatio.WithLabelValues("ack").Set(0)

}

// -----------------------------------------------------------------------------
func startPrometheusEndpoint(ctx context.Context) *http.Server {
	cfg := currentConfig.Load().(*Config)

	initMetrics()

	prometheus.MustRegister(
		isInstanceLeader, messagesSavedToDBTotal, messagesSavedToSensorErrorTotal, dpWrittenIntoDBTotal,
		dpCountPerMessageHistogram, dbInsertDuration, groupConsumerRebalanceTotal, kafkaErrorsByErrorTotal,
		dbInsertRetryTotal, dbOpRetryTotal, kafkaOffsetCommitTotal, serviceUptimeSeconds, ackChannelUtilizationRatio,
		lastMessageTimestampPerPartition,
	)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	httpAddr := fmt.Sprintf("0.0.0.0:%d", cfg.HTTP.MetricsPort)
	httpServer := &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	// Start HTTP server in background
	go func() {
		log.Info().Msgf("HTTP endpoints: /metrics at %s", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("HTTP server failed")
		}
	}()

	// just for debugging we will print DPS to the stdout
	go func() {
		start := time.Now()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		var last uint64

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				uptime := time.Since(start).Seconds()
				serviceUptimeSeconds.WithLabelValues().Set(uptime)

				// DPS
				current := datapointsReceivedAtomic.Load()
				delta := current - last
				last = current

				log.Info().
					Float64("dps since start", float64(current)/uptime).
					Float64("dps last 10s", float64(delta)/10).
					Msg("prometheus report")
			}
		}

	}()
	return httpServer
}

func startHTTP(ctx context.Context) *http.Server {
	return startPrometheusEndpoint(ctx)
}

func gracefulHTTPShutdown(server *http.Server) {
	log.Info().Msg("Stopping HTTP metrics server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}
