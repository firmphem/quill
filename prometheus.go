// implementation of prometheus endpoint used to monitor quill

package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

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

// -----------------------------------------------------------------------------
func startPrometheusEndpoint(ctx context.Context, dbPool *pgxpool.Pool) *http.Server {
	cfg := currentConfig.Load().(*Config)

	prometheus.MustRegister(
		messagesTotal, batchesTotal, errorsTotal, batchSizeHist, retriesTotal,
		rowsInsertedTotal, rowsFailedTotal, rowsEnqueued,
		messagesReceivedTotal, rowsReceivedTotal, datapointsReceivedTotal, offsetsCommittedTotal,
	)
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

	return httpServer
}
