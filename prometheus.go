// implementation of prometheus endpoint used to monitor quill

package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

// TODO:
// - monitor channel
// -

// just for debugging datapointsReceivedTotal
var datapointsReceivedAtomic atomic.Uint64

var (
	isInstanceLeader        prometheus.Gauge
	messagesTotal           prometheus.Counter
	batchesTotal            prometheus.Counter
	errorsTotal             prometheus.Counter
	retriesTotal            prometheus.Counter
	rowsInsertedTotal       prometheus.Counter
	rowsFailedTotal         prometheus.Counter
	rowsEnqueued            prometheus.Counter
	messagesReceivedTotal   prometheus.Counter
	rowsReceivedTotal       prometheus.Counter
	datapointsReceivedTotal prometheus.Counter
	offsetsCommittedTotal   prometheus.Counter
	serviceUptimeSeconds    prometheus.Gauge
)

// -----------------------------------------------------------------------------
func initMetrics() {
	isInstanceLeader = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: promMetricNameToRealName("quill_is_leader"),
		Help: "Whether this instance is currently the leader (1 = leader, 0 = not leader)",
	})
	messagesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: promMetricNameToRealName("quill_messages_total"),
		Help: "Total number of sensor datapoints processed",
	})
	batchesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: promMetricNameToRealName("quill_batches_total"),
		Help: "Total number of batches inserted",
	})
	errorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: promMetricNameToRealName("quill_errors_total"),
		Help: "Total number of DB insert errors",
	})
	retriesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: promMetricNameToRealName("quill_retries_total"),
		Help: "Total retries due to DB failures",
	})
	rowsInsertedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: promMetricNameToRealName("quill_rows_inserted_total"),
		Help: "Rows successfully inserted",
	})
	rowsFailedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: promMetricNameToRealName("quill_rows_failed_total"),
		Help: "Rows failed during insert",
	})
	rowsEnqueued = prometheus.NewCounter(prometheus.CounterOpts{
		Name: promMetricNameToRealName("quill_rows_enqueued_total"),
		Help: "Rows enqueued from Kafka",
	})
	messagesReceivedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: promMetricNameToRealName("quill_messages_received_total"),
		Help: "Kafka messages consumed",
	})
	rowsReceivedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: promMetricNameToRealName("quill_rows_received_total"),
		Help: "Rows received from Kafka messages",
	})
	datapointsReceivedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: promMetricNameToRealName("quill_datapoints_received_total"),
		Help: "Datapoints received from Kafka",
	})
	offsetsCommittedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: promMetricNameToRealName("quill_offsets_committed_total"),
			Help: "Total number of Kafka offsets successfully committed",
		})
	serviceUptimeSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: promMetricNameToRealName("quill_uptime_seconds"),
			Help: "Service uptime in seconds",
		})
}

// -----------------------------------------------------------------------------
func promMetricNameToRealName(metric string) string {
	cfg := currentConfig.Load().(*Config)
	return strings.Replace(metric, "quill_", "quill_"+cfg.Kafka.Topic+"_", -1)
}

// -----------------------------------------------------------------------------
// func startPrometheusEndpoint(ctx context.Context, dbPool *pgxpool.Pool) *http.Server {
func startPrometheusEndpoint(ctx context.Context) *http.Server {
	cfg := currentConfig.Load().(*Config)

	initMetrics()

	prometheus.MustRegister(
		isInstanceLeader, messagesTotal, batchesTotal, errorsTotal, retriesTotal,
		rowsInsertedTotal, rowsFailedTotal, rowsEnqueued,
		messagesReceivedTotal, rowsReceivedTotal, datapointsReceivedTotal, offsetsCommittedTotal, serviceUptimeSeconds,
	)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	// mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
	// 	if err := dbPool.Ping(ctx); err != nil {
	// 		http.Error(w, "DB not ready", http.StatusServiceUnavailable)
	// 		return
	// 	}
	// 	w.Write([]byte("ok"))
	// })

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
				serviceUptimeSeconds.Set(uptime)

				// DPS
				current := datapointsReceivedAtomic.Load()
				delta := current - last
				last = current

				log.Info().Float64("dps since start", float64(current)/uptime).Float64("dps last 10s", float64(delta)/10).Msg("prometheus dummy report")
			}
		}
	}()

	return httpServer
}

//	func startHTTP(ctx context.Context, dbPool *pgxpool.Pool) *http.Server {
//		return startPrometheusEndpoint(ctx, dbPool)
//	}
func startHTTP(ctx context.Context) *http.Server {
	return startPrometheusEndpoint(ctx)
}

func gracefulHTTPShutdown(server *http.Server) {
	log.Info().Msg("Stopping HTTP metrics server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}
