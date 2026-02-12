package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

type Metric struct {
	Name      string      `json:"name"`
	Timestamp int64       `json:"timestamp"`
	DataType  string      `json:"dataType"`
	Value     interface{} `json:"value"`
}

type Payload struct {
	Timestamp int64    `json:"timestamp"`
	Metrics   []Metric `json:"metrics"`
	Seq       int      `json:"seq"`
}

type Topic struct {
	Namespace          string `json:"namespace"`
	EdgeNodeDescriptor string `json:"edgeNodeDescriptor"`
	GroupID            string `json:"groupId"`
	EdgeNodeID         string `json:"edgeNodeId"`
	DeviceID           string `json:"deviceId"`
	Type               string `json:"type"`
}

type KafkaMessage struct {
	Topic   Topic   `json:"topic"`
	Payload Payload `json:"payload"`
}

func randomDataType(rng *rand.Rand) string {
	types := []string{"Float", "Double", "Int32", "Bool"}
	return types[rng.Intn(len(types))]
}

func randomMetricName(n int) string {
	return fmt.Sprintf("Signal_%04d", n)
}

func generateMessage(nodeID, metricCount, seq int, rng *rand.Rand) ([]byte, error) {
	devices := []string{"MainMeter", "Rtu", "SCADA", "PCS", "BMS", "Inverter", "BatteryRack"}
	device := devices[rng.Intn(len(devices))]

	topic := Topic{
		Namespace:          "spBv1.0",
		EdgeNodeDescriptor: fmt.Sprintf("DDS/PUC_%03d", nodeID),
		GroupID:            "DDS",
		EdgeNodeID:         fmt.Sprintf("PUC_%03d", nodeID),
		DeviceID:           device,
		Type:               "DDATA",
	}

	now := time.Now().UnixMilli()
	metrics := make([]Metric, metricCount)
	for i := 0; i < metricCount; i++ {
		m := rng.Intn(5000) + 1
		metrics[i] = Metric{
			Name:      randomMetricName(m),
			Timestamp: now - int64(rng.Intn(5000)),
			DataType:  randomDataType(rng),
		}
		switch metrics[i].DataType {
		case "Float", "Double":
			fmt.Println("float")
			metrics[i].Value = rng.Float64() * 100000
		case "Int32":
			fmt.Println("int")
			metrics[i].Value = rng.Intn(100000)
		case "Bool":
			fmt.Println("bool")
			metrics[i].Value = rng.Intn(2) == 0
		}
	}

	msg := KafkaMessage{
		Topic: topic,
		Payload: Payload{
			Timestamp: now,
			Metrics:   metrics,
			Seq:       seq,
		},
	}
	return json.Marshal(msg)
}

func splitBrokers(s string) []string {
	s = strings.ReplaceAll(s, ",", " ")
	return strings.Fields(s)
}

func main() {
	var (
		brokers     string
		topic       string
		nodes       int
		metrics     int
		batchSize   int
		workers     int
		durationStr string
		rate        int
		printJSON   bool
	)

	flag.StringVar(&brokers, "brokers", "", "Kafka brokers (comma or space separated)")
	flag.StringVar(&topic, "topic", "mqtt", "Kafka topic")
	flag.IntVar(&nodes, "nodes", 10, "Number of nodes")
	flag.IntVar(&metrics, "metrics", 100, "Total metric pool size")
	flag.IntVar(&batchSize, "batch-size", 100, "Metrics per Kafka message")
	flag.IntVar(&workers, "workers", 1, "Number of workers")
	flag.StringVar(&durationStr, "duration", "30s", "Run duration (e.g. 30s, 2m)")
	flag.IntVar(&rate, "rate", 0, "Target datapoints per second (0 = unlimited)")
	flag.BoolVar(&printJSON, "print-json", false, "Print sample JSON instead of sending")
	flag.Parse()

	if brokers == "" || nodes == 0 || batchSize == 0 {
		fmt.Println("❌ Required parameters missing. Example:")
		fmt.Println("   go run ./cmd/loadgen -brokers \"10.117.209.25 10.117.209.26\" -topic mqtt -nodes 200 -batch-size 900 -workers 12 -rate 1000000 -duration 60s")
		os.Exit(1)
	}

	brokerList := splitBrokers(brokers)
	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		fmt.Printf("❌ Invalid duration: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerList...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		Async:    true,
	}
	defer writer.Close()

	fmt.Printf("🚀 Deterministic loadgen → topic=%s brokers=%v\n", topic, brokerList)
	fmt.Printf("   nodes=%d metrics=%d batch=%d workers=%d duration=%s rate=%d dp/s\n", nodes, metrics, batchSize, workers, duration, rate)

	if printJSON {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		b, _ := generateMessage(rng.Intn(nodes)+1, batchSize, 0, rng)
		fmt.Println(string(b))
		return
	}

	var totalMsgs, totalDP int64
	start := time.Now()

	// global ticker logging
	stopCh := make(chan struct{})
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		var lastM, lastD int64
		lastT := start
		for {
			select {
			case <-t.C:
				now := time.Now()
				m := atomic.LoadInt64(&totalMsgs)
				d := atomic.LoadInt64(&totalDP)
				dt := now.Sub(lastT).Seconds()
				fmt.Printf("📊 %.1f msgs/s  %.0f dp/s  (total=%d)\n",
					float64(m-lastM)/dt, float64(d-lastD)/dt, d)
				lastM, lastD, lastT = m, d, now
			case <-stopCh:
				return
			}
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			nodeIDs := make([]int, nodes)
			for i := range nodeIDs {
				nodeIDs[i] = i + 1
			}

			msgRatePerWorker := 0.0
			if rate > 0 {
				msgRatePerWorker = float64(rate) / float64(batchSize*workers)
			}
			var ticker *time.Ticker
			if time.Duration(msgRatePerWorker) > 0 {
				interval := time.Second / time.Duration(msgRatePerWorker)
				if interval < time.Microsecond {
					interval = time.Microsecond
				}
				ticker = time.NewTicker(interval)
				defer ticker.Stop()
			}

			seq := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if ticker != nil {
					<-ticker.C
				}

				node := nodeIDs[rng.Intn(nodes)]
				b, err := generateMessage(node, batchSize, seq, rng)
				if err != nil {
					fmt.Printf("⚠️  Worker %d JSON error: %v\n", id, err)
					continue
				}

				msg := kafka.Message{
					Key:   []byte(fmt.Sprintf("PUC_%03d", node)),
					Value: b,
				}

				if err := writer.WriteMessages(ctx, msg); err != nil {
					if ctx.Err() != nil {
						return
					}
					fmt.Printf("⚠️  Worker %d send error: %v\n", id, err)
					continue
				}
				atomic.AddInt64(&totalMsgs, 1)
				atomic.AddInt64(&totalDP, int64(batchSize))
				seq++
			}
		}(w)
	}

	wg.Wait()
	close(stopCh)

	elapsed := time.Since(start)
	msgs := atomic.LoadInt64(&totalMsgs)
	dps := atomic.LoadInt64(&totalDP)
	fmt.Printf("🏁 Done: %d workers, %d msgs, %d datapoints in %s (%.0f dp/s)\n",
		workers, msgs, dps, elapsed, float64(dps)/elapsed.Seconds())
}
