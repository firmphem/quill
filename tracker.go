package main

import (
	"fmt"
	"sync"
	"time"
)

type Tracker struct {
	mu            sync.Mutex
	timings       map[string][]time.Duration
	reportEvery   time.Duration
	resetEveryN   int
	reportCounter int
}

// -----------------------------------------------------------------------------
func NewTracker(reportEvery time.Duration, resetEveryN int) *Tracker {
	t := &Tracker{
		timings:     make(map[string][]time.Duration),
		reportEvery: reportEvery,
		resetEveryN: resetEveryN,
	}

	go t.reportingLoop()
	return t
}

// -----------------------------------------------------------------------------
func (t *Tracker) Start(stage string) func() {
	start := time.Now()
	return func() {
		duration := time.Since(start)

		t.mu.Lock()
		t.timings[stage] = append(t.timings[stage], duration)
		t.mu.Unlock()
	}
}

// -----------------------------------------------------------------------------
func (t *Tracker) reportingLoop() {
	ticker := time.NewTicker(t.reportEvery)
	for range ticker.C {
		t.reportAndMaybeReset()
	}
}

// -----------------------------------------------------------------------------
func (t *Tracker) reportAndMaybeReset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	fmt.Println("=== Metrics Report ===")
	for stage, durations := range t.timings {
		if len(durations) == 0 {
			continue
		}

		var total time.Duration
		for _, d := range durations {
			total += d
		}
		avg := total / time.Duration(len(durations))

		fmt.Printf("  %-20s avg: %v, events count:%d\n", stage, avg, len(durations))
	}
	fmt.Println("======================")

	t.reportCounter++

	if t.reportCounter >= t.resetEveryN {
		t.timings = make(map[string][]time.Duration)
		t.reportCounter = 0
	}
}
