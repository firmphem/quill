package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

var tracker *Tracker

type stats struct {
	total time.Duration
	count int64
}

type Stage struct {
	t       *Tracker
	name    string
	started time.Time
}

func (s *Stage) end() {
	if s.t == nil || s.name == "" {
		return
	}

	if !s.started.IsZero() {
		s.t.record(s.name, time.Since(s.started))
	}

	s.name = ""
	s.started = time.Time{}
}

type Tracker struct {
	enabled  atomic.Bool
	mu       sync.Mutex
	active   map[string][]time.Time // currenly active stage
	stages   map[string]*stats      // data for all closed/completed stages
	overhead int64                  // overhead, ie.. spent inside the tracker
	logger   zerolog.Logger
	interval time.Duration
	stopCh   chan struct{}
	ctx      context.Context
}

func newTracker(ctx context.Context, interval time.Duration, logger zerolog.Logger) *Tracker {
	t := &Tracker{
		active:   make(map[string][]time.Time),
		stages:   make(map[string]*stats),
		logger:   logger,
		interval: interval,
		stopCh:   make(chan struct{}),
		ctx:      ctx,
	}
	go t.loop()
	return t
}

func (t *Tracker) enable()  { t.enabled.Store(true) }
func (t *Tracker) disable() { t.enabled.Store(false); t.reset() }

func (t *Tracker) reset() {
	t.mu.Lock()
	t.active = make(map[string][]time.Time)
	t.stages = make(map[string]*stats)
	atomic.StoreInt64(&t.overhead, 0)
	t.mu.Unlock()
}

func (t *Tracker) startStage(name string) Stage {
	if !t.enabled.Load() {
		return Stage{}
	}

	startOver := time.Now()
	defer t.addOverhead(startOver)

	return Stage{
		t:       t,
		name:    name,
		started: time.Now(),
	}
}

func (t *Tracker) record(name string, dur time.Duration) {
	if !t.enabled.Load() {
		return
	}
	startOver := time.Now()
	defer t.addOverhead(startOver)

	t.mu.Lock()

	s := t.stages[name]
	if s == nil {
		s = &stats{}
		t.stages[name] = s
	}
	s.total += dur
	s.count++

	t.mu.Unlock()
}

func (t *Tracker) addOverhead(start time.Time) {
	if t.enabled.Load() {
		atomic.AddInt64(&t.overhead, time.Since(start).Nanoseconds())
	}
}

func (t *Tracker) loop() {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.report()
		case <-t.stopCh:
			return
		}
	}
}

func (t *Tracker) report() {
	if !t.enabled.Load() {
		return
	}

	t.mu.Lock()
	stages := t.stages
	t.stages = make(map[string]*stats) // reset
	over := atomic.SwapInt64(&t.overhead, 0)
	t.mu.Unlock()

	if len(stages) == 0 && over == 0 {
		return
	}

	evt := t.logger.Info()

	// internal collection just for sorting purposes
	type row struct {
		name  string
		total time.Duration
		avg   time.Duration
		count int64
	}

	rows := make([]row, 0, len(stages))
	maxNameLen := 0

	for name, s := range stages {
		avg := time.Duration(0)
		if s.count > 0 {
			avg = s.total / time.Duration(s.count)
		}
		rows = append(rows, row{name, s.total, avg, s.count})
		if len(name) > maxNameLen {
			maxNameLen = len(name)
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].total > rows[j].total
	})

	for _, r := range rows {
		fmt.Printf("%*s: %s/%s/%d\n", maxNameLen, r.name, fmtDuration(r.total), fmtDuration(r.avg), r.count)
	}

	evt = evt.Str("tracker_overhead", time.Duration(over).String())
	evt.Msg("stage statistics")
}

func (t *Tracker) stop() {
	close(t.stopCh)
}
