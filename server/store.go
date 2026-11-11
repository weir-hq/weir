package main

import (
	"math"
	"sort"
	"sync"
	"time"
)

const (
	windowSeconds = 10
	retention     = 6 * time.Hour
	// Older waiting reports aren't current: a stopped scraper's last "0 waiting" would look healthy.
	waitingStaleSeconds = 60
)

// Counts are the additive per-window aggregates of one stage, built from the ingested data points.
type Counts struct {
	Received      map[string]float64 // by upstream stage
	Originated    float64
	Emitted       float64
	Passed        float64
	Done          float64
	Dropped       map[string]float64 // by reason
	Vanished      float64
	Errored       float64
	Retried       float64
	ContextErrors float64
	AgeAtEntry    Hist
	AgeAtExit     Hist
	TimeInStage   Hist
}

// add records one counter or histogram point. Unknown names are ignored.
func (c *Counts) add(p *Point) {
	v := 0.0
	if p.Value != nil {
		v = *p.Value
	}
	switch p.Name {
	case metricStageReceivedItems:
		c.Received = mergeMap(c.Received, map[string]float64{p.Attributes[attrUpstreamStageName]: v})
	case metricStageOriginatedItems:
		c.Originated += v
	case metricStageEmittedItems:
		c.Emitted += v
	case metricStageRetries:
		c.Retried += v
	case metricStageContextErrors:
		c.ContextErrors += v
	case metricStageCompletedItems:
		switch p.Attributes[attrItemOutcome] {
		case "passed":
			c.Passed += v
		case "done":
			c.Done += v
		case "dropped":
			c.Dropped = mergeMap(c.Dropped, map[string]float64{p.Attributes[attrDropReason]: v})
		case "vanished":
			c.Vanished += v
		case "errored":
			c.Errored += v
		}
	case metricItemAge, metricStageProcessDuration:
		if p.Histogram == nil {
			return
		}
		switch {
		case p.Name == metricStageProcessDuration:
			c.TimeInStage.Merge(*p.Histogram)
		case p.Attributes[attrStageBoundary] == "entry":
			c.AgeAtEntry.Merge(*p.Histogram)
		case p.Attributes[attrStageBoundary] == "exit":
			c.AgeAtExit.Merge(*p.Histogram)
		}
	}
}

func (c *Counts) Merge(o *Counts) {
	c.Received = mergeMap(c.Received, o.Received)
	c.Dropped = mergeMap(c.Dropped, o.Dropped)
	c.Originated += o.Originated
	c.Emitted += o.Emitted
	c.Passed += o.Passed
	c.Done += o.Done
	c.Vanished += o.Vanished
	c.Errored += o.Errored
	c.Retried += o.Retried
	c.ContextErrors += o.ContextErrors
	c.AgeAtEntry.Merge(o.AgeAtEntry)
	c.AgeAtExit.Merge(o.AgeAtExit)
	c.TimeInStage.Merge(o.TimeInStage)
}

// In is every item that entered the stage.
func (c *Counts) In() float64 { return sum(c.Received) + c.Originated }

// Closed is every item that reached an outcome.
func (c *Counts) Closed() float64 {
	return c.Passed + c.Done + sum(c.Dropped) + c.Vanished + c.Errored
}

func mergeMap(dst, src map[string]float64) map[string]float64 {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = map[string]float64{}
	}
	for k, v := range src {
		dst[k] += v
	}
	return dst
}

func sum(m map[string]float64) float64 {
	t := 0.0
	for _, v := range m {
		t += v
	}
	return t
}

// Gauge is a point-in-time value reported by one SDK instance.
type Gauge struct {
	InFlight       float64
	OldestInFlight *float64 // origin event time of the oldest open item
}

// Wait is what a scraper reported about the items waiting to reach a stage from one upstream stage.
type Wait struct {
	Items   float64
	Counted bool     // false when the scraper did not count the items, e.g. for a big table
	Oldest  *float64 // birth time of the oldest waiting item; nil when unknown or nothing waits
}

type Window struct {
	Counts  Counts
	Gauges  map[string]Gauge           // by instance; the latest report in the window wins
	Waiting map[string]map[string]Wait // by instance, then by upstream stage; the latest report wins
}

type Series struct {
	Pipeline string
	Stage    string
	Windows  map[int64]*Window // by window start (unix seconds)
}

func (s *Series) Label(name string) string {
	switch name {
	case "pipeline":
		return s.Pipeline
	case "stage":
		return s.Stage
	}
	return ""
}

// CountsIn merges every window that starts in [from, to).
func (s *Series) CountsIn(from, to int64) Counts {
	var c Counts
	for start, w := range s.Windows {
		if start >= from && start < to {
			c.Merge(&w.Counts)
		}
	}
	return c
}

// GaugeAt sums the gauges of the latest window starting before t. ok is false if none.
func (s *Series) GaugeAt(t int64) (inFlight float64, oldest *float64, ok bool) {
	best := int64(math.MinInt64)
	for start, w := range s.Windows {
		if start < t && start > best && len(w.Gauges) > 0 {
			best = start
		}
	}
	if best == math.MinInt64 {
		return 0, nil, false
	}
	return s.Windows[best].sumGauges()
}

// InFlightBefore is the number of items in flight at t. Without a report before t (e.g. after
// a server restart) it is derived from the first window: gauge - items in + items closed.
func (s *Series) InFlightBefore(t int64) float64 {
	if v, _, ok := s.GaugeAt(t); ok {
		return v
	}
	first := int64(math.MaxInt64)
	for start, w := range s.Windows {
		if start >= t && start < first && len(w.Gauges) > 0 {
			first = start
		}
	}
	if first == math.MaxInt64 {
		return 0
	}
	w := s.Windows[first]
	g, _, _ := w.sumGauges()
	return g - w.Counts.In() + w.Counts.Closed()
}

// WaitingAt is each upstream's latest waiting report before t, skipping stale ones.
func (s *Series) WaitingAt(t int64) map[string]Wait {
	latest := map[string]int64{} // upstream -> start of its latest window with a report
	for start, w := range s.Windows {
		if start >= t || t-start > waitingStaleSeconds+windowSeconds {
			continue
		}
		for _, byUpstream := range w.Waiting {
			for up := range byUpstream {
				if start > latest[up] {
					latest[up] = start
				}
			}
		}
	}
	out := map[string]Wait{}
	for up, start := range latest {
		out[up] = s.Windows[start].sumWaiting()[up]
	}
	return out
}

func (w *Window) sumWaiting() map[string]Wait {
	out := map[string]Wait{}
	for _, byUpstream := range w.Waiting {
		for up, wt := range byUpstream {
			total, seen := out[up]
			total.Counted = wt.Counted && (total.Counted || !seen) // counted only if every report counted
			total.Items += wt.Items
			if wt.Oldest != nil && (total.Oldest == nil || *wt.Oldest < *total.Oldest) {
				v := *wt.Oldest
				total.Oldest = &v
			}
			out[up] = total
		}
	}
	return out
}

func (w *Window) sumGauges() (inFlight float64, oldest *float64, ok bool) {
	for _, g := range w.Gauges {
		inFlight += g.InFlight
		if g.OldestInFlight != nil && (oldest == nil || *g.OldestInFlight < *oldest) {
			v := *g.OldestInFlight
			oldest = &v
		}
	}
	return inFlight, oldest, len(w.Gauges) > 0
}

// Point is one data point of one metric from the Weir conventions. It has a value or a histogram.
type Point struct {
	Name       string            `json:"name"`
	Attributes map[string]string `json:"attributes"`
	Value      *float64          `json:"value"`
	Histogram  *Hist             `json:"histogram"`
}

// IngestPayload is the body of POST /ingest: one SDK instance's data points for one flush interval.
type IngestPayload struct {
	Resource     map[string]string `json:"resource"`
	TimeUnixNano int64             `json:"time_unix_nano"`
	// "cumulative": totals since StartTimeUnixNano (cumulative.go). Otherwise: counts for this report.
	Temporality       string  `json:"temporality"`
	StartTimeUnixNano int64   `json:"start_time_unix_nano"`
	Metrics           []Point `json:"metrics"`
}

type Store struct {
	mu        sync.RWMutex
	series    map[string]*Series
	senders   map[string]*sender // by service.instance.id, for cumulative reports
	startedAt time.Time
	lastGC    time.Time
}

func NewStore() *Store {
	return &Store{series: map[string]*Series{}, senders: map[string]*sender{}, startedAt: time.Now()}
}

func windowStart(ts float64) int64 {
	return int64(ts) / windowSeconds * windowSeconds
}

func (s *Store) Ingest(p IngestPayload) {
	ts := float64(p.TimeUnixNano) / 1e9
	start := windowStart(ts)
	instance := p.Resource[attrServiceInstanceID]
	s.mu.Lock()
	defer s.mu.Unlock()
	var snd *sender
	if p.Temporality == "cumulative" {
		if snd = s.cumulativeSender(instance, p); snd == nil {
			return // a repeat of a report already counted
		}
	}
	// Gauges replace the instance's previous report for the window as a whole, so collect them first.
	gauges := map[*Window]Gauge{}
	waiting := map[*Window]map[string]Wait{}
	for i := range p.Metrics {
		pt := &p.Metrics[i]
		pipeline, stage := pt.Attributes[attrPipelineName], pt.Attributes[attrStageName]
		if pipeline == "" || stage == "" {
			continue
		}
		w := s.window(pipeline, stage, start)
		switch pt.Name {
		case metricItemActive:
			if pt.Value != nil {
				g := gauges[w]
				g.InFlight = *pt.Value
				gauges[w] = g
			}
		case metricItemOldestAge:
			if pt.Value != nil {
				g := gauges[w]
				oldest := ts - *pt.Value // stored as the origin event time of the oldest open item
				g.OldestInFlight = &oldest
				gauges[w] = g
			}
		case metricItemWaiting, metricItemOldestWaitingAge:
			if pt.Value == nil {
				continue
			}
			if waiting[w] == nil {
				waiting[w] = map[string]Wait{}
			}
			up := pt.Attributes[attrUpstreamStageName]
			wt := waiting[w][up]
			if pt.Name == metricItemWaiting {
				wt.Items = *pt.Value
				wt.Counted = true
			} else {
				born := ts - *pt.Value // stored as the birth time of the oldest waiting item
				wt.Oldest = &born
			}
			waiting[w][up] = wt
		default:
			if snd != nil {
				pt = snd.delta(pt)
			}
			if pt != nil {
				w.Counts.add(pt)
			}
		}
	}
	for w, g := range gauges {
		w.Gauges[instance] = g
	}
	for w, byUpstream := range waiting {
		w.Waiting[instance] = byUpstream
	}
	if time.Since(s.lastGC) > time.Minute {
		s.gc()
	}
}

// window returns the window starting at start for one stage, creating it if needed. Callers must hold s.mu.
func (s *Store) window(pipeline, stage string, start int64) *Window {
	key := pipeline + "\x00" + stage
	sr := s.series[key]
	if sr == nil {
		sr = &Series{Pipeline: pipeline, Stage: stage, Windows: map[int64]*Window{}}
		s.series[key] = sr
	}
	w := sr.Windows[start]
	if w == nil {
		w = &Window{Gauges: map[string]Gauge{}, Waiting: map[string]map[string]Wait{}}
		sr.Windows[start] = w
	}
	return w
}

func (s *Store) gc() {
	s.lastGC = time.Now()
	cutoff := time.Now().Add(-retention).Unix()
	for instance, snd := range s.senders {
		if snd.lastTime/1e9 < cutoff {
			delete(s.senders, instance)
		}
	}
	for key, sr := range s.series {
		for start := range sr.Windows {
			if start < cutoff {
				delete(sr.Windows, start)
			}
		}
		if len(sr.Windows) == 0 {
			delete(s.series, key)
		}
	}
}

// Select returns the series matching every matcher. Callers must hold s.mu.
// LabelValues lists the distinct values of a label over the series that match ms.
func (s *Store) LabelValues(label string, ms []Matcher) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seen := map[string]bool{}
	for _, sr := range s.Select(ms) {
		if v := sr.Label(label); v != "" {
			seen[v] = true
		}
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func (s *Store) Select(ms []Matcher) []*Series {
	var out []*Series
	for _, sr := range s.series {
		match := true
		for _, m := range ms {
			if !m.Matches(sr.Label(m.Label)) {
				match = false
				break
			}
		}
		if match {
			out = append(out, sr)
		}
	}
	return out
}
