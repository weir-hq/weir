package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// report sends one cumulative report from instance "enrich-1", as the SDK does: received total
// and a process-duration histogram total, at unix second ts, for totals started at start.
func report(t *testing.T, s *Store, ts, start int64, received float64, durations ...float64) {
	t.Helper()
	metrics := fmt.Sprintf(`{"name": "weir.stage.received.items", "attributes": {%s}, "value": %v}`, edge, received)
	if len(durations) > 0 {
		var sum float64
		for _, d := range durations {
			sum += d
		}
		metrics += fmt.Sprintf(`, {"name": "weir.stage.process.duration", "attributes": {%s},
			"histogram": {"scale": 3, "offset": -27, "bucket_counts": [%v], "sum": %v, "count": %v}}`, enrich, len(durations), sum, len(durations))
	}
	body := fmt.Sprintf(`{"resource": {"service.instance.id": "enrich-1"}, "time_unix_nano": %d,
		"start_time_unix_nano": %d, "temporality": "cumulative", "metrics": [%s]}`, ts*1e9, start*1e9, metrics)
	var p IngestPayload
	if err := json.Unmarshal([]byte(body), &p); err != nil {
		t.Fatal(err)
	}
	s.Ingest(p)
}

// newTestStore is a store that started before the test's senders, so their totals count from 0.
func newTestStore() *Store {
	s := NewStore()
	s.startedAt = time.Unix(0, 0)
	s.lastGC = time.Now() // the test data is older than the retention
	return s
}

func received(s *Store) float64 {
	return s.series["orders\x00enrich"].CountsIn(0, 1<<40).Received["producer"]
}

func TestLostReportLosesNothing(t *testing.T) {
	s := newTestStore()
	report(t, s, 1005, 1000, 100)
	// The report at 1015 (total 200) never arrives.
	report(t, s, 1025, 1000, 300)
	if got := received(s); got != 300 {
		t.Fatalf("received %v, want 300: the report after the lost one carries its counts", got)
	}
	if got := s.series["orders\x00enrich"].CountsIn(1020, 1030).Received["producer"]; got != 200 {
		t.Fatalf("window 1020: %v, want 200 (100 of them late)", got)
	}
}

func TestRepeatedOrLateReportCountsNothingTwice(t *testing.T) {
	s := newTestStore()
	report(t, s, 1005, 1000, 100)
	report(t, s, 1005, 1000, 100) // delivered twice
	report(t, s, 1015, 1000, 250)
	report(t, s, 1010, 1000, 180) // arrives after a newer report
	if got := received(s); got != 250 {
		t.Fatalf("received %v, want 250", got)
	}
}

func TestRestartCountsFromZero(t *testing.T) {
	s := newTestStore()
	report(t, s, 1005, 1000, 100)
	report(t, s, 1035, 1030, 40) // restarted at 1030: new start time, total back to 40
	if got := received(s); got != 140 {
		t.Fatalf("received %v, want 140", got)
	}
}

func TestTotalGoingDownIsARestart(t *testing.T) {
	s := newTestStore()
	report(t, s, 1005, 1000, 100)
	report(t, s, 1015, 1000, 30) // same start time, but the totals were reset
	if got := received(s); got != 130 {
		t.Fatalf("received %v, want 130", got)
	}
}

// After a server restart, a sender that kept running reports totals that include counts from
// before. They must not land as one huge spike in the current window.
func TestServerRestartDoesNotCountOldTotals(t *testing.T) {
	s := NewStore() // started now: the sender below started long before
	s.lastGC = time.Now()
	report(t, s, 1005, 1000, 1_000_000)
	report(t, s, 1015, 1000, 1_000_050)
	if got := received(s); got != 50 {
		t.Fatalf("received %v, want 50: the first report is only a baseline", got)
	}
}

func TestHistogramsAreSubtracted(t *testing.T) {
	s := newTestStore()
	report(t, s, 1005, 1000, 1, 0.1)
	report(t, s, 1015, 1000, 3, 0.1, 0.1, 0.1)
	h := s.series["orders\x00enrich"].CountsIn(1010, 1020).TimeInStage
	if h.Count != 2 || h.Sum < 0.19 || h.Sum > 0.21 {
		t.Fatalf("window 1010 histogram %+v, want count 2, sum 0.2", h)
	}
}

func TestSeriesFirstReportedLaterCountsFromZero(t *testing.T) {
	s := newTestStore()
	report(t, s, 1005, 1000, 100)
	report(t, s, 1015, 1000, 100, 0.1) // the histogram appears for the first time
	if got := s.series["orders\x00enrich"].CountsIn(1010, 1020).TimeInStage.Count; got != 1 {
		t.Fatalf("histogram count %v, want 1", got)
	}
}
