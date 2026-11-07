package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// ingest posts one payload at unix second ts, as an SDK or a scraper would.
func ingest(t *testing.T, s *Store, instance string, ts int64, metrics string) {
	t.Helper()
	body := fmt.Sprintf(`{"resource": {"service.instance.id": %q}, "time_unix_nano": %d, "metrics": [%s]}`, instance, ts*1e9, metrics)
	var p IngestPayload
	if err := json.Unmarshal([]byte(body), &p); err != nil {
		t.Fatal(err)
	}
	s.Ingest(p)
}

const (
	producer = `"weir.pipeline.name": "orders", "weir.stage.name": "producer"`
	enrich   = `"weir.pipeline.name": "orders", "weir.stage.name": "enrich"`
	edge     = `"weir.pipeline.name": "orders", "weir.stage.name": "enrich", "weir.upstream_stage.name": "producer"`
)

func query(t *testing.T, s *Store, src string, from, to int64) *Result {
	t.Helper()
	q, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	res, err := Execute(s, q, from, to)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// Producer sent 10, enrich received 7, and the waiting count grew from 2 to 3: one more item is
// waiting, so 2 are unaccounted for between the two stages.
func TestTransitSeparatesWaitingFromLost(t *testing.T) {
	s := NewStore()
	s.lastGC = time.Now() // the test data is older than the retention
	ingest(t, s, "scraper", 985, `{"name": "weir.item.waiting", "attributes": {`+edge+`}, "value": 2}`)
	ingest(t, s, "producer-1", 995, `{"name": "weir.stage.emitted.items", "attributes": {`+producer+`}, "value": 10}`)
	ingest(t, s, "enrich-1", 995, `{"name": "weir.stage.received.items", "attributes": {`+edge+`}, "value": 7}`)
	ingest(t, s, "scraper", 1005, `{"name": "weir.item.waiting", "attributes": {`+edge+`}, "value": 3},
		{"name": "weir.item.oldest_waiting.age", "attributes": {`+edge+`}, "value": 40}`)

	res := query(t, s, `transit({pipeline="orders"})`, 990, 1010)
	if len(res.Rows) != 1 {
		t.Fatalf("want one edge, got %v", res.Rows)
	}
	row := map[string]any{}
	for i, col := range res.Columns {
		row[col] = res.Rows[0][i]
	}
	want := map[string]any{
		"edge": "producer → enrich", "sent": 10.0, "received": 7.0, "waiting_start": 2.0, "waiting_end": 3.0,
		"unaccounted": 2.0, "oldest_waiting_s": 45.0, "status": "ok", // born at 1005-40 = 965, now 1010
	}
	for col, v := range want {
		if row[col] != v {
			t.Errorf("%s = %v, want %v", col, row[col], v)
		}
	}
}

// A consumer that stopped receives nothing, so it has no received edge; the waiting edge must
// still show up in the graph, and turn amber once the oldest item has waited too long.
func TestGraphShowsWaitingEdgeOfStoppedStage(t *testing.T) {
	s := NewStore()
	s.lastGC = time.Now()
	ingest(t, s, "producer-1", 995, `{"name": "weir.stage.emitted.items", "attributes": {`+producer+`}, "value": 10}`)
	ingest(t, s, "scraper", 1005, `{"name": "weir.item.waiting", "attributes": {`+edge+`}, "value": 1200},
		{"name": "weir.item.oldest_waiting.age", "attributes": {`+edge+`}, "value": 95}`)

	res := query(t, s, `graph({pipeline="orders"})`, 990, 1010)
	if len(res.Edges) != 1 {
		t.Fatalf("want the waiting edge, got %+v", res.Edges)
	}
	e := res.Edges[0]
	if e.From != "orders/producer" || e.To != "orders/enrich" || e.Waiting != 1200.0 || e.OldestWaitingAge != 100.0 || e.Status != "warn" {
		t.Fatalf("unexpected edge: %+v", e)
	}
}

// A scraper that stopped reporting must not leave its last numbers on screen as the current state.
func TestStaleWaitingIsNotShown(t *testing.T) {
	s := NewStore()
	s.lastGC = time.Now()
	ingest(t, s, "scraper", 1005, `{"name": "weir.item.waiting", "attributes": {`+edge+`}, "value": 0}`)
	if got := s.series["orders\x00enrich"].WaitingAt(1005 + 5*60); len(got) != 0 {
		t.Fatal("a report from 5 minutes ago must not count as the current state")
	}
}

// A table scraped without count(*) reports only the oldest age: the edge shows it, but no
// waiting count and no unaccounted, rather than a wrong 0.
func TestUncountedEdgeShowsAgeButNoCount(t *testing.T) {
	s := NewStore()
	s.lastGC = time.Now()
	agg := `"weir.pipeline.name": "orders", "weir.stage.name": "aggregate", "weir.upstream_stage.name": "worker"`
	ingest(t, s, "scraper", 985, `{"name": "weir.item.oldest_waiting.age", "attributes": {`+agg+`}, "value": 20}`)
	ingest(t, s, "scraper", 1005, `{"name": "weir.item.oldest_waiting.age", "attributes": {`+agg+`}, "value": 80}`)

	res := query(t, s, `graph({pipeline="orders"})`, 990, 1010)
	if len(res.Edges) != 1 || res.Edges[0].Waiting != nil || res.Edges[0].OldestWaitingAge != 85.0 {
		t.Fatalf("unexpected edge: %+v", res.Edges)
	}
	res = query(t, s, `transit({pipeline="orders"})`, 990, 1010)
	if row := res.Rows[0]; row[3] != nil || row[4] != nil || row[5] != nil {
		t.Fatalf("want no counts and no unaccounted, got %v", row)
	}
}

// Edges of one stage scraped at different intervals each keep their latest report.
func TestEdgesScrapedAtDifferentIntervals(t *testing.T) {
	s := NewStore()
	s.lastGC = time.Now()
	other := `"weir.pipeline.name": "orders", "weir.stage.name": "enrich", "weir.upstream_stage.name": "backfill"`
	ingest(t, s, "scraper", 975, `{"name": "weir.item.waiting", "attributes": {`+other+`}, "value": 4}`)
	ingest(t, s, "scraper", 1005, `{"name": "weir.item.waiting", "attributes": {`+edge+`}, "value": 7}`)
	got := s.series["orders\x00enrich"].WaitingAt(1010)
	if got["backfill"].Items != 4 || got["producer"].Items != 7 {
		t.Fatalf("want both edges, got %+v", got)
	}
}
