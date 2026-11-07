package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestIngest(t *testing.T) {
	body := `{
		"resource": {"service.name": "enrich", "service.instance.id": "host-1"},
		"time_unix_nano": 1000000000000,
		"metrics": [
			{"name": "weir.stage.received.items", "attributes": {"weir.pipeline.name": "orders", "weir.stage.name": "enrich", "weir.upstream_stage.name": "producer"}, "value": 5},
			{"name": "weir.stage.completed.items", "attributes": {"weir.pipeline.name": "orders", "weir.stage.name": "enrich", "weir.item.outcome": "passed"}, "value": 3},
			{"name": "weir.stage.completed.items", "attributes": {"weir.pipeline.name": "orders", "weir.stage.name": "enrich", "weir.item.outcome": "dropped", "weir.drop.reason": "duplicate"}, "value": 1},
			{"name": "weir.item.age", "attributes": {"weir.pipeline.name": "orders", "weir.stage.name": "enrich", "weir.stage.boundary": "exit"}, "histogram": {"scale": 3, "offset": -8, "bucket_counts": [4], "sum": 2, "count": 4}},
			{"name": "weir.item.active", "attributes": {"weir.pipeline.name": "orders", "weir.stage.name": "enrich"}, "value": 1},
			{"name": "weir.item.oldest.age", "attributes": {"weir.pipeline.name": "orders", "weir.stage.name": "enrich"}, "value": 30},
			{"name": "weir.not.known.yet", "attributes": {"weir.pipeline.name": "orders", "weir.stage.name": "enrich"}, "value": 9}
		]
	}`
	var p IngestPayload
	if err := json.Unmarshal([]byte(body), &p); err != nil {
		t.Fatal(err)
	}
	s := NewStore()
	s.lastGC = time.Now() // the test data is older than the retention
	s.Ingest(p)

	sr := s.series["orders\x00enrich"]
	if sr == nil {
		t.Fatal("series not created")
	}
	c := sr.CountsIn(0, 2000)
	if c.Received["producer"] != 5 || c.Passed != 3 || c.Dropped["duplicate"] != 1 || c.AgeAtExit.Count != 4 {
		t.Fatalf("unexpected counts: %+v", c)
	}
	inFlight, oldest, ok := sr.GaugeAt(2000)
	if !ok || inFlight != 1 || oldest == nil || *oldest != 970 {
		t.Fatalf("unexpected gauges: %v %v %v", inFlight, oldest, ok)
	}
}

// The names in semconv.go must be exactly the weir.* names defined in conventions/.
func TestSemconvMatchesConventions(t *testing.T) {
	files, _ := filepath.Glob("../conventions/*.yaml")
	var yaml strings.Builder
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		yaml.Write(b)
	}
	defined := map[string]bool{}
	for _, re := range []string{`metric_name: (\S+)`, `- id: (weir\.\S+)`} {
		for _, m := range regexp.MustCompile(re).FindAllStringSubmatch(yaml.String(), -1) {
			defined[m[1]] = true
		}
	}

	src, err := os.ReadFile("semconv.go")
	if err != nil {
		t.Fatal(err)
	}
	inServer := map[string]bool{}
	for _, m := range regexp.MustCompile(`"(weir\.[^"]+)"`).FindAllStringSubmatch(string(src), -1) {
		inServer[m[1]] = true
	}

	for name := range defined {
		if !inServer[name] {
			t.Errorf("%s is in conventions/ but not in semconv.go", name)
		}
	}
	for name := range inServer {
		if !defined[name] {
			t.Errorf("%s is in semconv.go but not in conventions/", name)
		}
	}
}
