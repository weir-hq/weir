package plugin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// fakeWeir answers like the Weir server, with one canned body per query function.
func fakeWeir(t *testing.T, bodies map[string]string) *Datasource {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/functions":
			_, _ = w.Write([]byte(`["graph","leak","oldest"]`))
		case "/api/query":
			q := r.URL.Query().Get("q")
			if body, ok := bodies[q]; ok {
				_, _ = w.Write([]byte(body))
				return
			}
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"unknown function"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	inst, err := NewDatasource(context.Background(), backend.DataSourceInstanceSettings{URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	return inst.(*Datasource)
}

func runQuery(t *testing.T, d *Datasource, expr string) backend.DataResponse {
	t.Helper()
	body, _ := json.Marshal(queryModel{Expr: expr})
	resp, err := d.QueryData(context.Background(), &backend.QueryDataRequest{Queries: []backend.DataQuery{{
		RefID:     "A",
		JSON:      body,
		TimeRange: backend.TimeRange{From: time.Unix(1000, 0), To: time.Unix(1060, 0)},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return resp.Responses["A"]
}

func TestSeries(t *testing.T) {
	d := fakeWeir(t, map[string]string{`oldest({pipeline="orders"})`: `{
		"type": "series", "unit": "s", "from": 1000, "to": 1060,
		"series": [{"name": "enrich", "labels": {"pipeline": "orders", "stage": "enrich"},
			"points": [[1000000, 4.5], [1010000, null]]}]}`})
	r := runQuery(t, d, `oldest({pipeline="orders"})`)
	if r.Error != nil {
		t.Fatal(r.Error)
	}
	if len(r.Frames) != 1 {
		t.Fatalf("got %d frames", len(r.Frames))
	}
	v := r.Frames[0].Fields[1]
	if v.Labels["stage"] != "enrich" || v.Config.Unit != "s" || v.Len() != 2 {
		t.Fatalf("value field: labels %v, unit %q, len %d", v.Labels, v.Config.Unit, v.Len())
	}
	if got := v.At(0).(*float64); got == nil || *got != 4.5 {
		t.Fatalf("first value = %v", got)
	}
	if v.At(1).(*float64) != nil {
		t.Fatal("null point should stay null")
	}
}

func TestTable(t *testing.T) {
	d := fakeWeir(t, map[string]string{`leak({})`: `{
		"type": "table", "columns": ["stage", "in", "ok"],
		"rows": [["enrich", 10, true], ["batcher", null, false]]}`})
	r := runQuery(t, d, `leak({})`)
	if r.Error != nil {
		t.Fatal(r.Error)
	}
	f := r.Frames[0]
	want := []data.FieldType{data.FieldTypeNullableString, data.FieldTypeNullableFloat64, data.FieldTypeNullableBool}
	for i, w := range want {
		if got := f.Fields[i].Type(); got != w {
			t.Fatalf("column %d type = %s, want %s", i, got, w)
		}
	}
}

func TestGraph(t *testing.T) {
	d := fakeWeir(t, map[string]string{`graph({})`: `{
		"type": "graph",
		"nodes": [
			{"id": "orders/producer", "pipeline": "orders", "stage": "producer", "status": "ok", "rate_per_sec": 5, "oldest_age": null},
			{"id": "orders/enrich", "pipeline": "orders", "stage": "enrich", "status": "bad", "rate_per_sec": 4.9, "oldest_age": 12, "vanished": 3}
		],
		"edges": [{"from": "orders/producer", "to": "orders/enrich", "status": "warn", "rate_per_sec": 5, "waiting": 7}]}`})
	r := runQuery(t, d, `graph({})`)
	if r.Error != nil {
		t.Fatal(r.Error)
	}
	if len(r.Frames) != 2 || r.Frames[0].Name != "nodes" || r.Frames[1].Name != "edges" {
		t.Fatalf("want nodes and edges frames, got %d", len(r.Frames))
	}
	nodes := r.Frames[0]
	bad, _ := nodes.FieldByName("arc__bad")
	if bad.At(1).(float64) != 1 || bad.At(0).(float64) != 0 {
		t.Fatal("enrich should be red and producer not")
	}
	edges := r.Frames[1]
	color, _ := edges.FieldByName("color")
	if color.At(0).(string) != "orange" {
		t.Fatalf("edge color = %v", color.At(0))
	}
}

func TestErrors(t *testing.T) {
	d := fakeWeir(t, nil)
	r := runQuery(t, d, `nope({})`)
	if r.Error == nil || r.Error.Error() != "unknown function" || r.Status != backend.StatusBadRequest {
		t.Fatalf("error = %v, status %d", r.Error, r.Status)
	}
	h, _ := d.CheckHealth(context.Background(), &backend.CheckHealthRequest{})
	if h.Status != backend.HealthStatusOk {
		t.Fatalf("health: %s", h.Message)
	}
}
