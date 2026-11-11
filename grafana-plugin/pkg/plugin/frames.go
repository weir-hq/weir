package plugin

import (
	"fmt"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// result is the body of GET /api/query on the Weir server (server/query.go).
type result struct {
	Type    string   `json:"type"` // series | table | graph
	Unit    string   `json:"unit"`
	Series  []series `json:"series"`
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
	Nodes   []node   `json:"nodes"`
	Edges   []edge   `json:"edges"`
}

type series struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
	Points [][]*float64      `json:"points"` // [unix ms, value|null]
}

type node struct {
	ID            string   `json:"id"`
	Pipeline      string   `json:"pipeline"`
	Stage         string   `json:"stage"`
	Status        string   `json:"status"` // ok | warn | bad
	In            float64  `json:"in"`
	Passed        float64  `json:"passed"`
	Done          float64  `json:"done"`
	Dropped       float64  `json:"dropped"`
	Vanished      float64  `json:"vanished"`
	Errored       float64  `json:"errored"`
	Retried       float64  `json:"retried"`
	InFlight      float64  `json:"in_flight"`
	Unaccounted   float64  `json:"unaccounted"`
	Unreceived    float64  `json:"unreceived"`
	RatePerSec    float64  `json:"rate_per_sec"`
	OldestAge     *float64 `json:"oldest_age"`
	AgeP50        *float64 `json:"age_p50"`
	AgeP99        *float64 `json:"age_p99"`
	InStageAvg    *float64 `json:"in_stage_avg"`
	ContextErrors float64  `json:"context_errors"`
}

type edge struct {
	From             string   `json:"from"`
	To               string   `json:"to"`
	Status           string   `json:"status"` // ok | warn
	Count            float64  `json:"count"`
	RatePerSec       float64  `json:"rate_per_sec"`
	TransitAvg       *float64 `json:"transit_avg"`
	Waiting          *float64 `json:"waiting"`
	OldestWaitingAge *float64 `json:"oldest_waiting_age"`
}

func toFrames(refID string, res *result) (data.Frames, error) {
	switch res.Type {
	case "series":
		return seriesFrames(refID, res), nil
	case "table":
		return data.Frames{tableFrame(refID, res)}, nil
	case "graph":
		return graphFrames(refID, res), nil
	}
	return nil, fmt.Errorf("unknown result type %q", res.Type)
}

// Weir units to Grafana unit ids.
var units = map[string]string{
	"s":     "s",
	"/s":    "suffix:/s",
	"items": "short",
}

// seriesFrames returns one frame per series, labelled by pipeline and stage, which is the shape
// Grafana alerting expects.
func seriesFrames(refID string, res *result) data.Frames {
	frames := make(data.Frames, 0, len(res.Series))
	for _, s := range res.Series {
		times := make([]time.Time, 0, len(s.Points))
		values := make([]*float64, 0, len(s.Points))
		for _, p := range s.Points {
			if len(p) != 2 || p[0] == nil {
				continue
			}
			times = append(times, time.UnixMilli(int64(*p[0])))
			values = append(values, p[1])
		}
		value := data.NewField("value", data.Labels(s.Labels), values)
		value.Config = &data.FieldConfig{DisplayNameFromDS: s.Name, Unit: units[res.Unit]}
		f := data.NewFrame(s.Name, data.NewField("time", nil, times), value)
		f.RefID = refID
		f.Meta = &data.FrameMeta{Type: data.FrameTypeTimeSeriesMulti, TypeVersion: data.FrameTypeVersion{0, 1}}
		frames = append(frames, f)
	}
	return frames
}

// tableFrame types each column by its values: numbers, booleans, or text.
func tableFrame(refID string, res *result) *data.Frame {
	f := data.NewFrame("")
	f.RefID = refID
	for c, name := range res.Columns {
		kind := "number"
		for _, row := range res.Rows {
			if c >= len(row) || row[c] == nil {
				continue
			}
			switch row[c].(type) {
			case float64:
			case bool:
				kind = "bool"
			default:
				kind = "string"
			}
			if kind != "number" {
				break
			}
		}
		switch kind {
		case "number":
			vals := make([]*float64, len(res.Rows))
			for i, row := range res.Rows {
				if c < len(row) {
					if v, ok := row[c].(float64); ok {
						vals[i] = &v
					}
				}
			}
			f.Fields = append(f.Fields, data.NewField(name, nil, vals))
		case "bool":
			vals := make([]*bool, len(res.Rows))
			for i, row := range res.Rows {
				if c < len(row) {
					if v, ok := row[c].(bool); ok {
						vals[i] = &v
					}
				}
			}
			f.Fields = append(f.Fields, data.NewField(name, nil, vals))
		default:
			vals := make([]*string, len(res.Rows))
			for i, row := range res.Rows {
				if c < len(row) && row[c] != nil {
					v := fmt.Sprint(row[c])
					vals[i] = &v
				}
			}
			f.Fields = append(f.Fields, data.NewField(name, nil, vals))
		}
	}
	return f
}

// Arc colors and legend names in the Node graph panel, by Weir status.
var (
	statusColors = map[string]string{"ok": "green", "warn": "orange", "bad": "red"}
	statusNames  = map[string]string{"ok": "OK", "warn": "Old items or retries", "bad": "Lost or unaccounted items"}
)

// graphFrames returns the nodes and edges frames the Node graph panel reads.
func graphFrames(refID string, res *result) data.Frames {
	n := len(res.Nodes)
	ids, titles, subtitles := make([]string, n), make([]string, n), make([]string, n)
	rate, oldest := make([]float64, n), make([]*float64, n)
	arcs := map[string][]float64{"ok": make([]float64, n), "warn": make([]float64, n), "bad": make([]float64, n)}
	details := []struct {
		name, unit string
		get        func(node) *float64
	}{
		{"In", "short", func(x node) *float64 { return &x.In }},
		{"Passed", "short", func(x node) *float64 { return &x.Passed }},
		{"Done", "short", func(x node) *float64 { return &x.Done }},
		{"Dropped", "short", func(x node) *float64 { return &x.Dropped }},
		{"Vanished", "short", func(x node) *float64 { return &x.Vanished }},
		{"Errored", "short", func(x node) *float64 { return &x.Errored }},
		{"Retried", "short", func(x node) *float64 { return &x.Retried }},
		{"In flight", "short", func(x node) *float64 { return &x.InFlight }},
		{"Unaccounted", "short", func(x node) *float64 { return &x.Unaccounted }},
		{"Age p50", "s", func(x node) *float64 { return x.AgeP50 }},
		{"Age p99", "s", func(x node) *float64 { return x.AgeP99 }},
		{"Time in stage", "s", func(x node) *float64 { return x.InStageAvg }},
	}
	detailVals := make([][]*float64, len(details))
	for i := range details {
		detailVals[i] = make([]*float64, n)
	}
	for i, x := range res.Nodes {
		ids[i], titles[i], subtitles[i] = x.ID, x.Stage, x.Pipeline
		rate[i], oldest[i] = x.RatePerSec, x.OldestAge
		status := x.Status
		if _, ok := arcs[status]; !ok {
			status = "ok"
		}
		arcs[status][i] = 1
		for d, det := range details {
			if v := det.get(x); v != nil {
				val := *v
				detailVals[d][i] = &val
			}
		}
	}

	nodes := data.NewFrame("nodes",
		data.NewField("id", nil, ids),
		data.NewField("title", nil, titles),
		data.NewField("subtitle", nil, subtitles),
		withConfig(data.NewField("mainstat", nil, rate), "Rate", "/s"), // Node graph prints the unit as is
		withConfig(data.NewField("secondarystat", nil, oldest), "Oldest in flight", "s"),
	)
	for _, status := range []string{"ok", "warn", "bad"} {
		f := data.NewField("arc__"+status, nil, arcs[status])
		f.Config = &data.FieldConfig{
			DisplayName: statusNames[status],
			Color:       map[string]any{"mode": "fixed", "fixedColor": statusColors[status]},
		}
		nodes.Fields = append(nodes.Fields, f)
	}
	for d, det := range details {
		nodes.Fields = append(nodes.Fields, withConfig(data.NewField(fmt.Sprintf("detail__%d", d), nil, detailVals[d]), det.name, det.unit))
	}

	m := len(res.Edges)
	eids, sources, targets := make([]string, m), make([]string, m), make([]string, m)
	erate, waiting, oldestWaiting, colors := make([]float64, m), make([]*float64, m), make([]*float64, m), make([]string, m)
	for i, e := range res.Edges {
		eids[i], sources[i], targets[i] = e.From+"->"+e.To, e.From, e.To
		erate[i], waiting[i], oldestWaiting[i] = e.RatePerSec, e.Waiting, e.OldestWaitingAge
		colors[i] = statusColors["ok"]
		if c, ok := statusColors[e.Status]; ok {
			colors[i] = c
		}
	}
	edges := data.NewFrame("edges",
		data.NewField("id", nil, eids),
		data.NewField("source", nil, sources),
		data.NewField("target", nil, targets),
		withConfig(data.NewField("mainstat", nil, erate), "Rate", "/s"),
		withConfig(data.NewField("secondarystat", nil, waiting), "Waiting", "short"),
		withConfig(data.NewField("detail__oldest_waiting", nil, oldestWaiting), "Oldest waiting", "s"),
		data.NewField("color", nil, colors),
	)

	for _, f := range []*data.Frame{nodes, edges} {
		f.RefID = refID
		f.Meta = &data.FrameMeta{PreferredVisualization: data.VisTypeNodeGraph}
	}
	return data.Frames{nodes, edges}
}

func withConfig(f *data.Field, displayName, unit string) *data.Field {
	f.Config = &data.FieldConfig{DisplayName: displayName, Unit: unit}
	return f
}
