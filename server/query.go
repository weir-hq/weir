package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

type Result struct {
	Type    string      `json:"type"` // series | table | graph
	Unit    string      `json:"unit,omitempty"`
	From    int64       `json:"from"`
	To      int64       `json:"to"`
	Series  []SeriesOut `json:"series,omitempty"`
	Columns []string    `json:"columns,omitempty"`
	Rows    [][]any     `json:"rows,omitempty"`
	Nodes   []NodeOut   `json:"nodes,omitempty"`
	Edges   []EdgeOut   `json:"edges,omitempty"`
}

type SeriesOut struct {
	Name   string  `json:"name"`
	Points [][]any `json:"points"` // [unix ms, value|null]
}

type NodeOut struct {
	ID            string  `json:"id"`
	Pipeline      string  `json:"pipeline"`
	Stage         string  `json:"stage"`
	Status        string  `json:"status"` // ok | warn | bad
	In            float64 `json:"in"`
	Originated    float64 `json:"originated"`
	Emitted       float64 `json:"emitted"`
	Passed        float64 `json:"passed"`
	Done          float64 `json:"done"`
	Dropped       float64 `json:"dropped"`
	Vanished      float64 `json:"vanished"`
	Errored       float64 `json:"errored"`
	Retried       float64 `json:"retried"`
	ContextErrors float64 `json:"context_errors"`
	InFlight      float64 `json:"in_flight"`
	Unaccounted   float64 `json:"unaccounted"`
	Unreceived    float64 `json:"unreceived"`
	RatePerSec    float64 `json:"rate_per_sec"`
	OldestAge     any     `json:"oldest_age"`
	AgeP50        any     `json:"age_p50"`
	AgeP99        any     `json:"age_p99"`
	InStageAvg    any     `json:"in_stage_avg"`
}

type EdgeOut struct {
	From       string  `json:"from"`
	To         string  `json:"to"`
	Status     string  `json:"status"` // ok | warn
	Count      float64 `json:"count"`
	RatePerSec float64 `json:"rate_per_sec"`
	TransitAvg any     `json:"transit_avg"`
	// From a scraper, when one watches this edge: items waiting now, and the oldest one's age.
	Waiting          any `json:"waiting"`
	OldestWaitingAge any `json:"oldest_waiting_age"`
}

// node is the computed state of one stage over a range.
type node struct {
	sr            *Series
	id            string
	c             Counts
	inFlightStart float64
	inFlightEnd   float64
	oldest        *float64
	unreceived    float64
	upstreams     []string // node ids
	downstreams   []string
	// Scraper reports by upstream stage, at the end and start of the range.
	waitingEnd   map[string]Wait
	waitingStart map[string]Wait
}

// upstreamNames includes edges with waiting items, so a stage that stopped receiving keeps its edge.
func (n *node) upstreamNames() []string {
	seen := map[string]bool{}
	for up := range n.c.Received {
		seen[up] = true
	}
	for up := range n.waitingEnd {
		seen[up] = true
	}
	names := make([]string, 0, len(seen))
	for up := range seen {
		names = append(names, up)
	}
	sort.Strings(names)
	return names
}

// oldestWaitingAge is NaN when unknown or nothing waits.
func (n *node) oldestWaitingAge(up string, end int64) float64 {
	wt, ok := n.waitingEnd[up]
	if !ok || wt.Oldest == nil || (wt.Counted && wt.Items == 0) {
		return math.NaN()
	}
	return float64(end) - *wt.Oldest
}

// edgeStatus warns when the oldest waiting item is far older than the stage's usual entry age,
// so a healthy 5-minute cron doesn't look stuck between runs.
func (n *node) edgeStatus(up string, end int64) string {
	usual := n.c.AgeAtEntry.Mean()
	if math.IsNaN(usual) {
		usual = 0
	}
	if n.oldestWaitingAge(up, end) > math.Max(60, 3*usual) {
		return "warn"
	}
	return "ok"
}

func (n *node) unaccounted() float64 {
	return n.c.In() - n.c.Closed() - (n.inFlightEnd - n.inFlightStart)
}

type evalCtx struct {
	store    *Store
	q        *Query
	from, to int64 // range of the query, unix seconds
}

var functions = map[string]func(*evalCtx) (*Result, error){
	"graph":      evalGraph,
	"leak":       evalLeak,
	"drops":      evalDrops,
	"decompose":  evalDecompose,
	"age":        seriesFunc("s", ageValue),
	"oldest":     seriesFunc("s", oldestValue),
	"in_flight":  seriesFunc("items", inFlightValue),
	"throughput": seriesFunc("/s", throughputValue),
	"lost":       seriesFunc("/s", lostValue),

	"transit":        evalTransit,
	"waiting":        edgeSeriesFunc("items", waitingValue),
	"oldest_waiting": edgeSeriesFunc("s", oldestWaitingValue),
}

func FunctionNames() []string {
	names := make([]string, 0, len(functions))
	for n := range functions {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func Execute(store *Store, q *Query, from, to int64) (*Result, error) {
	fn, ok := functions[q.Func]
	if !ok {
		return nil, fmt.Errorf("unknown function %q; available: %s", q.Func, strings.Join(FunctionNames(), ", "))
	}
	if q.Range > 0 {
		from = to - int64(q.Range.Seconds())
	}
	// Align to window boundaries so counts and gauges cover exactly the same span.
	from = windowStart(float64(from))
	store.mu.RLock()
	defer store.mu.RUnlock()
	res, err := fn(&evalCtx{store: store, q: q, from: from, to: to})
	if res != nil {
		res.From, res.To = from, to
	}
	return res, err
}

// nodes computes every selected stage over the range, plus edges, in topological order.
func (e *evalCtx) nodes() ([]*node, map[string]*node) {
	byID := map[string]*node{}
	var list []*node
	for _, sr := range e.store.Select(e.q.Matchers) {
		n := &node{sr: sr, id: sr.Pipeline + "/" + sr.Stage, c: sr.CountsIn(e.from, e.to)}
		n.inFlightEnd, n.oldest, _ = sr.GaugeAt(e.to)
		n.inFlightStart = sr.InFlightBefore(e.from)
		n.waitingEnd = sr.WaitingAt(e.to)
		n.waitingStart = sr.WaitingAt(e.from)
		byID[n.id] = n
		list = append(list, n)
	}
	for _, n := range list {
		for _, up := range n.upstreamNames() {
			upID := n.sr.Pipeline + "/" + up
			n.upstreams = append(n.upstreams, upID)
			if u := byID[upID]; u != nil {
				u.downstreams = append(u.downstreams, n.id)
			}
		}
	}
	for _, n := range list {
		received := 0.0
		for _, d := range n.downstreams {
			received += byID[d].c.Received[n.sr.Stage]
		}
		n.unreceived = n.c.Emitted - received
	}
	return topoSort(list, byID), byID
}

func topoSort(list []*node, byID map[string]*node) []*node {
	sort.Slice(list, func(i, j int) bool { return list[i].id < list[j].id })
	indeg := map[string]int{}
	for _, n := range list {
		for _, u := range n.upstreams {
			if byID[u] != nil {
				indeg[n.id]++
			}
		}
	}
	var out, queue []*node
	for _, n := range list {
		if indeg[n.id] == 0 {
			queue = append(queue, n)
		}
	}
	seen := map[string]bool{}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		seen[n.id] = true
		out = append(out, n)
		for _, d := range n.downstreams {
			if indeg[d]--; indeg[d] == 0 {
				queue = append(queue, byID[d])
			}
		}
	}
	for _, n := range list { // cycles: append the rest in name order
		if !seen[n.id] {
			out = append(out, n)
		}
	}
	return out
}

func (e *evalCtx) seconds() float64 { return math.Max(1, float64(e.to-e.from)) }

func evalGraph(e *evalCtx) (*Result, error) {
	nodes, byID := e.nodes()
	res := &Result{Type: "graph"}
	for _, n := range nodes {
		out := NodeOut{
			ID: n.id, Pipeline: n.sr.Pipeline, Stage: n.sr.Stage,
			In: n.c.In(), Originated: n.c.Originated, Emitted: n.c.Emitted, Passed: n.c.Passed, Done: n.c.Done,
			Dropped: sum(n.c.Dropped), Vanished: n.c.Vanished, Errored: n.c.Errored, Retried: n.c.Retried,
			ContextErrors: n.c.ContextErrors,
			InFlight:      n.inFlightEnd, Unaccounted: n.unaccounted(), Unreceived: n.unreceived,
			RatePerSec: n.c.In() / e.seconds(),
			AgeP50:     num(n.c.AgeAtExit.Quantile(0.5)), AgeP99: num(n.c.AgeAtExit.Quantile(0.99)),
			InStageAvg: num(n.c.TimeInStage.Mean()),
		}
		oldestAge := math.NaN()
		if n.oldest != nil {
			oldestAge = float64(e.to) - *n.oldest
			out.OldestAge = num(oldestAge)
		}
		switch {
		case out.Vanished > 0 || out.Errored > 0 || math.Abs(out.Unaccounted) > math.Max(1, 0.01*out.In):
			out.Status = "bad"
		case oldestAge > 60 || out.Retried > 0 || out.ContextErrors > 0:
			out.Status = "warn"
		default:
			out.Status = "ok"
		}
		res.Nodes = append(res.Nodes, out)

		for _, up := range n.upstreamNames() {
			upID := n.sr.Pipeline + "/" + up
			count := n.c.Received[up]
			transit := math.NaN()
			if u := byID[upID]; u != nil {
				transit = n.c.AgeAtEntry.Mean() - u.c.AgeAtExit.Mean()
			}
			edge := EdgeOut{
				From: upID, To: n.id, Status: n.edgeStatus(up, e.to),
				Count: count, RatePerSec: count / e.seconds(), TransitAvg: num(transit),
				OldestWaitingAge: num(n.oldestWaitingAge(up, e.to)),
			}
			if wt, ok := n.waitingEnd[up]; ok && wt.Counted {
				edge.Waiting = wt.Items
			}
			res.Edges = append(res.Edges, edge)
		}
	}
	sort.Slice(res.Edges, func(i, j int) bool { return res.Edges[i].From+res.Edges[i].To < res.Edges[j].From+res.Edges[j].To })
	return res, nil
}

func evalLeak(e *evalCtx) (*Result, error) {
	nodes, _ := e.nodes()
	res := &Result{Type: "table", Columns: []string{
		"stage", "in", "passed", "done", "dropped", "vanished", "errored", "in_flight_delta", "unaccounted", "lost_pct", "queue_growth",
	}}
	for _, n := range nodes {
		in := n.c.In()
		lost := n.c.Vanished + n.c.Errored + math.Max(0, n.unaccounted())
		res.Rows = append(res.Rows, []any{
			e.name(n), in, n.c.Passed, n.c.Done, sum(n.c.Dropped), n.c.Vanished, n.c.Errored,
			n.inFlightEnd - n.inFlightStart, round(n.unaccounted()), num(100 * lost / in), n.unreceived,
		})
	}
	return res, nil
}

func evalDrops(e *evalCtx) (*Result, error) {
	nodes, _ := e.nodes()
	byStage := len(e.q.By) == 0
	for _, b := range e.q.By {
		if b == "stage" {
			byStage = true
		}
	}
	type key struct{ stage, reason string }
	counts := map[key]float64{}
	ins := map[string]float64{}
	for _, n := range nodes {
		stage := ""
		if byStage {
			stage = e.name(n)
		}
		ins[stage] += n.c.In()
		for reason, c := range n.c.Dropped {
			counts[key{stage, reason}] += c
		}
		counts[key{stage, "(vanished: no output, no outcome)"}] += n.c.Vanished
		counts[key{stage, "(errored)"}] += n.c.Errored
	}
	res := &Result{Type: "table", Columns: []string{"reason", "count", "pct_of_in"}}
	if byStage {
		res.Columns = append([]string{"stage"}, res.Columns...)
	}
	keys := make([]key, 0, len(counts))
	for k, c := range counts {
		if c > 0 {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return counts[keys[i]] > counts[keys[j]] })
	for _, k := range keys {
		row := []any{k.reason, counts[k], num(100 * counts[k] / ins[k.stage])}
		if byStage {
			row = append([]any{k.stage}, row...)
		}
		res.Rows = append(res.Rows, row)
	}
	return res, nil
}

func evalDecompose(e *evalCtx) (*Result, error) {
	nodes, byID := e.nodes()
	res := &Result{Type: "table", Columns: []string{"stage", "wait_before_s", "in_stage_s", "age_at_exit_s", "share_of_e2e_pct"}}
	e2e := math.NaN()
	for _, n := range nodes {
		if len(n.downstreams) == 0 {
			if m := n.c.AgeAtExit.Mean(); !math.IsNaN(m) && (math.IsNaN(e2e) || m > e2e) {
				e2e = m
			}
		}
	}
	for _, n := range nodes {
		// Wait before = mean age on entry minus the received-weighted mean age when upstream let go.
		upAge, weight := 0.0, 0.0
		for up, count := range n.c.Received {
			if u := byID[n.sr.Pipeline+"/"+up]; u != nil {
				if m := u.c.AgeAtExit.Mean(); !math.IsNaN(m) {
					upAge += m * count
					weight += count
				}
			}
		}
		wait := n.c.AgeAtEntry.Mean()
		if weight > 0 {
			wait -= upAge / weight
		}
		inStage := n.c.TimeInStage.Mean()
		res.Rows = append(res.Rows, []any{e.name(n), num(wait), num(inStage), num(n.c.AgeAtExit.Mean()), num(100 * (wait + inStage) / e2e)})
	}
	res.Rows = append(res.Rows, []any{"end-to-end", nil, nil, num(e2e), num(100)})
	return res, nil
}

// evalTransit: per edge, unaccounted = sent - received - growth in waiting.
func evalTransit(e *evalCtx) (*Result, error) {
	nodes, byID := e.nodes()
	res := &Result{Type: "table", Columns: []string{
		"edge", "sent", "received", "waiting_start", "waiting_end", "unaccounted", "oldest_waiting_s", "avg_wait_s", "status",
	}}
	for _, n := range nodes {
		for _, up := range n.upstreamNames() {
			received := n.c.Received[up]
			sent, unaccounted, avgWait := math.NaN(), math.NaN(), math.NaN()
			if u := byID[n.sr.Pipeline+"/"+up]; u != nil {
				avgWait = n.c.AgeAtEntry.Mean() - u.c.AgeAtExit.Mean()
				if len(u.downstreams) == 1 { // emitted is not split by edge, so only exact with one downstream
					sent = u.c.Emitted
				}
			}
			var waitingStart, waitingEnd any
			start, okStart := n.waitingStart[up]
			end, okEnd := n.waitingEnd[up]
			okStart, okEnd = okStart && start.Counted, okEnd && end.Counted
			if okStart {
				waitingStart = start.Items
			}
			if okEnd {
				waitingEnd = end.Items
			}
			if okStart && okEnd { // only with both counts: an edge scraped without counting has none
				unaccounted = sent - received - (end.Items - start.Items)
			}
			res.Rows = append(res.Rows, []any{
				up + " → " + e.name(n), num(sent), received, waitingStart, waitingEnd, num(unaccounted),
				num(n.oldestWaitingAge(up, e.to)), num(avgWait), n.edgeStatus(up, e.to),
			})
		}
	}
	return res, nil
}

// edgeSeriesFunc evaluates value per window for every edge a scraper watches.
func edgeSeriesFunc(unit string, value func(wt Wait, end int64) float64) func(*evalCtx) (*Result, error) {
	return func(e *evalCtx) (*Result, error) {
		nodes, _ := e.nodes()
		res := &Result{Type: "series", Unit: unit}
		for _, n := range nodes {
			upstreams := map[string]bool{}
			for start, w := range n.sr.Windows {
				if start >= windowStart(float64(e.from)) && start < e.to {
					for _, byUpstream := range w.Waiting {
						for up := range byUpstream {
							upstreams[up] = true
						}
					}
				}
			}
			names := make([]string, 0, len(upstreams))
			for up := range upstreams {
				names = append(names, up)
			}
			sort.Strings(names)
			for _, up := range names {
				s := SeriesOut{Name: up + " → " + e.name(n)}
				for t := windowStart(float64(e.from)); t < e.to; t += windowSeconds {
					var v any
					if w := n.sr.Windows[t]; w != nil && len(w.Waiting) > 0 {
						if wt, ok := w.sumWaiting()[up]; ok {
							v = num(value(wt, t+windowSeconds))
						}
					}
					s.Points = append(s.Points, []any{t * 1000, v})
				}
				res.Series = append(res.Series, s)
			}
		}
		return res, nil
	}
}

func waitingValue(wt Wait, _ int64) float64 {
	if !wt.Counted {
		return math.NaN()
	}
	return wt.Items
}

func oldestWaitingValue(wt Wait, end int64) float64 {
	if wt.Oldest != nil {
		return float64(end) - *wt.Oldest
	}
	if wt.Counted && wt.Items == 0 {
		return 0 // nothing waits
	}
	return math.NaN()
}

// seriesFunc evaluates value per window for every selected stage.
func seriesFunc(unit string, value func(e *evalCtx, w *Window, end int64) float64) func(*evalCtx) (*Result, error) {
	return func(e *evalCtx) (*Result, error) {
		nodes, _ := e.nodes()
		res := &Result{Type: "series", Unit: unit}
		for _, n := range nodes {
			s := SeriesOut{Name: e.name(n)}
			for t := windowStart(float64(e.from)); t < e.to; t += windowSeconds {
				var v any
				if w := n.sr.Windows[t]; w != nil {
					v = num(value(e, w, t+windowSeconds))
				}
				s.Points = append(s.Points, []any{t * 1000, v})
			}
			res.Series = append(res.Series, s)
		}
		return res, nil
	}
}

func ageValue(e *evalCtx, w *Window, _ int64) float64 {
	q := 0.99
	if e.q.Param != nil {
		q = *e.q.Param
	}
	return w.Counts.AgeAtExit.Quantile(q)
}

func oldestValue(_ *evalCtx, w *Window, end int64) float64 {
	_, oldest, ok := w.sumGauges()
	if !ok {
		return math.NaN()
	}
	if oldest == nil {
		return 0
	}
	return float64(end) - *oldest
}

func inFlightValue(_ *evalCtx, w *Window, _ int64) float64 {
	v, _, ok := w.sumGauges()
	if !ok {
		return math.NaN()
	}
	return v
}

func throughputValue(_ *evalCtx, w *Window, _ int64) float64 {
	return w.Counts.In() / windowSeconds
}

func lostValue(_ *evalCtx, w *Window, _ int64) float64 {
	return (w.Counts.Vanished + w.Counts.Errored) / windowSeconds
}

// name is the display name of a stage: qualified only when several pipelines are selected.
func (e *evalCtx) name(n *node) string {
	for _, m := range e.q.Matchers {
		if m.Label == "pipeline" && m.Op == "=" {
			return n.sr.Stage
		}
	}
	return n.id
}

// num makes a float JSON-safe: NaN and Inf become null.
func num(v float64) any {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return round(v)
}

func round(v float64) float64 { return math.Round(v*1000) / 1000 }
