package main

import (
	"sort"
	"strings"
)

// Cumulative reports carry running totals; subtracting each sender's previous report gives
// per-window counts. A lost report loses nothing, a repeated or late one is ignored, and a new
// start time or a falling total means the sender restarted from 0.

// sender is one reporting process (service.instance.id).
type sender struct {
	startTime int64
	lastTime  int64
	last      map[string]Point // by pointKey
	// First report of a sender older than this server: its totals predate us, so only remember them.
	baseline bool
}

// cumulativeSender returns nil for a report that repeats or predates one already counted.
func (s *Store) cumulativeSender(instance string, p IngestPayload) *sender {
	snd := s.senders[instance]
	if snd != nil && snd.startTime == p.StartTimeUnixNano {
		if p.TimeUnixNano <= snd.lastTime {
			return nil // delivered twice, or arrived after a newer report: already counted
		}
		snd.lastTime = p.TimeUnixNano
		snd.baseline = false
		return snd
	}
	// New or restarted sender.
	firstSighting := snd == nil
	snd = &sender{
		startTime: p.StartTimeUnixNano,
		lastTime:  p.TimeUnixNano,
		last:      map[string]Point{},
		baseline:  firstSighting && p.StartTimeUnixNano < s.startedAt.UnixNano(),
	}
	s.senders[instance] = snd
	return snd
}

// delta returns the counts since the sender's previous report, or nil if there are none.
func (snd *sender) delta(pt *Point) *Point {
	key := pointKey(pt)
	prev, seen := snd.last[key]
	snd.last[key] = *pt
	if snd.baseline {
		return nil
	}
	if !seen {
		// SDKs leave zeros out, so a new series was 0 until now.
		return pt
	}
	switch {
	case pt.Value != nil && prev.Value != nil:
		if *pt.Value < *prev.Value {
			return pt // reset
		}
		d := *pt.Value - *prev.Value
		if d == 0 {
			return nil
		}
		return &Point{Name: pt.Name, Attributes: pt.Attributes, Value: &d}
	case pt.Histogram != nil && prev.Histogram != nil:
		if pt.Histogram.Count < prev.Histogram.Count {
			return pt
		}
		if pt.Histogram.Count == prev.Histogram.Count {
			return nil
		}
		return &Point{Name: pt.Name, Attributes: pt.Attributes, Histogram: pt.Histogram.Sub(*prev.Histogram)}
	}
	return nil
}

// pointKey is the metric name plus all attributes.
func pointKey(pt *Point) string {
	keys := make([]string, 0, len(pt.Attributes))
	for k := range pt.Attributes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(pt.Name)
	for _, k := range keys {
		b.WriteString("\x00" + k + "=" + pt.Attributes[k])
	}
	return b.String()
}
