package main

import "math"

// Hist is an OpenTelemetry base-2 exponential histogram of seconds: Counts[i] holds values in
// (base^(Offset+i), base^(Offset+i+1)], base = 2^(2^-Scale). ZeroCount holds values near 0.
type Hist struct {
	Scale     int       `json:"scale"`
	ZeroCount float64   `json:"zero_count"`
	Offset    int       `json:"offset"`
	Counts    []float64 `json:"bucket_counts"`
	Sum       float64   `json:"sum"`
	Count     float64   `json:"count"`
}

// Merge adds o into h. Histograms at different scales meet at the coarser one.
func (h *Hist) Merge(o Hist) {
	if o.Count == 0 {
		return // an empty histogram (e.g. a window with no observations) has no scale to respect
	}
	if h.Count == 0 {
		h.Scale = o.Scale
	}
	switch {
	case o.Scale < h.Scale:
		h.downscale(h.Scale - o.Scale)
	case o.Scale > h.Scale:
		o.downscale(o.Scale - h.Scale)
	}
	h.ZeroCount += o.ZeroCount
	h.Sum += o.Sum
	h.Count += o.Count
	for i, c := range o.Counts {
		h.addBucket(o.Offset+i, c)
	}
}

// Sub returns h minus an earlier total of the same series.
func (h Hist) Sub(prev Hist) *Hist {
	d := Hist{
		Scale: h.Scale, ZeroCount: h.ZeroCount - prev.ZeroCount, Sum: h.Sum - prev.Sum, Count: h.Count - prev.Count,
		Offset: h.Offset, Counts: append([]float64(nil), h.Counts...),
	}
	if prev.Scale > d.Scale {
		prev.downscale(prev.Scale - d.Scale)
	}
	for i, c := range prev.Counts {
		d.addBucket(prev.Offset+i, -c)
	}
	return &d
}

func (h Hist) Mean() float64 {
	if h.Count == 0 {
		return math.NaN()
	}
	return h.Sum / h.Count
}

// Quantile interpolates linearly inside the bucket that holds the rank.
func (h Hist) Quantile(q float64) float64 {
	if h.Count == 0 {
		return math.NaN()
	}
	rank := q * h.Count
	if rank <= h.ZeroCount {
		return 0
	}
	base := math.Exp2(math.Exp2(-float64(h.Scale)))
	cum := h.ZeroCount
	for i, c := range h.Counts {
		if c > 0 && cum+c >= rank {
			lo := math.Pow(base, float64(h.Offset+i))
			return lo + (lo*base-lo)*(rank-cum)/c
		}
		cum += c
	}
	return math.Pow(base, float64(h.Offset+len(h.Counts)))
}

func (h *Hist) addBucket(index int, c float64) {
	switch {
	case len(h.Counts) == 0:
		h.Offset, h.Counts = index, []float64{0}
	case index < h.Offset:
		h.Counts = append(make([]float64, h.Offset-index), h.Counts...)
		h.Offset = index
	case index >= h.Offset+len(h.Counts):
		h.Counts = append(h.Counts, make([]float64, index-h.Offset-len(h.Counts)+1)...)
	}
	h.Counts[index-h.Offset] += c
}

// downscale halves the resolution `by` times: bucket i becomes i >> by (a floor, for negative
// indices too). It builds a new slice, since Counts may be shared with the caller's copy.
func (h *Hist) downscale(by int) {
	counts, offset := h.Counts, h.Offset
	h.Scale -= by
	h.Counts, h.Offset = nil, 0
	for i, c := range counts {
		h.addBucket((offset+i)>>by, c)
	}
}
