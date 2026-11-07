package main

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

// observe adds seconds the way the SDK does: bucket ceil(log2(v) * 2^scale) - 1.
func observe(h *Hist, v float64) {
	h.Sum += v
	h.Count++
	if v <= 1e-6 {
		h.ZeroCount++
		return
	}
	h.addBucket(int(math.Ceil(math.Log2(v)*math.Exp2(float64(h.Scale))))-1, 1)
}

func TestQuantilesAreWithinAFewPercent(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	h := Hist{Scale: 3}
	var values []float64
	for i := 0; i < 100000; i++ {
		v := math.Exp(r.Float64()*12 - 4) // 0.02 s to 3 h, log-uniform
		values = append(values, v)
		observe(&h, v)
	}
	sort.Float64s(values)
	for _, q := range []float64{0.5, 0.9, 0.99} {
		want := values[int(q*float64(len(values)))]
		if got := h.Quantile(q); math.Abs(got-want)/want > 0.05 {
			t.Errorf("p%v = %v, want %v within 5%%", q*100, got, want)
		}
	}
}

func TestMergeAcrossOffsetsAndScales(t *testing.T) {
	a, b := Hist{Scale: 3}, Hist{Scale: 4}
	for _, v := range []float64{0.01, 0.02} {
		observe(&a, v)
	}
	for _, v := range []float64{100, 200, 0} {
		observe(&b, v)
	}
	var m Hist
	m.Merge(a)
	m.Merge(b) // finer scale: downscaled to 3
	if m.Scale != 3 || m.Count != 5 || m.ZeroCount != 1 {
		t.Fatalf("merged %+v", m)
	}
	if got := m.Quantile(1); got < 190 || got > 220 {
		t.Fatalf("max %v, want about 200", got)
	}
}

// Windows without observations hold zero-value histograms (scale 0). Merging one must not
// coarsen the result: it once turned a true p99 of 34 s into 57 s.
func TestMergingAnEmptyHistogramKeepsTheScale(t *testing.T) {
	var h Hist
	h.Merge(Hist{}) // a window with no observations first
	full := Hist{Scale: 3}
	observe(&full, 35)
	h.Merge(full)
	h.Merge(Hist{})
	if h.Scale != 3 {
		t.Fatalf("scale %d, want 3", h.Scale)
	}
	if got := h.Quantile(0.99); got < 34.9 || got > 38.1 { // 35 s lies in the bucket (34.9, 38.05]
		t.Fatalf("p99 %v, want within the 35 s bucket", got)
	}
}

func TestSubGivesTheNewObservationsOnly(t *testing.T) {
	prev := Hist{Scale: 3}
	observe(&prev, 1)
	now := prev
	now.Counts = append([]float64(nil), prev.Counts...)
	observe(&now, 1)
	observe(&now, 0.001) // a new, lower bucket: the offset moves
	d := now.Sub(prev)
	if d.Count != 2 || d.Quantile(0.25) > 0.002 || d.Quantile(1) < 0.9 {
		t.Fatalf("delta %+v", d)
	}
}
