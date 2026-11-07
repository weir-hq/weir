package main

import (
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	q, err := Parse(`drops({pipeline="orders", stage=~"enrich|sink"}[15m], 0.5) by (stage, reason)`)
	if err != nil {
		t.Fatal(err)
	}
	if q.Func != "drops" || len(q.Matchers) != 2 || q.Range != 15*time.Minute || *q.Param != 0.5 || len(q.By) != 2 {
		t.Fatalf("unexpected query: %+v", q)
	}
	if !q.Matchers[1].Matches("sink") || q.Matchers[1].Matches("sinker") {
		t.Fatal("regex matcher must be anchored")
	}
}

func TestParseErrors(t *testing.T) {
	for _, src := range []string{``, `graph`, `graph({pipeline="x"`, `graph({pipeline=x})`, `graph({}[5x])`, `graph({}) extra`} {
		if _, err := Parse(src); err == nil {
			t.Errorf("expected error for %q", src)
		}
	}
}
