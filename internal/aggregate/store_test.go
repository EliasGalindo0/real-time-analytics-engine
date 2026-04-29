package aggregate

import (
	"testing"
	"time"
)

func TestStoreAddAndSnapshot(t *testing.T) {
	s := New(Config{MaxSeries: 10})
	now := time.Unix(1_700_000_000, 0).UTC()

	a1, ok := s.Add(Event{Name: "x", Value: 2, TS: now, Tags: map[string]string{"a": "b"}})
	if !ok {
		t.Fatalf("expected ok")
	}
	if a1.Count != 1 || a1.Sum != 2 || a1.Min != 2 || a1.Max != 2 || a1.Last != 2 {
		t.Fatalf("unexpected agg: %+v", a1)
	}

	a2, ok := s.Add(Event{Name: "x", Value: 3, TS: now.Add(1 * time.Second), Tags: map[string]string{"a": "b"}})
	if !ok {
		t.Fatalf("expected ok")
	}
	if a2.Count != 2 || a2.Sum != 5 || a2.Min != 2 || a2.Max != 3 || a2.Last != 3 {
		t.Fatalf("unexpected agg: %+v", a2)
	}

	snap := s.Snapshot(100)
	if len(snap) != 1 {
		t.Fatalf("expected 1 series, got %d", len(snap))
	}
	if s.Len() != 1 {
		t.Fatalf("expected len=1, got %d", s.Len())
	}
}

func TestStoreMaxSeriesCap(t *testing.T) {
	s := New(Config{MaxSeries: 1})
	now := time.Now().UTC()

	if _, ok := s.Add(Event{Name: "a", Value: 1, TS: now}); !ok {
		t.Fatalf("expected first series ok")
	}
	if _, ok := s.Add(Event{Name: "b", Value: 1, TS: now}); ok {
		t.Fatalf("expected second series rejected due to cap")
	}
}

