package aggregate

import (
	"sync"
	"time"
)

type Config struct {
	MaxSeries int
}

type Store struct {
	cfg Config

	mu     sync.RWMutex
	series map[SeriesKey]*SeriesAgg
}

type SeriesAgg struct {
	Key  SeriesKey         `json:"key"`
	Name string            `json:"name"`
	Tags map[string]string `json:"tags,omitempty"`

	Count uint64    `json:"count"`
	Sum   float64   `json:"sum"`
	Min   float64   `json:"min"`
	Max   float64   `json:"max"`
	Last  float64   `json:"last"`
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

func New(cfg Config) *Store {
	if cfg.MaxSeries <= 0 {
		cfg.MaxSeries = 50_000
	}
	return &Store{
		cfg:    cfg,
		series: make(map[SeriesKey]*SeriesAgg, 1024),
	}
}

// Add updates the aggregation for the event's series.
// Returns (updatedAgg, ok). ok=false when series cap is exceeded.
func (s *Store) Add(e Event) (SeriesAgg, bool) {
	key := MakeSeriesKey(e.Name, e.Tags)

	s.mu.Lock()
	defer s.mu.Unlock()

	a := s.series[key]
	if a == nil {
		if len(s.series) >= s.cfg.MaxSeries {
			return SeriesAgg{}, false
		}
		a = &SeriesAgg{
			Key:   key,
			Name:  e.Name,
			Tags:  cloneTags(e.Tags),
			Min:   e.Value,
			Max:   e.Value,
			Last:  e.Value,
			Sum:   e.Value,
			Count: 1,
			Start: e.TS,
			End:   e.TS,
		}
		s.series[key] = a
		return *a, true
	}

	a.Count++
	a.Sum += e.Value
	a.Last = e.Value
	if e.Value < a.Min {
		a.Min = e.Value
	}
	if e.Value > a.Max {
		a.Max = e.Value
	}
	if e.TS.Before(a.Start) {
		a.Start = e.TS
	}
	if e.TS.After(a.End) {
		a.End = e.TS
	}
	return *a, true
}

func (s *Store) Snapshot(limit int) []SeriesAgg {
	if limit <= 0 {
		limit = 10_000
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]SeriesAgg, 0, min(limit, len(s.series)))
	for _, a := range s.series {
		out = append(out, *a)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.series)
}

func (s *Store) AtCapacity() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.series) >= s.cfg.MaxSeries
}

func (s *Store) HasSeries(key SeriesKey) bool {
	s.mu.RLock()
	_, ok := s.series[key]
	s.mu.RUnlock()
	return ok
}

func cloneTags(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

