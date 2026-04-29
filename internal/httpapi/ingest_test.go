package httpapi

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"real-time-analytics-engine/internal/aggregate"
	"real-time-analytics-engine/internal/hub"
)

func TestIngestRejectsUnknownFields(t *testing.T) {
	q := make(chan aggregate.Event, 10)
	api := New(Config{
		IngestQ:        q,
		IngestMaxBytes: 1 << 20,
		Store:          aggregate.New(aggregate.Config{MaxSeries: 10}),
		Hub:            hub.New(hub.Config{}),
		StartTime:      time.Now(),
	})

	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewBufferString(`{"name":"x","wat":1}`))
	req.Header.Set("content-type", "application/json")
	rr := httptest.NewRecorder()
	api.ingest(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestIngestOverloadReturns429(t *testing.T) {
	q := make(chan aggregate.Event, 0) // always full
	api := New(Config{
		IngestQ:        q,
		IngestMaxBytes: 1 << 20,
		Store:          aggregate.New(aggregate.Config{MaxSeries: 10}),
		Hub:            hub.New(hub.Config{}),
		StartTime:      time.Now(),
	})

	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewBufferString(`{"name":"x","value":1}`))
	req.Header.Set("content-type", "application/json")
	rr := httptest.NewRecorder()
	api.ingest(rr, req)

	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d: %s", rr.Code, rr.Body.String())
	}
}

