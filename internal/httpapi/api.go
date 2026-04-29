package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"real-time-analytics-engine/internal/aggregate"
	"real-time-analytics-engine/internal/hub"
)

type Config struct {
	IngestQ        chan<- aggregate.Event
	IngestMaxBytes int64

	Store *aggregate.Store
	Hub   *hub.Hub
}

type API struct {
	cfg Config

	upgrader websocket.Upgrader
}

func New(cfg Config) *API {
	if cfg.IngestMaxBytes <= 0 {
		cfg.IngestMaxBytes = 1 << 20
	}
	return &API{
		cfg: cfg,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4 << 10,
			WriteBufferSize: 4 << 10,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}
}

func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("POST /ingest", a.ingest)
	mux.HandleFunc("GET /metrics", a.metrics)
	mux.HandleFunc("GET /ws", a.ws)
}

func (a *API) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

type ingestReq struct {
	Name  string            `json:"name"`
	Value *float64          `json:"value,omitempty"`
	TS    *time.Time        `json:"ts,omitempty"`
	Tags  map[string]string `json:"tags,omitempty"`
}

func (a *API) ingest(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		http.Error(w, "missing body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	body, err := io.ReadAll(io.LimitReader(r.Body, a.cfg.IngestMaxBytes))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}

	var req ingestReq
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	v := 1.0
	if req.Value != nil {
		v = *req.Value
	}
	ts := time.Now().UTC()
	if req.TS != nil && !req.TS.IsZero() {
		ts = req.TS.UTC()
	}

	e := aggregate.Event{
		Name:  req.Name,
		Value: v,
		TS:    ts,
		Tags:  req.Tags,
	}

	select {
	case a.cfg.IngestQ <- e:
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("accepted"))
	default:
		http.Error(w, "ingest overloaded", http.StatusTooManyRequests)
	}
}

func (a *API) metrics(w http.ResponseWriter, r *http.Request) {
	snap := a.cfg.Store.Snapshot(10_000)
	writeJSON(w, http.StatusOK, map[string]any{
		"series": snap,
	})
}

func (a *API) ws(w http.ResponseWriter, r *http.Request) {
	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	c := hub.NewClient(conn, 256)
	if ok := a.cfg.Hub.Add(c); !ok {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "server overloaded"), time.Now().Add(1*time.Second))
		_ = conn.Close()
		return
	}

	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_ = conn.WriteJSON(map[string]any{
		"type":   "snapshot",
		"series": a.cfg.Store.Snapshot(2_000),
	})

	go func() {
		defer a.cfg.Hub.Remove(c)
		c.WritePump(20*time.Second, 5*time.Second)
	}()

	conn.SetReadLimit(64 << 10)
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			if !errors.Is(err, websocket.ErrCloseSent) {
				// ignore
			}
			c.Close()
			return
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

