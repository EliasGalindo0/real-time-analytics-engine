package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"real-time-analytics-engine/internal/aggregate"
	"real-time-analytics-engine/internal/hub"
)

type Config struct {
	IngestQ        chan aggregate.Event
	IngestMaxBytes int64

	Store *aggregate.Store
	Hub   *hub.Hub

	Logger    *slog.Logger
	StartTime time.Time
}

type API struct {
	cfg Config

	upgrader websocket.Upgrader
}

func New(cfg Config) *API {
	if cfg.IngestMaxBytes <= 0 {
		cfg.IngestMaxBytes = 1 << 20
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
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
	mux.HandleFunc("GET /stats", a.stats)
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
	if ct := r.Header.Get("content-type"); ct != "" && !strings.HasPrefix(strings.ToLower(ct), "application/json") {
		writeErr(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "content-type must be application/json")
		return
	}
	if r.Body == nil {
		writeErr(w, http.StatusBadRequest, "missing_body", "missing body")
		return
	}
	defer r.Body.Close()

	body, err := io.ReadAll(io.LimitReader(r.Body, a.cfg.IngestMaxBytes))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read_error", "read error")
		return
	}
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		writeErr(w, http.StatusBadRequest, "empty_body", "empty body")
		return
	}

	// Accept either a single event or a batch (array) to reduce per-request overhead.
	if body[0] == '[' {
		var batch []ingestReq
		if err := decodeStrict(body, &batch); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
			return
		}
		if len(batch) == 0 {
			writeErr(w, http.StatusBadRequest, "empty_batch", "batch cannot be empty")
			return
		}
		if len(batch) > 10_000 {
			writeErr(w, http.StatusRequestEntityTooLarge, "batch_too_large", "batch max is 10000 events")
			return
		}
		accepted, rejected, overloaded := a.enqueueBatch(batch)
		status := http.StatusAccepted
		if overloaded {
			status = http.StatusTooManyRequests
		}
		writeJSON(w, status, map[string]any{
			"accepted":   accepted,
			"rejected":   rejected,
			"overloaded": overloaded,
		})
		return
	}

	var req ingestReq
	if err := decodeStrict(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	e, err := toEvent(req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_event", err.Error())
		return
	}
	if ok := tryEnqueue(a.cfg.IngestQ, e); !ok {
		writeErr(w, http.StatusTooManyRequests, "ingest_overloaded", "ingest queue is full")
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("accepted"))
}

func (a *API) metrics(w http.ResponseWriter, r *http.Request) {
	snap := a.cfg.Store.Snapshot(10_000)
	writeJSON(w, http.StatusOK, map[string]any{
		"series": snap,
	})
}

func (a *API) stats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"uptime_sec": int64(time.Since(a.cfg.StartTime).Seconds()),
		"ingest_q": map[string]any{
			"len": len(a.cfg.IngestQ),
			"cap": cap(a.cfg.IngestQ),
		},
		"store": map[string]any{
			"series": a.cfg.Store.Len(),
		},
		"hub": a.cfg.Hub.Stats(),
	})
}

func (a *API) ws(w http.ResponseWriter, r *http.Request) {
	conn, err := a.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	c := hub.NewClient(conn, a.cfg.Hub.ClientQueueDepth())
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
		go c.ReadPump(60*time.Second, 64<<10)
		c.WritePump(20*time.Second, 5*time.Second)
	}()

	// The read pump owns read deadlines; this handler just returns.
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code string, msg string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": msg,
		},
	})
}

func decodeStrict(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("trailing json")
	}
	return nil
}

func toEvent(req ingestReq) (aggregate.Event, error) {
	if strings.TrimSpace(req.Name) == "" {
		return aggregate.Event{}, errors.New("name is required")
	}
	name := strings.TrimSpace(req.Name)
	if len(name) > 128 {
		return aggregate.Event{}, errors.New("name too long (max 128)")
	}

	v := 1.0
	if req.Value != nil {
		v = *req.Value
	}

	ts := time.Now().UTC()
	if req.TS != nil && !req.TS.IsZero() {
		ts = req.TS.UTC()
	}

	tags, err := normalizeTags(req.Tags)
	if err != nil {
		return aggregate.Event{}, err
	}

	return aggregate.Event{
		Name:  name,
		Value: v,
		TS:    ts,
		Tags:  tags,
	}, nil
}

func normalizeTags(tags map[string]string) (map[string]string, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	if len(tags) > 20 {
		return nil, errors.New("too many tags (max 20)")
	}
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		kk := strings.TrimSpace(k)
		vv := strings.TrimSpace(v)
		if kk == "" {
			return nil, errors.New("tag key cannot be empty")
		}
		if len(kk) > 64 || len(vv) > 128 {
			return nil, errors.New("tag key/value too long")
		}
		out[kk] = vv
	}
	return out, nil
}

func tryEnqueue(q chan<- aggregate.Event, e aggregate.Event) bool {
	select {
	case q <- e:
		return true
	default:
		return false
	}
}

func (a *API) enqueueBatch(batch []ingestReq) (accepted int, rejected int, overloaded bool) {
	for _, req := range batch {
		e, err := toEvent(req)
		if err != nil {
			rejected++
			continue
		}
		if ok := tryEnqueue(a.cfg.IngestQ, e); !ok {
			overloaded = true
			break
		}
		accepted++
	}
	return accepted, rejected, overloaded
}

