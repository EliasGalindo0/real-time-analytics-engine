package hub

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	MaxClients          int
	PerClientQueueDepth int
	BroadcastCoalesce   time.Duration
}

type Hub struct {
	cfg Config

	mu      sync.RWMutex
	clients map[*Client]struct{}

	coalesceMu sync.Mutex
	pending    map[string]json.RawMessage
	timer      *time.Timer

	closed atomic.Bool
}

func New(cfg Config) *Hub {
	if cfg.MaxClients <= 0 {
		cfg.MaxClients = 10_000
	}
	if cfg.PerClientQueueDepth <= 0 {
		cfg.PerClientQueueDepth = 256
	}
	if cfg.BroadcastCoalesce <= 0 {
		cfg.BroadcastCoalesce = 100 * time.Millisecond
	}
	return &Hub{
		cfg:     cfg,
		clients: make(map[*Client]struct{}),
		pending: make(map[string]json.RawMessage, 1024),
	}
}

func (h *Hub) Close() {
	if h.closed.Swap(true) {
		return
	}
	h.mu.Lock()
	for c := range h.clients {
		c.Close()
	}
	h.clients = nil
	h.mu.Unlock()

	h.coalesceMu.Lock()
	if h.timer != nil {
		h.timer.Stop()
	}
	h.pending = nil
	h.coalesceMu.Unlock()
}

func (h *Hub) Add(c *Client) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed.Load() {
		return false
	}
	if len(h.clients) >= h.cfg.MaxClients {
		return false
	}
	h.clients[c] = struct{}{}
	return true
}

func (h *Hub) Remove(c *Client) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

func (h *Hub) Publish(v any) {
	if h.closed.Load() {
		return
	}
	b, err := json.Marshal(v)
	if err != nil {
		return
	}

	key := extractKey(b)

	h.coalesceMu.Lock()
	if h.pending != nil {
		h.pending[key] = b
	}
	if h.timer == nil {
		h.timer = time.AfterFunc(h.cfg.BroadcastCoalesce, h.flush)
	}
	h.coalesceMu.Unlock()
}

func (h *Hub) flush() {
	h.coalesceMu.Lock()
	pending := h.pending
	h.pending = make(map[string]json.RawMessage, 1024)
	h.timer = nil
	h.coalesceMu.Unlock()

	if len(pending) == 0 {
		return
	}

	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()

	for _, msg := range pending {
		for _, c := range clients {
			c.TrySend(msg)
		}
	}
}

func extractKey(b []byte) string {
	needle := []byte(`"key":"`)
	for i := 0; i+len(needle) < len(b); i++ {
		match := true
		for j := 0; j < len(needle); j++ {
			if b[i+j] != needle[j] {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		start := i + len(needle)
		for end := start; end < len(b); end++ {
			if b[end] == '"' {
				return string(b[start:end])
			}
		}
	}
	return string(time.Now().UnixNano())
}

