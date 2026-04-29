package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"
)

type ctxKey string

const requestIDKey ctxKey = "request_id"

func WrapMiddleware(next http.Handler, logger *slog.Logger) http.Handler {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
	})
	h = withRequestID(h)
	h = withRecovery(h, logger)
	h = withAccessLog(h, logger)
	return h
}

func withRequestID(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("x-request-id")
		if id == "" {
			id = newRequestID()
		}
		w.Header().Set("x-request-id", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	}
}

func withRecovery(next http.Handler, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				logger.Error("panic", "err", rec, "stack", string(debug.Stack()))
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	}
}

func withAccessLog(next http.Handler, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := &wrapWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(ww, r)
		logger.Info("http",
			"method", r.Method,
			"path", r.URL.Path,
			"status", ww.status,
			"bytes", ww.bytes,
			"dur_ms", time.Since(start).Milliseconds(),
		)
	}
}

type wrapWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *wrapWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *wrapWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

func newRequestID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

