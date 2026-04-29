package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"log/slog"

	"real-time-analytics-engine/internal/aggregate"
	"real-time-analytics-engine/internal/httpapi"
	"real-time-analytics-engine/internal/hub"
)

func main() {
	start := time.Now()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg := loadConfig()

	agg := aggregate.New(aggregate.Config{
		MaxSeries: cfg.MaxSeries,
	})

	h := hub.New(hub.Config{
		MaxClients:          cfg.MaxClients,
		PerClientQueueDepth: cfg.WSClientQueueDepth,
		BroadcastCoalesce:   cfg.BroadcastCoalesce,
	})
	defer h.Close()

	ingestQ := make(chan aggregate.Event, cfg.IngestQueueDepth)
	processor := aggregate.NewProcessor(aggregate.ProcessorConfig{
		Workers:  cfg.ProcessorWorkers,
		IngestQ:  ingestQ,
		Store:    agg,
		Outbound: h,
	})
	processor.Start()
	defer processor.Stop()

	mux := http.NewServeMux()
	api := httpapi.New(httpapi.Config{
		IngestQ:        ingestQ,
		IngestMaxBytes: cfg.IngestMaxBytes,
		Store:          agg,
		Hub:            h,
		Logger:         logger,
		StartTime:      start,
	})
	api.Register(mux)

	handler := httpapi.WrapMiddleware(mux, logger)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Info("engine listening", "addr", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	waitForSignal()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

type config struct {
	Addr              string
	IngestQueueDepth  int
	IngestMaxBytes    int64
	ProcessorWorkers  int
	MaxSeries         int
	MaxClients        int
	WSClientQueueDepth int
	BroadcastCoalesce time.Duration
}

func loadConfig() config {
	return config{
		Addr:               env("ENGINE_ADDR", ":8080"),
		IngestQueueDepth:   envInt("ENGINE_INGEST_QUEUE_DEPTH", 100_000),
		IngestMaxBytes:     envInt64("ENGINE_INGEST_MAX_BYTES", 1<<20),
		ProcessorWorkers:   envInt("ENGINE_PROCESSOR_WORKERS", 4),
		MaxSeries:          envInt("ENGINE_MAX_SERIES", 50_000),
		MaxClients:         envInt("ENGINE_MAX_CLIENTS", 10_000),
		WSClientQueueDepth: envInt("ENGINE_WS_CLIENT_QUEUE_DEPTH", 256),
		BroadcastCoalesce:  envDuration("ENGINE_BROADCAST_COALESCE", 100*time.Millisecond),
	}
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func envInt64(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}

