package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"real-time-analytics-engine/internal/aggregate"
	"real-time-analytics-engine/internal/httpapi"
	"real-time-analytics-engine/internal/hub"
)

func main() {
	addr := env("ENGINE_ADDR", ":8080")

	agg := aggregate.New(aggregate.Config{
		MaxSeries: 50_000,
	})

	h := hub.New(hub.Config{
		MaxClients:          10_000,
		PerClientQueueDepth: 256,
		BroadcastCoalesce:   100 * time.Millisecond,
	})
	defer h.Close()

	ingestQ := make(chan aggregate.Event, 100_000)
	processor := aggregate.NewProcessor(aggregate.ProcessorConfig{
		Workers:  4,
		IngestQ:  ingestQ,
		Store:    agg,
		Outbound: h,
	})
	processor.Start()
	defer processor.Stop()

	mux := http.NewServeMux()
	api := httpapi.New(httpapi.Config{
		IngestQ:        ingestQ,
		IngestMaxBytes: 1 << 20, // 1MB
		Store:          agg,
		Hub:            h,
	})
	api.Register(mux)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("engine listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
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

func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}

