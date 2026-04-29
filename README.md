# Real-Time Analytics Engine (minimal)

This repository contains a minimal, working real-time analytics engine:

- HTTP event ingestion
- In-memory aggregation
- WebSocket metric updates

## Run

### Option A: Docker (fastest)

```bash
docker compose up --build
```

### Option B: Local Go

Prereqs: Go 1.22+

```bash
go run ./cmd/engine
```

Server defaults to `:8080`.

## Ingest an event

```bash
curl -sS -X POST http://localhost:8080/ingest \
  -H 'content-type: application/json' \
  -d '{"name":"page_view","value":1,"ts":"2026-04-29T12:00:00Z","tags":{"route":"/","country":"BR"}}'
```

## Watch metrics via WebSocket

Use any WS client against `ws://localhost:8080/ws`.

## Get a snapshot

```bash
curl -sS http://localhost:8080/metrics
```

# real-time-analytics-engine
