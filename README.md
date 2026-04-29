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

### Option A2: just recipes (recommended)

Prereq: install `just` from `https://just.systems/`.

```bash
just up-d
just health
just ingest
just logs
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

## Ingest a batch (array)

```bash
curl -sS -X POST http://localhost:8080/ingest \
  -H 'content-type: application/json' \
  -d '[{"name":"page_view","value":1,"tags":{"route":"/"}},{"name":"signup","value":1}]'
```

## Watch metrics via WebSocket

Use any WS client against `ws://localhost:8080/ws`.

## Get a snapshot

```bash
curl -sS http://localhost:8080/metrics
```

## Stats (queue depth, clients, drops)

```bash
curl -sS http://localhost:8080/stats
```

# real-time-analytics-engine
