set dotenv-load := true

# Real-time analytics engine - quickstart recipes
#
# Install just: https://just.systems/man/en/

default:
  @just --list

# --- Run (Docker) ---

up:
  docker compose up --build

up-d:
  docker compose up -d --build

logs:
  docker compose logs -f --tail=200

down:
  docker compose down

restart:
  just down
  just up-d

# --- Smoke tests ---

health:
  curl -sS -i http://localhost:8080/healthz

ready:
  curl -sS -i http://localhost:8080/readyz

ingest name="page_view" value="1" route="/":
  curl -sS -i -X POST http://localhost:8080/ingest \
    -H 'content-type: application/json' \
    -d '{"name":"{{name}}","value":{{value}},"tags":{"route":"{{route}}"}}'

metrics:
  curl -sS http://localhost:8080/metrics

stats:
  curl -sS http://localhost:8080/stats

# --- Chaos / failure simulation (Node 18+; you have Node installed) ---

# Usage examples (positional args):
# - just chaos-burst 20000 200 0.02
# - just chaos-ws 200 0.3
# - just chaos-restart 5 2

chaos-burst total="50000" concurrency="200" invalid_rate="0.01":
  node tools/chaos/burst_ingest.mjs --total {{total}} --concurrency {{concurrency}} --invalid-rate {{invalid_rate}}

chaos-ws clients="200" slow_ratio="0.3":
  node tools/chaos/ws_clients.mjs --clients {{clients}} --slow-ratio {{slow_ratio}}

chaos-restart loops="5" sleep_sec="2":
  node tools/chaos/restart_compose.mjs --loops {{loops}} --sleep-sec {{sleep_sec}}

# --- Optional: run locally (requires Go 1.22+) ---

run:
  go run ./cmd/engine

