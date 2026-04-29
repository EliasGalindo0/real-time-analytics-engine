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

ingest name="page_view" value="1" route="/":
  curl -sS -i -X POST http://localhost:8080/ingest \
    -H 'content-type: application/json' \
    -d '{"name":"{{name}}","value":{{value}},"tags":{"route":"{{route}}"}}'

metrics:
  curl -sS http://localhost:8080/metrics

# --- Optional: run locally (requires Go 1.22+) ---

run:
  go run ./cmd/engine

