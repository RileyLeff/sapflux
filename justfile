set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := true

up:
    docker compose up -d

down:
    docker compose down

logs service="":
    if [ "{{service}}" = "" ]; then \
        docker compose logs -f; \
    else \
        docker compose logs -f {{service}}; \
    fi

seed-bucket:
    uv run scripts/create_bucket.py

migrate-db:
    DATABASE_URL=${DATABASE_URL:-postgres://postgres:postgres@localhost:5432/sapflux} \
    cargo run -p sapflux-repository --bin migrate
