#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
COMPOSE="${COMPOSE:-docker compose}"
STACK_NAME="sapflux-rileydata"
MANIFEST_PATH="$REPO_ROOT/rileydata/transaction/meta_tx.toml"
OUTPUT_DIR="$REPO_ROOT/rileydata/output"

mkdir -p "$OUTPUT_DIR"

cleanup() {
  $COMPOSE -p "$STACK_NAME" down -v >/dev/null 2>&1 || true
}

trap cleanup EXIT

printf '==> Starting docker compose stack\n'
$COMPOSE -p "$STACK_NAME" down -v >/dev/null 2>&1 || true
$COMPOSE -p "$STACK_NAME" up -d --build

printf '==> Waiting for services to become healthy\n'
$COMPOSE -p "$STACK_NAME" wait >/dev/null 2>&1 || true

until curl -sf http://localhost:8080/health >/dev/null; do
  printf 'Waiting for API...\n'
  sleep 2
done

printf '==> Running migrations and seed\n'
curl -sf -X POST http://localhost:8080/admin/migrate >/dev/null
curl -sf -X POST http://localhost:8080/admin/seed >/dev/null

printf '==> Ensuring object-store bucket\n'
$COMPOSE -p "$STACK_NAME" run --rm --entrypoint '' minio-init \
  sh -c "mc alias set --api s3v4 local http://minio:9000 minio miniosecret >/dev/null && mc mb --ignore-existing local/sapflux >/dev/null && mc anonymous set download local/sapflux >/dev/null"

printf '==> Preparing placeholder file\n'
DATA_FILE="$REPO_ROOT/../crates/sapflux-parser/tests/data/CR300Series_420_SapFlowAll.dat"
if [ ! -f "$DATA_FILE" ]; then
  echo "Placeholder data file not found: $DATA_FILE"
  exit 1
fi

printf '==> Posting metadata manifest (%s)\n' "$MANIFEST_PATH"
response_file=$(mktemp)
set +e
http_code=$(curl -sS -o "$response_file" -w "%{http_code}" \
  -X POST http://localhost:8080/transactions \
  -F "message=rileydata-meta" \
  -F "dry_run=false" \
  -F "metadata_manifest=@$MANIFEST_PATH" \
  -F "files[]=@$DATA_FILE")
curl_status=$?
set -e

if [ "$curl_status" -ne 0 ]; then
  cat "$response_file"
  rm -f "$response_file"
  echo "Smoke test failed: curl error ($curl_status)"
  exit 1
fi

response=$(cat "$response_file")
rm -f "$response_file"

parsed_json=true
if ! echo "$response" | jq '.' >/dev/null 2>&1; then
  parsed_json=false
  echo "Raw response (non-JSON):"
  echo "$response"
else
  echo "$response" | jq '.'
fi

status=""
if [ "$parsed_json" = true ]; then
  status=$(echo "$response" | jq -r '.status // empty')
fi
if [ -z "$status" ]; then
  echo "Smoke test failed: missing status field"
  exit 1
fi

if [ "$status" != "success" ]; then
  echo "Smoke test failed: status $status (HTTP $http_code)"
  exit 1
fi

if [ "$parsed_json" = true ]; then
  summary=$(echo "$response" | jq '.receipt.metadata_summary')
  if [ "$summary" = "null" ]; then
    echo "Smoke test warning: metadata summary missing"
  else
    echo "Metadata summary:"
    echo "$summary" | jq '.'
  fi
fi

echo "Smoke rileydata test completed successfully"
