#!/usr/bin/env bash
set -euo pipefail

COMPOSE="${COMPOSE:-docker compose}"
STACK_NAME="sapflux-dev"

cleanup() {
  $COMPOSE -p "$STACK_NAME" down -v >/dev/null 2>&1 || true
}

trap cleanup EXIT

echo "==> Starting docker compose stack"
$COMPOSE -p "$STACK_NAME" up -d --build

echo "==> Waiting for services to become healthy"
$COMPOSE -p "$STACK_NAME" wait >/dev/null 2>&1 || true

until curl -sf http://localhost:8080/health >/dev/null; do
  echo "Waiting for API..."
  sleep 2
done

echo "==> Running migrations and seed"
curl -sf -X POST http://localhost:8080/admin/migrate >/dev/null
curl -sf -X POST http://localhost:8080/admin/seed >/dev/null

echo "==> Preparing manifest and sample payload"
TMP_DIR=$(mktemp -d)
cat >"$TMP_DIR/manifest.toml" <<'EOF'
[[deployments]]
project_code = "TEST"
plant_code = "PLANT"
stem_code = "STEM_OUT"
datalogger_code = "420"
sensor_type_code = "sapflux_probe"
sdi_address = "0"
start_timestamp_utc = "2025-07-28T00:00:00Z"
include_in_pipeline = true

[[deployments]]
project_code = "TEST"
plant_code = "PLANT"
stem_code = "STEM_IN"
datalogger_code = "420"
sensor_type_code = "sapflux_probe"
sdi_address = "1"
start_timestamp_utc = "2025-07-28T00:00:00Z"
include_in_pipeline = true
EOF

cp crates/sapflux-parser/tests/data/CR300Series_420_SapFlowAll.dat "$TMP_DIR"

echo "==> Posting multipart transaction"
response=$(curl -sf -X POST http://localhost:8080/transactions \
  -F "message=smoke-test" \
  -F "dry_run=false" \
  -F "metadata_manifest=@$TMP_DIR/manifest.toml" \
  -F "files[]=@$TMP_DIR/CR300Series_420_SapFlowAll.dat")

echo "$response" | jq '.'

status=$(echo "$response" | jq -r '.status')
if [ "$status" != "success" ]; then
  echo "Smoke test failed: status $status"
  exit 1
fi

output_id=$(echo "$response" | jq -r '.receipt.artifacts.output_id')

if [ -z "$output_id" ] || [ "$output_id" = "null" ]; then
  echo "Smoke test failed: missing output id"
  exit 1
fi

echo "==> Downloading output parquet"
download=$(curl -sf "http://localhost:8080/outputs/$output_id/download")
url=$(echo "$download" | jq -r '.url')
curl -sf "$url" -o "$TMP_DIR/output.parquet"

echo "Parquet saved to $TMP_DIR/output.parquet"
echo "Smoke test completed successfully"
