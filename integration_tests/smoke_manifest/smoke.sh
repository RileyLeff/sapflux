#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
COMPOSE="${COMPOSE:-docker compose}"
STACK_NAME="sapflux-manifest-smoke"
OUTPUT_DIR="$REPO_ROOT/smoke_manifest/output"

mkdir -p "$OUTPUT_DIR"

cleanup() {
  $COMPOSE -p "$STACK_NAME" down -v >/dev/null 2>&1 || true
}

printf '==> Starting docker compose stack\n'
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

TMP_DIR=$(mktemp -d)

cleanup_all() {
  cleanup
  rm -rf "$TMP_DIR"
}

trap cleanup_all EXIT INT TERM

cat >"$TMP_DIR/manifest.toml" <<'EOF_MANIFEST'
[[projects.add]]
code = "TXN_TEST"
name = "Transaction Smoke Test"

[[sites.add]]
code = "TXN_SITE"
name = "Transaction Site"
timezone = "UTC"

[[zones.add]]
site_code = "TXN_SITE"
name = "Zone A"

[[plots.add]]
site_code = "TXN_SITE"
zone_name = "Zone A"
name = "Plot 1"

[[species.add]]
code = "TXN_SPECIES"
common_name = { en = "Smoke" }

[[plants.add]]
site_code = "TXN_SITE"
zone_name = "Zone A"
plot_name = "Plot 1"
species_code = "TXN_SPECIES"
code = "PLANT01"

[[stems.add]]
plant_code = "PLANT01"
code = "STEM_OUT"

[[stems.add]]
plant_code = "PLANT01"
code = "STEM_IN"

[[datalogger_types.add]]
code = "CR300"
name = "CR300 Series"

[[dataloggers.add]]
datalogger_type_code = "CR300"
code = "420"

[[datalogger_aliases.add]]
datalogger_code = "420"
alias = "LOGGER420"
start_timestamp_utc = "2024-01-01T00:00:00Z"
end_timestamp_utc = "2026-01-01T00:00:00Z"

[[sensor_types.add]]
code = "sapflux_probe"
description = "Sapflux thermal sensor"

[[sensor_thermistor_pairs.add]]
sensor_type_code = "sapflux_probe"
name = "inner"
depth_mm = 10

[[sensor_thermistor_pairs.add]]
sensor_type_code = "sapflux_probe"
name = "outer"
depth_mm = 5

[[deployments]]
project_code = "TXN_TEST"
plant_code = "PLANT01"
stem_code = "STEM_OUT"
datalogger_code = "420"
sensor_type_code = "sapflux_probe"
sdi_address = "0"
start_timestamp_utc = "2025-01-01T00:00:00Z"
include_in_pipeline = true

[[deployments]]
project_code = "TXN_TEST"
plant_code = "PLANT01"
stem_code = "STEM_IN"
datalogger_code = "420"
sensor_type_code = "sapflux_probe"
sdi_address = "1"
start_timestamp_utc = "2025-01-01T00:00:00Z"
include_in_pipeline = true

[[parameter_overrides]]
parameter_code = "parameter_heat_pulse_duration_s"
value = 3.0
site_code = "TXN_SITE"
EOF_MANIFEST

cp "$REPO_ROOT/../crates/sapflux-parser/tests/data/CR300Series_420_SapFlowAll.dat" "$TMP_DIR/CR300Series_420_SapFlowAll.dat"

printf '==> Posting manifest transaction\n'
response=$(curl -sf -X POST http://localhost:8080/transactions \
  -F "message=manifest-smoke" \
  -F "dry_run=false" \
  -F "metadata_manifest=@$TMP_DIR/manifest.toml" \
  -F "files[]=@$TMP_DIR/CR300Series_420_SapFlowAll.dat")

echo "$response" | jq '.'

status=$(echo "$response" | jq -r '.status')
if [ "$status" != "success" ]; then
  echo "Smoke test failed: status $status"
  exit 1
fi

receipt_metadata=$(echo "$response" | jq '.receipt.metadata_summary.projects_added')
if [ "$receipt_metadata" = "null" ]; then
  echo "Smoke test failed: metadata summary missing"
  exit 1
fi

output_id=$(echo "$response" | jq -r '.receipt.artifacts.output_id')
if [ -z "$output_id" ] || [ "$output_id" = "null" ]; then
  echo "Smoke test failed: missing output id"
  exit 1
fi

parquet_key=$(echo "$response" | jq -r '.receipt.artifacts.parquet_key')

printf '==> Downloading output parquet\n'
download_json=$(curl -sf "http://localhost:8080/outputs/$output_id/download")
echo "$download_json" | jq '.' >/dev/null

timestamp=$(date +%Y%m%d_%H%M%S)
output_path="$OUTPUT_DIR/manifest_smoke_${timestamp}.parquet"

$COMPOSE -p "$STACK_NAME" run --rm --entrypoint '' minio-init \
  sh -c "mc alias set --api s3v4 local http://minio:9000 minio miniosecret >/dev/null && mc cat local/sapflux/$parquet_key" \
  > "$output_path"

printf 'Parquet saved to %s\n' "$output_path"
printf 'Smoke manifest test completed successfully\n'
