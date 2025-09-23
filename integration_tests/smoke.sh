#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
COMPOSE="${COMPOSE:-docker compose}"
STACK_NAME="sapflux-dev"
OUTPUT_DIR="$REPO_ROOT/integration_tests/output"

mkdir -p "$OUTPUT_DIR"

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

echo "==> Seeding lookup metadata"
$COMPOSE -p "$STACK_NAME" exec -T db psql -U sapflux -d sapflux <<'SQL'
INSERT INTO projects (project_id, code, name)
VALUES ('00000000-0000-0000-0000-000000000101', 'TEST', 'Test Project')
ON CONFLICT (code) DO NOTHING;

INSERT INTO species (species_id, code)
VALUES ('00000000-0000-0000-0000-000000000501', 'SPEC')
ON CONFLICT (code) DO NOTHING;

INSERT INTO sites (site_id, code, name, timezone)
VALUES ('00000000-0000-0000-0000-000000000201', 'TEST_SITE', 'Test Site', 'UTC')
ON CONFLICT (code) DO NOTHING;

INSERT INTO zones (zone_id, site_id, name)
VALUES ('00000000-0000-0000-0000-000000000301', '00000000-0000-0000-0000-000000000201', 'Zone A')
ON CONFLICT (site_id, name) DO NOTHING;

INSERT INTO plots (plot_id, zone_id, name)
VALUES ('00000000-0000-0000-0000-000000000401', '00000000-0000-0000-0000-000000000301', 'Plot 1')
ON CONFLICT (zone_id, name) DO NOTHING;

INSERT INTO plants (plant_id, plot_id, species_id, code)
VALUES ('00000000-0000-0000-0000-000000000601', '00000000-0000-0000-0000-000000000401', '00000000-0000-0000-0000-000000000501', 'PLANT')
ON CONFLICT (plot_id, code) DO NOTHING;

INSERT INTO stems (stem_id, plant_id, code)
VALUES
    ('00000000-0000-0000-0000-000000000602', '00000000-0000-0000-0000-000000000601', 'STEM_OUT'),
    ('00000000-0000-0000-0000-000000000603', '00000000-0000-0000-0000-000000000601', 'STEM_IN')
ON CONFLICT (plant_id, code) DO NOTHING;

INSERT INTO datalogger_types (datalogger_type_id, code, name)
VALUES ('00000000-0000-0000-0000-000000000701', 'CR300', 'CR300 Series')
ON CONFLICT (code) DO NOTHING;

INSERT INTO dataloggers (datalogger_id, datalogger_type_id, code)
VALUES ('00000000-0000-0000-0000-000000000801', '00000000-0000-0000-0000-000000000701', '420')
ON CONFLICT (code) DO NOTHING;

INSERT INTO sensor_types (sensor_type_id, code, description)
VALUES ('00000000-0000-0000-0000-000000000901', 'sapflux_probe', 'Sapflux thermal sensor')
ON CONFLICT (code) DO NOTHING;
SQL

echo "==> Ensuring object-store bucket"
$COMPOSE -p "$STACK_NAME" run --rm --entrypoint '' minio-init \
  sh -c "mc alias set --api s3v4 local http://minio:9000 minio miniosecret >/dev/null && mc mb --ignore-existing local/sapflux >/dev/null && mc anonymous set download local/sapflux >/dev/null"

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
echo "$download" | jq '.' >/dev/null
parquet_key=$(echo "$response" | jq -r '.receipt.artifacts.parquet_key')
timestamp=$(date +%Y%m%d_%H%M%S)
output_path="$OUTPUT_DIR/smoke_output_${timestamp}.parquet"

$COMPOSE -p "$STACK_NAME" run --rm --entrypoint '' minio-init \
  sh -c "mc alias set --api s3v4 local http://minio:9000 minio miniosecret >/dev/null && mc cat local/sapflux/$parquet_key" \
  > "$output_path"

echo "Parquet saved to $output_path"
echo "Smoke test completed successfully"

rm -rf "$TMP_DIR"
