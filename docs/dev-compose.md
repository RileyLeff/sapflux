# Docker Compose Harness

This stack brings up Postgres, MinIO, and the Sapflux API so you can exercise the full metadata + ingestion + publish flow locally.

```bash
docker compose up --build
./integration_tests/smoke.sh
```

`smoke.sh` will:

1. Wait for services to become healthy.
2. Call `/admin/migrate` and `/admin/seed`.
3. POST a multipart transaction (`metadata_manifest` + sample CR300 file).
4. Print the receipt JSON (pipeline status, metadata summary, artifact keys).
5. Fetch `/outputs/{id}/download` and download the parquet to a temp directory.

Buckets/paths:

- `raw-files/{hash}` for raw uploads.
- `outputs/{output_id}.parquet` for published data.
- `repro-cartridges/{output_id}.zip` for cartridges.

You can inspect MinIO via the console at http://localhost:9001 (user `minio`, password `miniosecret`).
