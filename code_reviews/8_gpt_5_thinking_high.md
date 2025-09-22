Excellent progress. Status 14 lands the publish/download loop and puts you in a good spot to go fully end-to-end locally.

Recommendation on priorities

Keep the same overall plan, but promote “metadata manifest + multipart ingestion” from “prepare” to the next blocking milestone. That’s the last major piece to reach “one transaction ingests full metadata + many files, publishes output” without shortcuts. You can do the docker-compose harness in parallel, but don’t defer the manifest/multipart work.
What’s good now

Outputs publishing: parquet + cartridge upload-first; runs/outputs rows recorded; is_latest flipping done pre-commit. Good.
Receipts contain artifact references; GET /outputs/{id}/download presigns parquet or cartridge. Good.
S3 backend smoke test exists. Good.
Next steps (concrete, ordered)

Metadata transaction engine (blocker to full local E2E)
Manifest schema (TOML): add/update blocks for projects, sites, zones, plots, species, plants, stems, datalogger_types, dataloggers, datalogger_aliases, sensor_types, deployments, parameter_overrides.
Preflight (read-only):
Resolve selectors; enforce uniqueness/FKs; validate deployment/alias ranges (no overlaps/adjacency).
Produce a metadata_summary in the receipt (counts + any validation errors).
Apply (single DB tx, dependency order):
projects → sites → zones → plots → species → plants → stems → datalogger_types → dataloggers → aliases → sensor_types → deployments → parameter_overrides.
Reject on conflict/ambiguity; bubble clear errors into the receipt.
After commit: load ExecutionContext from DB, then run pipeline on successfully parsed files.
Tests:
Unit: manifest parsing and selector resolution.
Integration: add/update happy paths, overlap/adjacency rejection, selector ambiguity.
2. Switch /transactions to streaming multipart (blocker for backlogs)

Handler: manifest (text/toml) + files[] parts; stream each file to hash (blake3), optionally upload-first to object store (idempotent), then parse → per-file report.
Increase body limits/timeouts; keep per-part bounded memory.
Dry-run: do preflight + parsing, but no DB/object writes (document that pipeline runs against current DB state).
Tests: submit hundreds of files locally (compose) without OOM; receipt shows parsed/failed/duplicates.
3. Docker-compose harness (can be parallelized)

Services: Postgres+PostGIS, MinIO (with mc init), API.
App env for MinIO:
SAPFLUX_OBJECT_STORE_KIND=s3
S3_ENDPOINT_URL=http://minio:9000
S3_REGION=us-east-1
S3_ACCESS_KEY_ID/SECRET
S3_FORCE_PATH_STYLE=true
Smoke script:
Run seed.
POST multipart (manifest + small file batch).
Check ACCEPTED receipt and download parquet via GET /outputs/{id}/download.
4. Receipt polish (optional)

Include artifact keys (done). Optionally embed short-lived presigned URLs if you want “click-to-download” UX directly from receipts; otherwise leave that to the download endpoint.
Keep summaries bounded (quality_summary, provenance_summary) to avoid large receipts.
5. GC tooling (dry-run → confirm)

Compare object store lists under raw-files/, outputs/, repro-cartridges/ to DB references.
Add dry-run admin command/endpoint; add confirm mode (optionally only delete > N hours old).
Test: create orphan → GC dry-run reports it → confirm deletes it.
6. Deterministic output guardrails (if not already pinned)

Stable row sort (e.g., timestamp_utc, deployment_id, sdi12_address, thermistor_depth, record) and fixed column order before Parquet write.
Consider uncompressed or pinned compression settings to maximize bit-identical results (document assumptions).
Acceptance checklist for “local end-to-end”

docker compose up brings DB + MinIO + API.
POST /transactions (multipart) with a real manifest + many files:
Metadata applied; some files may fail parse (receipt shows them), pipeline runs on the rest.
Outcome ACCEPTED; receipt: ingestion_summary, metadata_summary, pipeline summaries, artifact refs.
runs/outputs rows exist; latest flag flipped; download endpoint presigns and returns the parquet.
GC dry-run finds no referenced keys; confirm mode deletes only true orphans.
If you want, I can draft:

Manifest Rust structs + a minimal TOML example,
Axum Multipart handler skeleton (streaming hash + staging),
Preflight/apply DB routines in dependency order,
A docker-compose.yml and mc init job snippet.
But yes: proceed with the same outlook, with metadata manifest + multipart promoted to the next immediate milestone.