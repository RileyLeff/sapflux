Status 15 review: you’re on track for a no-shortcuts, end-to-end local run. Implementing TOML manifests with preflight+transactional apply and switching /transactions to streaming multipart were the two biggest blockers. From here, the compose harness + integration coverage will get you to “docker-compose up → post a big manifest+files → get a downloadable parquet” reliably.

What looks good

Metadata engine: TOML manifest, preflight validation, and transactional apply. This enables deployments and parameter_overrides to land atomically before the pipeline.
Streaming multipart /transactions: unlocks large batches without base64 bloat.
Receipts + download endpoint: artifact keys in receipts; GET /outputs/{id}/download presigns parquet or cartridge.
Keep/confirm these invariants

Outcome semantics: ACCEPTED even if some files fail to parse; REJECT only on manifest/apply failure, pipeline hard error, or object-store/publish failure.
Ordering:
Preflight manifest + file parsing
Apply metadata in one DB tx
Load context from DB and run the batch pipeline on successfully parsed files
Upload-first (raw files, artifacts) then persist runs/outputs and flip is_latest in one DB tx
Dry-run: no DB/object-store writes; run preflight and file parsing. If you run the pipeline, clarify it uses current DB (not preflight changes).
Recommended next steps (aligned to your Next Focus)

Docker-compose harness (do this now)
Services:
db: postgres + postgis
minio: minio/minio + mc init job to create the bucket
app: the sapflux API (env set for S3 MinIO + DB)
App env (example):
SAPFLUX_OBJECT_STORE_KIND=s3
S3_ENDPOINT_URL=http://minio:9000
S3_REGION=us-east-1
S3_ACCESS_KEY_ID=minio
S3_SECRET_ACCESS_KEY=miniosecret
S3_FORCE_PATH_STYLE=true
DATABASE_URL=postgres://...
Smoke script:
migrate + seed
POST /transactions (multipart): manifest + a handful of files
Expect ACCEPTED receipt; GET /outputs/{id}/download returns working presigned URL; file downloads and passes basic parquet read
2. Integration coverage (manifest + multipart + GC)

Manifest/multipart:
Happy path: adds across the hierarchy (projects→sites→zones→plots→species→plants→stems→dataloggers/types→aliases→sensor_types→deployments→parameter_overrides) + files; receipt shows metadata_summary; pipeline Success/Skipped; artifacts published
Selector ambiguity: update with selector resolving to 0 or >1 → REJECT with structured error
Overlap/adjacency: deployments and aliases that violate range constraints → REJECT
Large batch: hundreds of files to validate streaming resource usage and stability
GC dry-run/confirm:
Create a known orphan under each prefix (raw-files/, outputs/, repro-cartridges/), verify dry-run lists only orphans; confirm deletes them; referenced keys untouched
3. Deterministic output guardrails (soon after)

Stable row order before Parquet (e.g., timestamp_utc, deployment_id, sdi12_address, thermistor_depth, record)
Fixed column order
Pinned writer settings (document compression choice). If bit-identical reproducibility is a goal, prefer uncompressed or lock versions
Operational hardening (small but important)

Multipart limits/timeouts: raise max body size; bound per-part buffering; stream hash computation; log per-file timing
Presigned URLs: configurable expiry (e.g., 15 min), note clock skew in docs; MinIO path-style vs R2 virtual-host style is already handled by config
Receipts:
Keep artifact keys; optionally include short-lived presigned URLs (nice for UX, not required)
Keep quality/provenance summaries bounded (top-N reasons/overrides)
Transactions bookkeeping:
runs: set SUCCESS/FAILED; include git_commit_hash and run_log (row_count, quality_summary) for auditability
outputs: insert + is_latest flip in one DB tx
Acceptance checklist for “first local E2E”

docker compose up (db+minio+api)
POST /transactions (multipart) with a real manifest and multiple files
Receipt outcome ACCEPTED; ingestion_summary present; metadata_summary present; pipeline row_count present (or Skipped); artifact keys present
DB: runs + outputs rows present; latest flag flipped atomically
Download: GET /outputs/{id}/download returns a presigned URL that fetches a valid parquet
GC dry-run shows no referenced keys; confirm deletes only a known orphan
If you want, I can provide:

A docker-compose.yml skeleton with MinIO + mc init job
A curl example for multipart posting of manifest + files
A minimal GC dry-run/confirm endpoint sketch
But from Status 15, you’re in the “wire it up and prove it” phase. Proceed with the compose harness and integration tests, then tighten determinism and GC.