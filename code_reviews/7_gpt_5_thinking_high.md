Green light to continue. The direction in Update 13 is exactly what we planned: you’ve moved from “stable core” to “publish + storage.” A few focused checks and additions will make this solid and production-ready.

What looks good

Storage integration: ObjectStore now has put/presign/list/delete and is exercised in a gated MinIO/R2 smoke test. Good foundation for both local and prod.
Upload-first for artifacts: Parquet + cartridge written before DB commit mirrors our raw-file pattern. Correct approach.
Outputs publishing: Serializes the pipeline DataFrame, uploads artifacts, records runs/outputs, and flips is_latest. That’s the right lifecycle.
Gaps and recommendations before/while proceeding

Transaction/run/output orchestration and failure semantics
Ensure the sequence is explicit and enforced:
Run pipeline → upload parquet + cartridge (upload-first) → begin DB tx → insert runs (SUCCESS/FAILED), insert outputs, flip is_latest, commit.
On any failure (upload or DB), mark the transaction outcome REJECTED and include error in receipt; the orphaned objects are safe for GC.
runs row details:
processing_pipelines FK: write the pipeline_id you used.
status: SUCCESS or FAILED (FAILED if any publish step fails).
started_at/finished_at, git_commit_hash, optional run_log (JSON summary: row_count, quality summary).
outputs row details:
Set is_latest=true and in the same DB transaction flip all other rows to false.
2. Deterministic outputs (parquet) for reproducibility

To keep bit-identical results across re-runs:
Canonicalize row order before writing (e.g., sort by timestamp_utc, deployment_id, sdi12_address, thermistor_depth, record).
Choose deterministic writer options:
Consider uncompressed or zstd with fixed settings; note that compression libraries and Parquet created_by can impact bit identity across versions. If true bit-identical is a hard requirement, use uncompressed or document version pinning.
Fix schema ordering (stable column order across runs).
Avoid embedding non-deterministic metadata (timestamps) in the writer if possible.
3. Presigned download endpoint

Add GET /outputs/{id}/download to:
Return JSON { url, expires_at } or a 302 to presigned URL.
Keep bucket private; accept include_cartridge=true to presign the cartridge instead of parquet.
4. Receipts: include artifact references

In ACCEPTED transactions, include artifact keys (or presigned URLs if you want to be extra helpful) in the receipt:
outputs_key (outputs/{output_id}.parquet)
cartridge_key (repro-cartridges/{output_id}.zip)
Optionally presigned URLs with short expiry; otherwise, let the client call the download endpoint.
5. GC hardening

Use list_prefix for raw-files/, outputs/, repro-cartridges/ and compare to DB:
raw-files: raw_files.file_hash → raw-files/{hash}
outputs: outputs.object_store_path
cartridges: outputs.reproducibility_cartridge_path
Keep GC in dry-run mode initially; add “older than N hours” retention for deletes to avoid edge races.
6. MinIO/R2 addressing parity

Ensure S3 client config supports:
MinIO: http, force_path_style=true, region=us-east-1
R2: https account endpoint, force_path_style=false, region placeholder (us-east-1)
Presigner must honor path-style vs virtual-host style to generate correct URLs for both backends.
7. Tests to add next

Outputs integration (behind DB + S3 env guard):
Commit a transaction; assert:
runs row inserted with SUCCESS; outputs row inserted; is_latest flipped atomically.
Parquet key exists in object store; presigned URL fetch succeeds (200).
Optional: include_cartridge=true path returns the zip URL; fetch works.
Determinism smoke test (best-effort):
Run the same pipeline twice against same data; if you choose uncompressed and a pinned writer, compare object ETag or content hash to assert stability (only if feasible in your environment).
GC dry-run:
Create an orphan key under raw-files/ (not in DB), run GC dry-run, assert it’s listed as would-delete; referenced keys are not flagged.
Suggested next implementation steps (ordered)

Wire GET /outputs/{id}/download (pre-signed response).
Add artifact references to receipts on ACCEPTED transactions.
Add docker-compose harness:
MinIO (with mc init), Postgres, app; pre-wire env vars for S3 + DB.
Fortify outputs publishing code paths with explicit transaction boundaries and clear error returns:
If any upload or DB step fails, bubble up and produce a REJECTED receipt—consistent with existing raw-file behavior.
Add the new integration tests (outputs + download; GC dry-run).
Nice-to-have (non-blocking)

Consider returning pipeline row_count and quality_summary in the runs.run_log JSON; helpful for audits.
Optionally expose an admin endpoint to run GC dry-run and return the report (in addition to CLI).
Bottom line
You’re on the right track. Proceed with:

S3/R2 compose harness
Receipt artifact references
Download endpoint (presign)
GC dry-run against real uploads
Outputs determinism tweaks

Here's an addendum to the review regarding the longer-term path towards a complete working test of the system:

Here’s a focused review and marching orders for the next phase, assuming no shortcuts and aiming for a fully local, end-to-end run that ingests your full metadata and data backlog and publishes downloadable outputs.

Summary

Status 13 is on track: storage integration, outputs publishing, and object-store helpers set the foundation.
To reach “spin up locally and run a full backlog in one transaction” without shortcuts, the next big rocks are:
a real metadata transaction engine (manifest parse → preflight → apply in a DB tx),
a streaming multipart /transactions endpoint (manifest + many files),
deterministic outputs + download endpoint,
a docker-compose harness, and
GC tooling hardened.
Keep the outlook from the previous plan, and add the metadata engine and multipart work at the front of the queue.

Priority roadmap (no shortcuts)

Metadata transaction engine
Goal: From a single manifest (TOML), add/update all metadata entities atomically before the batch pipeline runs. No manual SQL.
Scope: projects, sites, zones, plots, species, plants, stems, datalogger_types, dataloggers, datalogger_aliases, sensor_types, deployments, parameter_overrides.
Manifest shape (example):
message = "Initial BNWR site + deployments"
[[sites.add]] code="BNWR" name="..." timezone="America/New_York"
[[zones.add]] site_code="BNWR" name="Upland Forest"
[[deployments.add]] datalogger_code="420" sdi_address="0" start_timestamp_utc="..." stem={ site="BNWR", plot="...", plant="...", stem_code="1" } sensor_type_code="sapflux_probe"
[[parameter_overrides.add]] code="quality_max_flux_cm_hr" value=35.0 selector = { species_code="PITA" }
[[deployments.update]] selector={ datalogger_code="420", sdi_address="0", start_timestamp_utc="..." } patch={ end_timestamp_utc="..." }
Semantics:
add: insert must fail on conflicts; the engine should reject with a clear receipt error (no silent upserts).
update: selector must resolve to exactly one row; else reject.
Idempotency: running the same manifest twice should either be rejected (adds) or be no-op for updates that don’t change anything.
Implementation notes:
Create a new module (e.g., sapflux-core/src/metadata_tx.rs) with:
parse_manifest(bytes) -> Manifest struct (serde + toml).
preflight(pool, &Manifest) -> Result<PreflightReport, Error>; resolves selectors, validates FK existence, checks temporal constraints (deployments + aliases overlap/adjacency).
apply(pool, &Manifest) -> runs in a single DB tx; inserts/updates in dependency order.
Order of operations:
projects → sites → zones → plots → species → plants → stems → datalogger_types → dataloggers → datalogger_aliases → sensor_types → deployments → parameter_overrides.
Reuse DB exclusion constraints for temporal validation; still surface user-friendly errors in receipts.
Receipts:
Include a metadata_summary: counts of add/update by entity; errors aligned to the entity and selector.
Acceptance criteria:

Dry-run (/transactions?dry_run=true) validates the manifest and returns a receipt with metadata_summary; no DB writes.
Commit: applies metadata in a DB tx, then runs the pipeline (so metadata is visible), then publishes outputs + receipt with artifacts.
2. Streaming multipart /transactions

Goal: Handle a manifest + a large set (hundreds/thousands) of files without base64 overhead or excessive memory use.
Changes:
Switch handler to axum::extract::Multipart.
Manifest part: read to string; parse to Manifest.
File parts: stream to memory/tempfile, compute blake3 incrementally; optional upload-first to object store (idempotent) before parsing; then parse and build per-file reports as you do now.
Increase body size limits/timeouts; ensure request doesn’t OOM (bounded buffering).
API contract:
POST /transactions multipart/form-data with fields:
manifest (text/toml)
files[] (one per raw file)
dry_run=true|false (optional)
Receipt:
Keep your existing ingestion_summary, parser_attempts, quality/provenance summaries; add artifact keys when ACCEPTED.
Acceptance criteria:

Multipart with 100s files works locally (compose); pipeline runs; receipt OK.
Dry-run behaves identically minus DB/object-store writes.
3. Deterministic outputs + download endpoint

Deterministic write:
Sort rows before writing (e.g., timestamp_utc, deployment_id, sdi12_address, thermistor_depth, record).
Fix column order consistently.
Choose a stable Parquet writer config (uncompressed or pinned settings) to maximize bit-identical outputs (document assumptions).
Endpoints:
GET /outputs/{output_id}/download → pre-signed URL (or 302) to parquet.
GET /outputs/{output_id}/download?include_cartridge=true → pre-signed cartridge.
Receipts:
On ACCEPTED: include outputs_key and cartridge_key; optionally pre-signed URLs with short expiry.
Acceptance criteria:

After a commit, outputs row exists; is_latest flipped atomically; download endpoint returns a working URL.
4. Docker compose harness

docker-compose.yml with services:
db: Postgres + PostGIS
minio: MinIO + mc init job to create bucket
app: sapflux API; env configured for S3 MinIO endpoint and DB.
A small smoke script:
seeds DB
posts a multipart manifest + small file batch
verifies ACCEPTED receipt; runs/outputs written; downloads output Parquet.
5. GC tooling (dry-run → confirm)

Use object store list_prefix/delete to:
Identify raw-files/, outputs/, repro-cartridges/ keys not referenced in DB.
Dry-run reporting; then a confirm mode.
Optional retention (only delete unreferenced > N hours old).
Acceptance criteria:

Create a known orphan; GC dry-run reports it; confirm deletes it; referenced objects remain untouched.
Concrete PR-sized tasks

PR1: Metadata manifest scaffolding

Add Manifest types and TOML parser.
Add unit tests for parsing and basic validation (selectors present, types correct).
PR2: Metadata preflight + apply

Implement preflight DB lookups and error mapping (exclusion violations, missing FKs).
Implement apply() with DB tx and dependency order.
Integration tests: add/update happy path; overlap/adjacency rejection; selector ambiguity.
PR3: Switch /transactions to multipart

Axum Multipart handler (manifest + files).
Stream blake3; optionally upload-first to object store for raw files (idempotent); then parse.
Update receipt plumbing to include metadata_summary.
Gated integration test (compose): submit many files.
PR4: Deterministic Parquet + download endpoint

Ensure stable row/column ordering; write Parquet; upload-first artifact publish.
GET /outputs/{id}/download (+ include_cartridge flag).
Integration test (compose + S3): presigned URL fetch returns the correct file.
PR5: Docker compose + local harness

Compose file with db/minio/app; mc init job; example .env.
Script to run a full ingestion (manifest + files), download output.
PR6: GC dry-run + confirm

list_prefix + delete across prefixes; dry-run endpoint/CLI; confirm flag.
Compose test: create orphan → GC dry-run/confirm.
Pitfalls to watch

Keep transaction atomicity: metadata DB tx must commit before running the pipeline; artifact uploads occur upload-first; DB rows for runs/outputs updated only after all uploads succeed.
Error mapping: return clear, structured errors for selector ambiguity, uniqueness violations, overlap/adjacency, and object-store failures (receipt should show the cause).
Multipart resource usage: cap per-part buffer, stream hash; avoid loading entire payload into memory.
MinIO vs R2: force_path_style=true for MinIO; virtual-hosted style for R2; presigner respects both.
What to keep as-is

Advisory lock around /transactions.
Upload-first semantics for raw files and artifacts.
Quality/provenance summaries in receipts.
Batch pipeline composition and ExecutionContext loader.
If you want, I can draft:

a minimal manifest TOML schema and Rust structs (serde) for add/update ops with selectors and patches,
an Axum Multipart handler skeleton with streaming hash + staging,
and a download endpoint snippet with presign.
But yes: keep the plan above and add the metadata engine + multipart first. That will get you to a robust, local, end-to-end run without shortcuts.