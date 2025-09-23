FILE: planning/agent_index.md

Title: Sapflux agent index and source-of-truth map (updated for transaction-driven metadata adds)

Purpose

Fast orientation for coding agents.
Points to the authoritative code and tests, the active tasks, and where to implement them.
Keeps context small and minimizes re-reading of the entire repo.
Authority and conflict resolution

If code/tests and docs disagree: code and tests in crates/** and migrations are authoritative.
Use the highest-numbered progress_report/statusX.md and code_reviews/N_*.md for “what’s next.”
Planning docs inform intent; do not override working code.
How to find “latest”

Latest progress: open the highest-numbered file in progress_report/.
Latest review: open the highest-numbered file in code_reviews/.
At the time of this index:
Latest progress: status16.md (compose + smoke test online).
Latest review: 12_gpt_5_thinking_high.md (add all metadata via manifest).
Minimal context set (what to load by default)

Must include:
crates/** (all code and tests)
crates/sapflux-core/migrations/0001_init.sql
planning/agent_index.md (this file)
When needed:
Highest-numbered progress_report/statusX.md
Highest-numbered code_reviews/N_*.md
One or two writeups for background (02_database_and_storage.md, 03_transaction_workflow.md, 05_processing_and_calculations.md)
Repository map (where things live)

API server and routes (Axum)
crates/sapflux/src/main.rs
Routes: POST /transactions, GET /health, POST /admin/migrate, POST /admin/seed, GET /outputs/{id}/download
Admin CLI
crates/sapflux-admin/src/main.rs (db-seed, object-store GC)
Database
crates/sapflux-core/src/db/mod.rs (connect, migrate)
crates/sapflux-core/migrations/0001_init.sql (schema, constraints)
Object storage
crates/sapflux-core/src/object_store.rs (Noop, LocalDir, S3/R2; put/presign/list/delete; env-configurable)
GC: crates/sapflux-core/src/object_gc.rs; admin: sapflux-admin object-store-gc
Parsers (reference TOA5 parsers)
crates/sapflux-parser/**
Core wrappers/registry: crates/sapflux-core/src/parsers.rs
Ingestion
crates/sapflux-core/src/ingestion.rs (blake3 hashing, parser attempts, reports)
Flattening
crates/sapflux-core/src/flatten.rs
Timestamp fixer
crates/sapflux-core/src/timestamp_fixer.rs (implied visit via file_set_signature; timezone-aware UTC)
Metadata enrichment
crates/sapflux-core/src/metadata_enricher.rs (canonical/alias resolution; hierarchy IDs; installation_metadata expansion)
Parameters
crates/sapflux-core/src/parameter_resolver.rs (JSONB overrides; precedence; provenance columns)
Calculator and quality
crates/sapflux-core/src/calculator.rs (DMA Péclet HRM/Tmax + switch)
crates/sapflux-core/src/quality_filters.rs (canonical rules; explanations)
Pipelines
crates/sapflux-core/src/pipelines.rs (standard_v1_dst_fix orchestrates flatten → timestamps → enrich → params → calc → quality)
Transactions and receipts
crates/sapflux-core/src/transactions.rs (PENDING row; advisory lock; ingestion; pipeline; outputs; outcome + receipt)
Metadata manifests (TOML)
crates/sapflux-core/src/metadata_manifest.rs
Today supports: deployments, parameter_overrides
Next task: extend to add all other metadata via manifest (see “Current tasks”)
Outputs (publish artifacts)
crates/sapflux-core/src/outputs.rs (serialize Parquet; build cartridge; upload-first; persist runs/outputs; flip is_latest)
Compose harness and smoke test
docker-compose.yaml (db + MinIO + API)
docs/dev-compose.md (how to run)
integration_tests/smoke.sh (migrate/seed → multipart transaction → publish → presigned download)
Data flow (end-to-end)

POST /transactions (multipart: message, manifest, files[]):
Acquire advisory lock; insert PENDING transaction row.
Preflight manifest (read-only); if not dry-run, apply metadata in one DB transaction.
Ingest files: compute blake3; parse with active parsers; collect parser_attempts and first_error_line.
Batch pipeline over parsed set:
flatten → timestamp_fixer (dedupe by (logger_id, record), file_set_signature grouping, timestamp_utc with UTC timezone) → metadata_enricher → parameter_resolver → calculator → quality_filters.
Upload-first raw files (idempotent), then on success persist raw_files rows.
If pipeline success: publish Parquet + cartridge (upload-first), then insert runs/outputs and flip is_latest in one DB transaction.
Update transactions row outcome and store JSON receipt; return receipt to client.
Dry-run: skip DB/object-store writes; still run preflight and pipeline (using current DB state).
Database quick facts

All timestamps TIMESTAMPTZ (UTC in app).
JSONB parameter_overrides.value; precedence resolves to typed columns.
Constraints:
deployments and datalogger_aliases: no overlaps and no adjacency (&& and -|- EXCLUDE constraints).
Scoped uniqueness for names/codes (zone within site, plot within zone, plant code within plot, stem code within plant).
outputs.is_latest: on insert set true and flip others to false in one DB tx.
Object storage keys

raw-files/{file_hash}
outputs/{output_id}.parquet
repro-cartridges/{output_id}.zip
Upload-first semantics; periodic GC deletes unreferenced keys.
Receipts

Ingestion: per-file status (Parsed/Duplicate/Failed), parser_attempts, first_error_line.
Pipeline summary: status (Skipped/Success/Failed), row_count, quality_summary (counts, suspect ratio, top reasons), provenance_summary (top overrides), record_summary (logger_count, sensor_count, timeframe_utc).
Artifacts on ACCEPTED: output_id + keys for parquet/cartridge.
Metadata summary: present when manifest applied (counts per entity).
Testing guidance

Unit tests under crates/sapflux-core/tests/** and crates/sapflux-parser/**.
Integration tests:
crates/sapflux/tests/transactions.rs (DB-gated)
crates/sapflux/tests/object_store_s3.rs (env-gated)
integration_tests/smoke.sh via compose (local E2E)
Key coverage:
Parsers: record +1, logger_id normalization, SDI-12 strictness, header/unit checks.
Timestamp fixer: DST transitions; dedup across overlapping files.
Enrichment: alias ambiguity fast-fail; hierarchy columns present; installation_metadata expansion.
Parameters: defaults + precedence + provenance.
Calculator/quality: HRM/Tmax switch; edge cases; quality rules with explanations.
Transactions: PENDING → outcome; raw_files insert only on acceptance; artifacts published.
Common conventions and invariants

logger_id is canonical per file (enforced by parsers). Aliases are resolved during enrichment when needed.
timestamp_utc is timezone-aware UTC (Polars Datetime with UTC timezone).
Dedup across files by (logger_id, record); “implied visit” grouping by sorted file_set_signature.
parameter_* columns hold resolved values; parameter_source_* columns hold provenance strings.
Quality outputs: quality (null for good, “SUSPECT” otherwise) and quality_explanation (pipe-delimited reasons).
Upload-first before DB writes; GC reclaims orphans.
API notes

POST /transactions: multipart form fields:
message (string)
manifest (TOML text)
files[] (one or more raw files)
dry_run=true|false
GET /outputs/{id}/download:
Returns JSON { url, expires_at } for presigned GET.
Query param include_cartridge=true to presign the cartridge instead of parquet.
Current tasks (from Code Review 12)
Goal: Make all metadata additions transaction-compatible via the manifest.

Extend metadata_manifest.rs (TOML schema and engine) to support adds for all entities:
projects, sites, zones, plots, species, plants, stems
datalogger_types, dataloggers, datalogger_aliases
sensor_types, sensor_thermistor_pairs
deployments and parameter_overrides already supported (keep as-is)
Preflight (read-only):
Resolve parent references; enforce uniqueness rules; validate IANA timezones; validate alias non-overlap and non-adjacency; reject duplicate-in-manifest; accumulate counts.
Return MetadataSummary with per-entity “*_added” counts. On any violation: REJECTED (no DB writes).
Apply (single DB tx, adds-only, no upserts):
Insert in dependency order (projects → sites → zones → plots → species → plants → stems → datalogger_types → dataloggers → datalogger_aliases → sensor_types → sensor_thermistor_pairs → deployments → parameter_overrides).
parameter_overrides.effective_transaction_id = triggering transaction_id.
Transactions flow remains the same:
PENDING row → preflight → apply → ingest/pipeline → publish → outcome+receipt.
Receipts:
Include metadata_summary for both dry-run (preflight) and commit.
Where to implement (files to edit)

crates/sapflux-core/src/metadata_manifest.rs
Extend MetadataManifest with new add arrays; define Resolved* structs and resolve functions.
Implement preflight_manifest() for new entities; return (ResolvedManifest, MetadataSummary).
Implement apply_manifest() to insert all adds in a single DB tx.
crates/sapflux-core/src/transactions.rs
Ensure metadata_summary from preflight/apply is included in the final TransactionReceipt (already plumbed).
Docs:
Update planning/writeups/03_transaction_workflow.md to reflect “adds” preflight+apply.
Update planning/writeups/06_cli_and_api_reference.md with TOML adds examples and multipart usage.
Acceptance criteria for the task

/transactions accepts TOML with adds blocks for every entity listed above.
Dry-run: returns REJECTED on any preflight error (clear reasons); returns ACCEPTED/Skipped (pipeline) if no files but metadata valid.
Commit: applies metadata in one DB tx; pipeline runs; outputs published; receipt includes metadata_summary.
Idempotency safety: reapplying the same adds manifest should REJECT (duplicates) rather than mutate state silently.
Constraints mirrored: preflight catches overlaps/adjacency (aliases, deployments) and scoped uniqueness before DB writes.
Manifest cheat sheet (adds-only example, no deployments)

Example TOML block names and key fields:
[[projects.add]]
code = "TEST"
name = "Test Project"

[[sites.add]]
code = "TEST_SITE"
name = "Test Site"
timezone = "America/New_York"

[[zones.add]]
site_code = "TEST_SITE"
name = "Zone A"

[[plots.add]]
site_code = "TEST_SITE"
zone_name = "Zone A"
name = "Plot 1"

[[species.add]]
code = "SPEC"

[[plants.add]]
site_code = "TEST_SITE"
zone_name = "Zone A"
plot_name = "Plot 1"
species_code = "SPEC"
code = "PLANT"

[[stems.add]]
plant_code = "PLANT"
code = "STEM_OUT"

[[stems.add]]
plant_code = "PLANT"
code = "STEM_IN"

[[datalogger_types.add]]
code = "CR300"
name = "CR300 Series"

[[dataloggers.add]]
datalogger_type_code = "CR300"
code = "420"

[[datalogger_aliases.add]]
datalogger_code = "420"
alias = "ALIAS420"
start_timestamp_utc = "2025-07-01T00:00:00Z"
end_timestamp_utc   = "2025-12-31T23:59:59Z"

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

Already supported today (works now):
[[parameter_overrides]]
parameter_code = "parameter_heat_pulse_duration_s"
value = 3.0
site_code = "TEST_SITE"

Lightweight prompt recipe (to keep tokens small)

Load crates/**, migrations/0001_init.sql, this agent_index.
If working on manifests: open metadata_manifest.rs, transactions.rs, status16.md, 12_gpt_5_thinking_high.md.
Only bring in writeups 02/03/06 if needed.
Ask: “Implement adds-only manifest support with preflight+apply for all metadata; update receipts; ensure DB constraints mirrored.”
Notes and reminders

Parsers enforce strict record +1 and SDI-12 validity; keep that invariant.
Timestamp_fixer must output timezone-aware timestamp_utc (UTC); this is tested.
Keep upload-first semantics; errors during uploads should REJECT and record a diagnostic receipt; GC will clean orphans.
Keep outputs deterministic: stable row/column ordering and pinned writer options are desirable for reproducibility (see outputs.rs).
End of agent index.