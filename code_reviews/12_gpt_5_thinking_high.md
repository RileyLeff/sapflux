FILE: code_reviews/12_gpt_5_thinking_high.md

Title: Code Review 12 — Make all metadata additions transaction‑compatible (manifest-first)

Summary
You’ve reached a stable, end-to-end baseline: ingestion + batch pipeline + outputs + presigned downloads + compose harness + smoke. The next high-impact win is to fulfill the core reproducibility goal: all metadata additions via transactions, not ad-hoc SQL. Concretely, extend the TOML manifest (and engine) from “deployments + parameter_overrides only” to “one-of-each” metadata entity, with robust preflight and single-transaction apply.

What’s solid and ready to build on

Transactions: PENDING row upfront, serialized execution with advisory lock, outcome update with persisted JSON receipt, dry-run path.
Ingestion: blake3 content hashing, parser attempts and first_error_line in receipts, batch orchestration.
Pipeline: flatten → timestamp_fixer (file-set signatures; timezone-aware UTC) → enrichment (canonical + alias with ambiguity checks) → parameter resolver (JSONB values + provenance) → calculator → quality filters.
Object store: upload-first semantics, presign, list/delete, and GC dry-run/confirm tooling in place.
Outputs: parquet + cartridge upload-first, runs/outputs persisted, is_latest flipped atomically.
Compose + smoke: local E2E validated, including multipart /transactions and presigned output download.
The gaps to close (goal: transaction-driven metadata)

Manifest support only includes [[deployments]] and [[parameter_overrides]]. Everything else must be pre-seeded in DB today.
To achieve fully reproducible metadata, add “add” blocks for:
projects, sites, zones, plots, species, plants, stems
datalogger_types, dataloggers, datalogger_aliases
sensor_types, sensor_thermistor_pairs
(deployments and parameter_overrides already supported)
Keep “update” blocks as a follow-up (nice to have). Start with adds-only to minimize scope and ensure clear failure semantics on duplicates.
Proposed manifest schema (adds only; minimal, practical fields)

projects.add: code, name?, description?
sites.add: code, name?, timezone (IANA), icon_path?, boundary? (accept JSON; store as JSONB or skip for now)
zones.add: site_code, name, boundary? (optional)
plots.add: site_code, zone_name, name, boundary? (optional)
species.add: code, latin_name? (JSON), common_name? (JSON), icon_path?
plants.add: site_code, zone_name, plot_name, species_code, code, location? (JSON point)
stems.add: plant_code, code, dbh_cm? (numeric)
datalogger_types.add: code, name?
dataloggers.add: datalogger_type_code, code (this is the canonical logger_id)
datalogger_aliases.add: datalogger_code, alias, start_timestamp_utc, end_timestamp_utc (enforce no overlaps/adjacency)
sensor_types.add: code, description?
sensor_thermistor_pairs.add: sensor_type_code, name, depth_mm
deployments.add, parameter_overrides: keep as implemented (already live)
Preflight validation (read-only, thorough but fast)

Referential existence: resolve all parent references (e.g., plot requires zone/site; plant requires plot/species; datalogger requires type).
Uniqueness: ensure adds would not violate uniqueness constraints:
codes (project.code, site.code, species.code, datalogger.code, sensor_type.code)
scoped uniqueness (zone name unique within site; plot name within zone; plant code within plot; stem code within plant)
Datetime/Time zone: validate IANA timezone strings for sites; ensure timestamps parse to UTC.
Datalogger alias temporal rules: reject any new alias whose active_during overlaps or touches another alias for the same alias string (mirror DB EXCLUDE USING gist … WITH && and -|-).
SDI-12 address check remains downstream (deployments). Keep a helper for sdi_address one-char ASCII alphanumeric (you already have Sdi12Address).
Geometry and icons: for now, accept optional JSON fields pass-through (or skip if out-of-scope); do not block MVP.
Duplicate-in-manifest: reject repeated adds for the same logical key in the same manifest.
Report all violations in a structured preflight error; receipt outcome REJECTED; no DB writes.
Apply logic (single DB transaction, adds-only, no upserts)

Execute in dependency order:
projects → sites → zones → plots
species → plants → stems
datalogger_types → dataloggers → datalogger_aliases
sensor_types → sensor_thermistor_pairs
deployments → parameter_overrides (existing code)
Every insert uses generated UUIDs; keep natural keys in receipt (codes, names) for operator visibility.
parameter_overrides.effective_transaction_id = triggering transaction_id (already implemented).
On any error, rollback the whole metadata phase and return a REJECTED receipt with details.
Receipt enhancements for metadata

Include metadata_summary with per-entity counts, e.g.:
projects_added, sites_added, zones_added, plots_added, species_added, plants_added, stems_added, datalogger_types_added, dataloggers_added, datalogger_aliases_added, sensor_types_added, sensor_thermistor_pairs_added, deployments_added, parameter_overrides_upserted
Optional: list of natural keys added (codes/names), capped to N per entity for brevity.
Concrete edits (minimal, well-contained)

crates/sapflux-core/src/metadata_manifest.rs
Extend MetadataManifest struct with optional Vecs for each entity listed above (adds only).
Add per-entity Resolved* structs with resolved UUIDs for parents.
Implement preflight_manifest to:
Resolve parents and enforce constraints per above.
Build a ResolvedManifest with all adds; produce MetadataSummary counts.
Implement apply_manifest to:
Run inserts in the dependency order within a single DB tx.
Reuse existing deployments and parameter_overrides logic at the end.
crates/sapflux-core/src/transactions.rs
No structural changes; you already:
Insert PENDING transaction
Preflight (manifest) → apply (if not dry_run)
Ingest + pipeline + publish
Update outcome/receipt
Ensure metadata_summary is included in both dry-run and committed receipts.
docs
planning/writeups/03_transaction_workflow.md: reflect the new “adds” in preflight/apply; clarify “no upserts for adds; use update later”.
planning/writeups/06_cli_and_api_reference.md: add examples of TOML with adds; keep message in multipart field.
planning/writeups/02_database_and_storage.md: unchanged (schema already supports constraints you’ll validate in preflight).
planning/writeups/05_processing_and_calculations.md: unchanged.
Testing plan

Unit (no DB):
TOML parse: ensure all new blocks deserialize; missing required fields rejected.
Preflight (DB-backed):
Happy path: one-of-each add resolves cleanly; counts match.
Ambiguity/duplication: zone/plot/plant/stem uniqueness violations; clear errors.
Time constraints: datalogger_alias overlaps and adjacency → REJECTED with explicit reason.
Bad IANA timezone: REJECTED.
Apply (DB-backed):
Insert order and FK integrity verified by DB; no partial side effects on failure.
Integration (compose):
Post a manifest with one-of-each + two deployments + parameter_overrides; then post a file batch; expect ACCEPTED with outputs produced.
Re-run same manifest: should REJECT on adds (duplicates), confirming idempotency safety (later, updates will cover edits).
Smoke update: optionally extend integration_tests/smoke.sh to include a minimal adds manifest before the existing deployments block.
Acceptance checklist

/transactions accepts a TOML manifest with adds for all metadata entities.
Dry-run: returns REJECTED on any preflight error with detailed reasons; returns ACCEPTED/Skipped pipeline if no files but metadata is valid (status should be Success/Skipped as you currently do).
Committed run: applies metadata in one DB tx, then runs pipeline; outputs published on success.
Receipts include metadata_summary counts.
DB constraints (overlap/adjacency on deployments/aliases, uniqueness) are mirrored in preflight so users get clear, early errors.
Nice-to-have (later)

Updates: add [[*.update]] with selectors that must resolve to exactly one row; “patch” subset of fields; preflight to enforce uniqueness and temporal constraints on the new values.
Asset ingestion: add an assets block with upload-first, private keys and pre-signed URL fetch (out of scope for now, but this will matter for icons).
Rich geometry: accept GeoJSON in manifest and transform to WKT for PostGIS columns.
Example “one-of-each” adds manifest (no deployments)
This shape is what your parser should accept and apply. It’s safe to start drafting real values now.

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

Already supported today:
[[parameter_overrides]]
parameter_code = "parameter_heat_pulse_duration_s"
value = 3.0
site_code = "TEST_SITE"

Risk notes and mitigations

Conflicts between manifest state and DB constraints: preflight must mirror DB constraints to avoid users discovering issues only at apply time.
Upgrade path: manifests created now should remain valid as you add updates; keep field names stable.
Performance: preflight uses targeted SELECTs; expected manifest sizes are small; no need for premature optimization.
Verdict
Proceed. Implement adds-only across all metadata in metadata_manifest.rs with robust preflight and a single apply transaction. This unlocks transactional, reproducible metadata and sets you up to author a single manifest that seeds a whole site and then runs a batch—exactly what you want for onboarding your real data.