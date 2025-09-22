Here’s a focused review of what’s in the repo today, what needs fixing, and concrete next steps in the style you asked for.

State and evaluation

Batch pipeline: The standard_v1_dst_fix pipeline is present and calls the full chain: flatten → timestamp_fixer → metadata_enricher → parameter_resolver → calculator (DMA Péclet) → quality_filters. This matches the plan. Calculator and filters are implemented in sapflux-core and have unit tests.
Transactions: POST /transactions exists, inserts a PENDING row (non-dry-run), serializes execution with a DB advisory lock, runs ingestion, executes the batch pipeline, updates outcome+receipt, and persists raw_files on ACCEPTED. Upload-first pattern is in place against a filesystem-backed object store with env-based config. Good.
Timestamp fixing: Implements the “implied visit” by deduping (logger_id, record), computing a sorted file_set_signature per record, grouping by (logger_id, file_set_signature), anchoring on min(record), resolving DST via site TZ, and joining timestamp_utc back. Cast uses timezone-aware UTC. Good.
Metadata enrichment: Resolves canonical datalogger_id or alias, does temporal joins to deployments, emits full hierarchy IDs, hydrates installation_metadata keys as columns. Good.
Parameter cascade: Canonical definitions (including quality_* thresholds), precedence and provenance are implemented. Good.
Fix these now

tests/pipeline.rs is asserting calculations and quality columns before the calculator runs
Problem: The test calls timestamp_fixer::correct_timestamps directly, then asserts calculation_method_used, sap_flux_density_j_dma_cm_hr, and quality exist. Those columns are only added by the calculator and quality_filters in the pipeline.
Fix: Change this test to execute the pipeline (StandardPipelineStub) over a parsed batch with a real ExecutionContext (or limit it to timestamp-only assertions). Suggested change:
Build context (you already have ExecutionContext::load_from_db for runtime; for a pure unit test, construct an ExecutionContext with timestamp_sites/deployments, enrichment_deployments, aliases, and canonical parameter definitions; or mark as integration behind the DB guard).
Run pipeline.run_batch(&context, &[&parsed_a, &parsed_b]) and assert the calculation+quality columns are present there.
Or, if you want to keep this as a timestamp-fixer unit test, drop the calculator/quality assertions and only assert UTC timestamps + file_set_signature.
2. sapflux-core parser wrappers call the aggregator, so attempts and identity are wrong

Problem: crates/sapflux-core/src/parsers.rs wraps sapflux-parser with two “parsers” (sapflow_all_v1, cr300_table_v1), but both wrappers call parse_sapflow_file (which tries both formats). That means:
The first wrapper always “succeeds” no matter the format, and attempts data is misleading.
You don’t get per-parser attempt granularity.
Fix:
Import and use the specific format parsers directly:
For SapFlowAllParserV1.parse: call sapflux_parser::formats::SapFlowAllParser::default().parse(content)
For Cr300TableParserV1.parse: call sapflux_parser::formats::Cr300TableParser::default().parse(content)
Keep descriptors as-is; only the parse() body changes.
3. Quality filter “record gap” rule does not sort by record, so it can mis-flag

Problem: quality_filters::apply_quality_filters iterates df row order and uses a last_timestamp map keyed by logger_id. The spec requires sorting by record within each logger before computing gaps. Without sorting, any shuffled order could spuriously trigger record_gap_gt_quality_gap_years.
Fix:
Either sort a stable index by (logger_id, record) before iterating gaps, or pre-group by logger and process each group ordered by record. Keep other rules unchanged. Add a unit test that would fail with the current unsorted traversal.
4. Minor: keep or drop file_set_signature in the final table

Clarify intent. It’s diagnostic scaffolding; either drop it post-join in timestamp_fixer or leave it and document. Your test uses it, so either keep and document or drop and adjust tests.
5. Minor: /transactions error body shape

You return plain text on some failures; consider returning a JSON error body consistently (receipt-like) to simplify clients. Not blocking.
What’s already correct (and aligns with the plan)

PENDING transactions row created up front, then updated to ACCEPTED/REJECTED with the final receipt.
Advisory lock guard uses Drop to unlock asynchronously to avoid leaks on panic/early return.
include_in_pipeline respected for deployments; alias disambiguation enforced with fast-fail if ambiguous.
Parameter defs include canonical quality_* thresholds and provenance columns are emitted consistently (parameter_source_<code>).
Object store upload-first prior to raw_files inserts; idempotent local-dir backend is wired.
Next steps (actionable, in your requested style)

Calculator/quality stages

standard_v1_dst_fix: ensure the pipeline’s calculator + quality stages are exercised end-to-end.
Fix the pipeline test as above; add an end-to-end pipeline test that:
Creates two overlapping files (same logger_id/record values) with different file_hash, runs the full pipeline, and asserts:
Dedup by (logger_id, record) happened across files (row counts match unique (logger_id, record) pairs per thermistor pair).
file_set_signature is a sorted “hash_a+hash_b” string for the deduped rows (if you choose to keep it).
calculation_method_used populated deterministically from beta, sap_flux_density_j_dma_cm_hr present, and quality flags behave as expected for synthetic thresholds.
Receipt: add pipeline row_count, count of rows flagged SUSPECT, and maybe a tiny parameter_provenance sample (e.g., top 3 override sources by code) to aid triage. This is receipt-only wiring.
Tests: add a unit test for the TMAX path (valid tm > heat_pulse) and for invalid log term (no NaN propagation).
Real object storage

Add an R2/S3 client alongside LocalDir:
Config via env: SAPFLUX_OBJECT_STORE_KIND=(local|r2), R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ACCOUNT_ID, R2_BUCKET, R2_REGION (or endpoint), and SAPFLUX_OBJECT_STORE_DIR for local.
Keep upload-first semantics and idempotent puts.
GC sketch:
Raw files: list keys under raw-files/, fetch hash list from DB, delete unreferenced keys (dry-run mode first).
Outputs and cartridges will follow the same pattern later.
Error path: on upload failure, update the PENDING transaction to REJECTED with an explicit “object store upload failed” reason (you already do this; keep it).
Broaden integration coverage

/transactions tests:
Dry-run vs commit: Already present for one file. Add:
Receipt shape (parser_attempts present, first_error_line when applicable).
Commit inserts raw_files only on ACCEPTED, not on REJECTED.
Overlapping-file batch:
Post a transaction with two overlapping files; assert dedup end-to-end via the pipeline’s row_count or a dedicated field in the receipt (e.g., “unique_records_deduped”).
Alias integration:
Include a deployment with a time-ranged alias and a file that uses the alias; assert enrichment resolves to the canonical datalogger_id and matches the deployment.
Unit test: quality filter gap rule after applying the sorting fix.
Output pipeline

After calculations land:
Output parquet:
Write outputs/{output_id}.parquet to object storage (upload-first).
Insert outputs row in a single DB tx and flip is_latest on all prior rows to false (single global stream).
Reproducibility cartridge:
Package db_state_N-1.sql (via replay for now), transaction_N manifest, raw_files.manifest, and scripts (download_data.sh to call API for pre-signed URLs; run_repro.sh).
Write repro-cartridges/{output_id}.zip (upload-first), then update outputs row with reproducibility_cartridge_path.
API: add GET /outputs/{id}/download to return a pre-signed URL or 302 redirect; keep R2 private. Keep an include_cartridge=true option to return the zip instead of the parquet.
Tests:
Integration test that runs a commit, writes an output parquet, marks is_latest correctly, and returns a pre-signed URL (mock for local store).
Cartridge smoke test (behind env guard) that produces a small zip and verifies manifest contents.
Small cleanups (non-blocking, opportunistic)

Adopt pattern-based header validation in production parsers (tests can stay strict).
Document whether you keep file_set_signature in outputs; if diagnostic-only, drop before final parquet.
In ingestion receipts, include original file path, file_hash, and the per-parser attempt list (you already include attempts and first_error_line; ensure parser identity is accurate once parser wrappers are fixed).
Summary of code changes to queue

Fix quality gap rule: sort by (logger_id, record) before computing record gaps.
Fix tests/pipeline.rs to run the full pipeline (or scope it to timestamps only).
Fix parser wrappers in sapflux-core to call specific parsers, not the aggregator.
Add an S3/R2 object-store implementation (keep LocalDir), env-configurable.
Wire receipt enhancements (pipeline row_count, suspect counts).
Prepare output write path (parquet + cartridge) and API download endpoint with pre-signed URLs.
With those patches, you’ll have the calculator/quality outputs flowing through receipts, robust object storage behavior, accurate parser attempts, and end-to-end tests covering deduplication, dry-run semantics, and alias resolution—all ready to attach the output/cartridge stage.