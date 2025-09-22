Overall verdict: solid progress, foundations are now correct and in-sync with the plan. The three review fixes landed cleanly, the full pipeline now runs end-to-end in tests, and calculator/quality compile and behave as intended. You’re ready to move on to receipt enrichment and then storage/output work.

What’s improved (matches the review plan)

Parser wrappers: sapflux-core/src/parsers.rs now calls the specific format parsers (SapFlowAllParser, Cr300TableParser) instead of the multi-parser aggregator. Parser attempt identities in ingestion reports will now be accurate.
Quality gap rule: quality_filters computes record-gap violations by sorting per-logger indices by record, then writing flags back to original row indices. A regression test covers scrambled row order. Output row order is preserved.
Pipeline test: tests/pipeline.rs exercises standard_v1_dst_fix end-to-end, not just timestamp_fixer. It asserts:
deduplication via file_set_signature across overlapping files
non-null timestamp_utc
calculator and quality columns exist Good choice to build a minimal ExecutionContext inline.
Calculator/quality compile: Code is aligned with Polars 0.48 and tests pass.
Architecture/state check

Batch pipeline sequence is correct: flatten → timestamp_fixer → metadata_enricher → parameter_resolver → calculator → quality_filters.
Timestamp_fixer:
Dedupes by (logger_id, record).
Computes sorted file_set_signature and groups by (logger_id, signature) for offset determination.
Emits timezone-aware UTC in timestamp_utc.
Metadata enrichment: Canonical/alias resolution with fast-fail ambiguity checks; full hierarchy IDs; installation_metadata expansion.
Parameter resolver: Canonical definitions include quality_* thresholds; precedence and provenance implemented.
Transactions:
Advisory lock around execute_transaction.
PENDING row upfront; outcome and receipt updated at end.
Upload-first to object store before raw_files DB writes.
Dry-run returns receipt, no DB writes.
Tests: Good coverage for each stage plus integration for /transactions (behind DB env guard).
Minor nits and polish (optional but worthwhile)

Keep or drop file_set_signature column
It’s diagnostic scaffolding. Decide:
Keep it (document it as diagnostic), or
Drop it in timestamp_fixer before returning and adjust tests accordingly.
upload_new_raw_files dedupe set
You currently use HashSet<&String> (insert(&report.hash)). Because Hash and Eq for &String are content-based, this dedup works fine. If you want to make intent explicit, change to HashSet<String> and insert report.hash.clone() or HashSet<&str> with insert(report.hash.as_str()).
Parser validations
Reference parsers remain strict (positional header validation). Keep TODOs to relax to pattern-based checks in production to tolerate firmware variants (you’ve mentioned this in comments).
Consistency of timestamp_utc dtype in tests
You already cast to timezone-aware UTC where needed; keep this consistent across any new tests to avoid subtle failures.
Recommended next steps (aligned with status10)

Receipt diagnostics
Add row_count to the pipeline summary (already present).
Add a quality summary: counts of SUSPECT vs GOOD (null) and perhaps most common reasons (top-N split of quality_explanation).
Consider including a tiny provenance snapshot (e.g., sample of parameter_source_* override levels used) for triage—purely receipt-side.
Broaden test coverage
Calculator: Add targeted tests for TMax branch edge cases (tm <= heat_pulse, log-term invalid).
Pipeline: Add an integration test for two overlapping files in one transaction to assert dedup end-to-end via receipt row_count (or quality summary), complementing the unit pipeline_full test.
Enrichment: A quick test that ambiguity errors propagate into a REJECTED pipeline summary (e.g., two deployments match).
Object storage
Add an R2/S3 client alongside LocalDir with env config; keep upload-first flow.
Sketch a GC plan (list keys by prefix and remove ones not referenced by DB). Implement a dry-run mode first.
Outputs and cartridge (next phase after receipts)
Write final parquet to outputs/{uuid}.parquet (upload-first).
In a single DB tx: insert outputs row and flip is_latest on all others to false.
Add pre-signed download endpoint (return URL or 302).
Generate cartridge zip (db_state_N-1.sql, transaction_N, raw_files.manifest, download_data.sh calling API for pre-signed URLs, run_repro.sh).
End-to-end test: run → download with cartridge → validate bit-identical re-run (behind env guard).
Quick quality gate checklist (current code)

cargo test passes across workspace (DB-guarded tests may skip).
/transactions integration test behavior:
Dry-run returns Success, no DB writes (confirmed).
Commit ACCEPTED persists raw_files; outcome updated (confirmed).
Provenance columns present for parameters (confirmed via tests).
Timestamp_utc is timezone-aware UTC (confirmed).
Parser_attempts + first_error_line are present in receipts (ingestion reports include these).
Conclusion
You’ve closed the correctness gaps and stabilized the foundation. Proceed with:

enriching receipts (row/quality summaries),
then object-store R2/S3 client + GC stub,
then outputs + cartridge wiring.
This keeps you tightly aligned with the plan and builds on a now-solid pipeline core.