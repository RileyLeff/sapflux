Great work. You’ve turned the plan into a coherent, test‑backed workspace with the core pipeline mechanics in place. Most of the critical architectural points from the design are now present in code, and your recent changes closed the biggest early gap (propagating file_hash into ParsedFileData). Below is a focused review with concrete, actionable items.

What’s working well

Batch pipeline architecture:
ProcessingPipeline::run_batch and the orchestrator feed the full parsed set into flatten → timestamp_fixer → metadata_enricher → parameter_resolver. This matches the “implied visit” design and enables cross‑file deduplication.
Timestamp fixing:
correct_timestamps implements the set‑signature algorithm: dedupe by (logger_id, record), compute sorted file_set_signature, anchor at min(record), compute per-chunk UTC offsets via site TZs. Good test coverage, including DST ambiguity.
Ingestion + file_hash propagation:
You compute blake3, parse with the registry, downcast_mut to sapflux_parser::ParsedFileData and set file_hash. The new test verifies it. This unblocks timestamp chunking.
Metadata enrichment:
Enrichment resolves deployments via (logger_id, sdi12) with time ranges and resolves alias→canonical logger_id when needed. It emits the full hierarchy (deployment_id, datalogger_id, project_id, site_id, zone_id, plot_id, plant_id, species_id, stem_id) and expands installation_metadata keys into columns. Unit tests cover both canonical and alias cases. Good.
Parameter cascade:
canonical_parameter_definitions with typed JSON defaults (including quality_*) + precedence logic + provenance columns. Tests cover precedence, default fallback, and presence of quality defs.
Transactions and receipts:
/transactions endpoint, transaction row inserted as PENDING, ingestion reports serialized, batch pipeline executed, receipt persisted with outcome. Good flow.
Object-store abstraction:
Upload-first pattern before raw_files DB writes, idempotent local store, and batch dedupe by hash. Good start on mirroring the “upload-first + GC” design.
Important improvements (do these next)

Add serialized execution (advisory lock)
The plan requires exactly one transaction at a time to avoid races (e.g., concurrent raw_files inserts, overlapping metadata changes).
Add a Postgres advisory lock around execute_transaction:
Acquire at start (e.g., pg_try_advisory_lock(hash_of(“sapflux_tx_queue”)) or a constant) and release on scope exit.
If lock acquisition fails, either 409/423 the request or block until available.
This change is small, high impact, and prevents concurrency edge cases from day one.
2. Object store configuration and behavior

Use ObjectStore::from_env() in sapflux main (currently hardcoded to noop). Fail fast with a clear log if misconfigured in prod; use noop for tests.
Keep uploads where you have them (before DB persistence) but also consider:
Upload even when the pipeline fails (optional): this can reduce user retries on flake failures. The GC can clean orphans. Not required; your current gate (only upload on accepted) is acceptable too.
Add a TODO or stub for GC listing and delete-by-prefix (raw-files/) for keys not referenced in DB.
3. Receipts: surface more signal for triage

You already collect parser_attempts; also include:
first_error_line (line_index from ParserError::DataRow).
parser name and “format mismatch” vs “data row” distinctions (you have this via error string, but pulling line_index into a field is nicer).
Include a summary: files_processed/accepted/rejected; you already have reports and pipeline row_count.
4. Enrichment disambiguation contract

The DB constraints should prevent alias overlaps and deployment overlaps, but document and enforce a fail‑fast path in code if you ever encounter ambiguous matches (same alias mapped to multiple canonical codes, or multiple deployments matching a timestamp). Today, you choose canonical first then alias; add a guard that if both match differently, return an error (or at minimum log and leave null) so it’s obvious during debugging.
5. Parameter resolver: align column expectations and provenance naming

You already emit parameter_source_<code> for all parameters, which is correct. Ensure your calculator will look for:
calculation params with “parameter_*”
quality thresholds with their own names (quality_) and provenance in parameter_source_quality_ if you follow the plan. You can keep the current uniform provenance naming and map in the calculator—just be explicit in docs/comments and tests.
6. Pipeline test coverage

Add a small end-to-end batch test:
Flatten + timestamp_fixer + enrichment + parameter_resolver on two files whose records overlap (to exercise set signatures). Assert deduplication by (logger_id, record) and that all rows get timestamp_utc populated; check that group counts match expected unique (logger_id, record) pairs.
Add a transactions integration test (feature = "runtime"):
Bring up a test DB (or use env var guard), POST /transactions with two small files base64, dry_run=true and false. Assert HTTP 200, receipt content (file statuses, pipeline status), and DB effects (transactions row, raw_files rows only on accepted).
7. Production parser header validation

Your reference parsers are still positionally strict for units/characteristics; keep the tests but add a TODO and code comments that production parsers should validate per‑column pattern families (so variant programs don’t get rejected).
Smaller nits and polish

ExecutionContext::load_from_db:
Good joins; you filter deployments by include_in_pipeline = TRUE. Keep it.
Consider left joins for optional hierarchy only if your schema may have missing relationships later (currently it’s fine with current constraints).
timestamp_fixer: consider dropping file_set_signature from the final frame after join (it’s only needed as scaffolding) or keep it deliberately for diagnostics and document it.
sapflux main:
Return JSON error body in /transactions on failure (today it returns plain text)—optional but nicer for clients.
You can add /health detail (db connectivity, eh) later.
Transactions: future step
Once outputs and cartridges are in, adopt the same upload‑first pattern for outputs/cartridges, and flip is_latest in a single DB transaction at insert time.
What you’ve nailed

The “implied visit” algorithm and the batch pipeline boundary are correct and in code.
file_hash propagation is now correct and tested.
Alias handling is implemented in enrichment with tests.
Parameter defaults and provenance are in place.
The transaction row lifecycle (PENDING→ACCEPTED/REJECTED) and dry-run behavior match the plan.
The object store abstraction is wired into the flow in the right place.
Proposed near-term task list

Add advisory lock to execute_transaction.
Use ObjectStore::from_env in sapflux main; keep noop default for dev/tests.
Enhance receipts with first_error_line and summary counts.
Add ambiguity guards to enrichment.
Add batch pipeline integration test and transactions endpoint integration test.
Stub calculator + quality filters next and wire them into the standard pipeline.
Overall: you’re on the right track, with the core, tricky parts already correct. Address the lock, receipts, and small integration details above, then move on to calculations/quality and object store real client (R2/S3).