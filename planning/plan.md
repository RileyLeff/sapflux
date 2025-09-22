Suggested implementation plan (in order)

1. Database and migrations
Create all tables, enums, constraints (including -|- adjacency and && overlap exclusions).
Add transaction_outcome with PENDING (or status column) and supporting indexes you deem necessary later.
Verify FK graph and ON DELETE behavior.
Seed minimal reference data via sapflux-admin (data_formats, parsers, pipelines, parameters with defaults).
Acceptance checks:

psql: constraint violations for overlapping/adjacent deployments and aliases.
Seed idempotent; re-running sapflux-admin db-seed yields no diffs.
2. Core crates and scaffolding

Workspace: sapflux-core (domain + traits + registries), sapflux (API/CLI), sapflux-admin.
Implement ProcessingPipeline::run_batch(...) and registries for parsers/pipelines.
Add tracing (JSON logs) and structured error mapping (thiserror) end-to-end.
Acceptance checks:

Health endpoint returns 200; structured logs print request_id and user_id (when auth wired).
3. Ingestion engine

Hashing (blake3), dedup check, parser loop (active parsers from DB include_in_pipeline).
Idempotent object upload (raw-files/{hash}) before DB tx; periodic GC stub.
Transactions flow:
Insert transactions row outcome=PENDING (autocommit), hold its id.
Begin DB tx, apply metadata mutations; for each accepted file insert raw_files with that transaction_id.
Commit; update transactions row outcome + receipt. Dry-run: no DB writes; return receipt.
Acceptance checks:

Duplicate file yields “already known” in receipt, no new raw_files row.
Corrupt file shows parser_attempts, first_error_line, reason in receipt.
4. Batch processing pipeline (standard_v1_dst_fix)

Flatten: turn hierarchical parse into one observation per (timestamp, record, logger_id, sdi12_address, thermistor_depth).
Timestamp fixer (batch): combine all accepted ParsedFileData, dedup by (logger_id, record), build file_set_signature, choose anchor timestamp, resolve timezone via deployments->site, compute offsets, produce timestamp_utc.
Metadata enrichment: temporal join to deployments; enforce alias resolution and fail fast on ambiguity (should be prevented by constraints).
Parameter resolver: fetch all overrides once; coalesce in precedence; emit parameter_* and quality_* plus provenance columns as specified.
Calculator: DMA Péclet parallel outputs and switched sap_flux_density_j_dma_cm_hr.
Quality filters: apply rules and explanations.
Acceptance checks:

Synthetic fixture covers DST spring/fall transitions (Single/Ambiguous/None cases).
Duplicate records across files deduped correctly by file_set_signature groups.
5. Outputs, downloads, and reproducibility

Write final parquet; upload-first to outputs/{uuid}.parquet.
Insert outputs row; flip prior rows is_latest=false in same tx.
GET /outputs/{id}/download returns pre-signed URL or 302 redirect.
Cartridge generator: N-1 db_state, transaction_N.toml, raw_files.manifest, download_data.sh (calls API for pre-signed URLs using user token), docker-compose, run_repro.sh.
Acceptance checks:

End-to-end reproducibility test: run → download with cartridge → run_repro.sh → hash matches original.
Private R2 validated (objects not world-readable).
6. API/CLI and minimal GUI

Clerk auth (JWT verification) for API; CLI login flow; basic commands for transactions/apply, outputs/list, outputs/download --with-cartridge.
GUI can come later; ensure assets are fetched via pre-signed URLs or authenticated proxy.
Acceptance checks:

CLI can submit manifest with files, perform dry run, then commit, and download an output.
7. Operations and jobs

GC job: list bucket keys, delete unreferenced objects (raw-files, outputs, cartridges).
WAL-G to R2.
Caddy with Cloudflare DNS-01; Tailscale; Uptime-Kuma/Healthchecks.io.
Acceptance checks:

GC dry-run mode to report would-be deletions; then a safe delete run.
TLS and proxying confirmed with Cloudflare orange-cloud enabled.
Key risks to watch (add tests early)

Timestamp fixer edge cases (DST transitions, clocks reset, duplicate files).
Alias resolution ambiguity (ensure constraints prevent it; still test fail-fast path).
Transaction PENDING row life cycle (ensure update always happens; handle process crashes by marking PENDING rows with a terminal error on restart if needed).
Memory: use Polars lazy where practical during joins and coalescing.
If you adopt the staged plan above, I’d let the agent proceed. This gives you fast feedback after step 3 (ingestion + transactions) and again after step 4 (first end-to-end batch processing on small fixtures).