# Sapflux Pipeline Progress Report (Update 6)

## Highlights Since Last Update
- Implemented the full execution context loader so the `standard_v1_dst_fix` pipeline now hydrates site time zones, deployment metadata (including zone/plot/plant/species hierarchy), datalogger aliases, and parameter overrides directly from Postgres.
- Extended metadata enrichment to resolve logger aliases and emit the full hierarchy IDs required by the parameter cascade, with regression tests covering both canonical and alias lookups.
- Added canonical parameter definitions (including quality thresholds) and default loading so batch runs include calculator and quality thresholds even before overrides are applied.
- Built the first transaction orchestration flow: `/transactions` accepts base64-encoded files, computes hashes, runs ingestion, records per-file outcomes, executes the batch pipeline, and stores the receipt/outcome in the transactions table; raw file hashes persist only for successful runs.
- Propagated per-file ingestion reports into the receipt structure, enabling consumers to see duplicates, parse failures, and parser attempt details.

## Current State
- API server now supports `/transactions` (dry-run or commit) in addition to health and admin endpoints; receipts surface pipeline status (`skipped` / `success` / `failed`) and row counts when available.
- Batch pipeline runs against real metadata/override context and continues to pass all unit tests.
- Ingestion remains local-only (objects not uploaded yet) and calculation/quality stages are still stubs pending future work.

## Next Focus
1. Integrate object storage (upload raw files before DB commit, add GC plan for orphaned objects) so the transaction flow mirrors production semantics.
2. Flesh out calculation + quality filter stages inside `standard_v1_dst_fix` and surface their outputs/flags in receipts.
3. Enhance receipts with richer detail (e.g., parser attempt summaries, pipeline row counts) and begin shaping the reproducibility cartridge workflow.
