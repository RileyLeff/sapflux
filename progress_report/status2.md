# Sapflux Pipeline Progress Report (Update 2)

## Highlights Since Last Report
- Integrated the reference parser crate (`sapflux-parser`) into the workspace and wired its strict SapFlowAll/CR300 logic into the core registries.
- Built a reusable flattening module that converts hierarchical `ParsedFileData` batches into observation-level frames; added regression tests to ensure expected row counts.
- Implemented the timestamp fixer using the "implied visit" algorithm from the planning docs, including support for DST ambiguity and deployment/site metadata lookups.
- Upgraded the processing pipeline context so `standard_v1_dst_fix` now flattens parsed files, runs timestamp correction, and returns a UTC-backed DataFrame.
- Added unit tests for timestamp correction edge cases and ensured seeding logic remains idempotent.

## Current State
- `cargo test` passes across the workspace (the DB seeding test still skips unless `SAPFLUX_TEST_DATABASE_URL` is provided).
- Parsers, flattening, and timestamp fixing are wired into the pipeline; metadata enrichment, parameter resolution, and calculations remain stubs.
- CLI commands (`sapflux migrate`, `sapflux db-seed`, `sapflux-admin db-seed`) exercise shared migration/seed helpers.

## Next Focus
1. Implement metadata enrichment: join observations with deployment/site/stem hierarchy and expand installation metadata columns.
2. Layer the parameter resolver and quality threshold cascade over the enriched frame.
3. Start shaping the ingestion orchestrator (raw file ingest → parse → dedupe → store) and scaffold the Axum API endpoint.
