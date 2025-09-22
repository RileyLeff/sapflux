# Sapflux Pipeline Progress Report (Update 3)

## Highlights Since Last Update
- Added a metadata enrichment stage that joins the flattened observation frame with deployment/project/site/stem metadata and dynamically expands `installation_metadata` keys into dedicated columns.
- Updated the `standard_v1_dst_fix` pipeline to flatten parsed files, apply timestamp correction, and run the metadata enricher before handing off to upcoming parameter/calculation stages.
- Imported the `sapflux-parser` crate into the workspace so strict SapFlowAll / CR300 parsing is reused directly by the core pipeline.
- Captured the new logic with unit tests for flattening, timestamp fixing (including DST ambiguity), and metadata enrichment.
- Ensured `cargo test` passes across the workspace (the DB-seed test still skips unless `SAPFLUX_TEST_DATABASE_URL` is set).

## Current State
- Ingestion pipeline now produces observation-level data that is timestamp-corrected and metadata-enriched; parameter resolution and calculations remain TODO.
- CLI commands (`sapflux migrate`, `sapflux db-seed`, `sapflux-admin db-seed`) reuse shared helpers and are ready for API orchestration once implemented.

## Next Focus
1. Implement the parameter cascade and quality threshold resolution, including provenance columns.
2. Build the ingestion orchestrator (dedupe → parse → store) and transaction workflow around it.
3. Stand up the Axum API (`sapflux serve`) starting with `/health` and `/transactions` so CLI ↔ server loops can be exercised end-to-end.
