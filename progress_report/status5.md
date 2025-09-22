# Sapflux Pipeline Progress Report (Update 5)

## Highlights Since Last Update
- Adjusted the CLI so it no longer performs direct ingestion or database access; instead it now spins up an Axum API server (`sapflux serve`) with `/health`, `/admin/migrate`, and `/admin/seed` endpoints backed by the shared Postgres pool.
- Incorporated `axum` and `tokio` server wiring (listener binding, structured logging) as the foundation for future transaction and ingestion endpoints.
- Confirmed the ingestion engine, metadata enrichment, parameter resolver, and other pipeline pieces continue to work (full `cargo test` run succeeds; the DB seed test still skips unless `SAPFLUX_TEST_DATABASE_URL` is set).

## Current State
- CLI, admin helpers, and future web GUI will all go through the API. The ingestion engine and pipeline stages are ready to be orchestrated from a `/transactions` handler.
- No direct pipeline run is exposed yet; the API skeleton is in place to add transaction and ingest routes next.

## Next Focus
1. Implement the transaction API (`POST /transactions`) to validate manifests, run the ingestion engine, and dispatch pipeline executions with persisted receipts.
2. Integrate object storage (MinIO/R2) handling for raw file uploads, outputs, and cartridges—initially via local dev stack (docker-compose) with `.env` configuration.
3. Finish the calculation/quality stages and add the run orchestrator so transactions produce stored outputs and reproducibility cartridges.
