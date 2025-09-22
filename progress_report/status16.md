# Sapflux Pipeline Progress Report (Update 16)

## Highlights Since Last Update
- Added a full docker-compose harness (Postgres + MinIO + API) and build Dockerfile so the entire pipeline can run locally without manual setup.
- Wrote `scripts/smoke.sh` to exercise the multipart manifest ingestion → pipeline publish → output download flow end-to-end, including seed/migrate and presigned parquet retrieval.
- Documented the workflow in `docs/dev-compose.md` covering startup, smoke test usage, and bucket layout.

## Next Focus
1. Expand integration coverage for manifests and multipart uploads (ambiguities, overlaps, large batches) plus GC dry-run/confirm exercises.
2. Harden deterministic output settings (row/column ordering, writer configuration) in preparation for reproducibility checks.
3. Continue toward the final “no-shortcuts local run” milestone using the compose harness.
