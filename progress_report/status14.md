# Sapflux Pipeline Progress Report (Update 14)

## Highlights Since Last Update
- Successful transactions now publish their pipeline dataframe to Parquet, generate a reproducibility cartridge, upload both artifacts, and record the run/output rows (including `is_latest` flipping) before committing.
- Transaction receipts include artifact metadata so clients know the parquet and cartridge keys (and the download endpoint can presign them on demand).
- Added a `GET /outputs/{id}/download` API that returns a presigned URL for either the parquet or cartridge, reusing the object-store abstraction.
- Introduced an optional MinIO/R2 smoke test verifying upload, list, presign, and delete behaviour for the S3 backend.

## Next Focus
1. Add a docker-compose harness (Postgres + MinIO + API) plus smoke script to exercise the full upload/publish/download loop locally.
2. Wire artifact references directly into receipts (optionally short-lived presigned URLs) and enhance integration tests accordingly.
3. Harden GC tooling against real uploads—dry-run first, then confirm mode—so orphaned outputs/cartridges/rows can be audited.
4. Prepare for metadata manifest + multipart ingestion work (manifest schema, preflight validation, streaming upload-first).
