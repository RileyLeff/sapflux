# Sapflux Pipeline Progress Report (Update 13)

## Highlights Since Last Update
- Integrated the S3/R2 object store path into the transaction flow: successful runs now upload both raw files and final artifacts before committing database state.
- Added an `outputs` publishing module that serializes the pipeline dataframe to Parquet, builds a reproducibility cartridge, uploads both artifacts, and records them in `runs`/`outputs` (toggling `is_latest`).
- Extended the `ObjectStore` abstraction with generic put/presign/list/delete helpers and dedicated key builders for parquet outputs and cartridges.
- Dropped a gated MinIO/R2 smoke test that exercises the new backend end-to-end (upload, list, presign, delete) when credentials are provided.

## Next Focus
1. Broaden MinIO/R2 verification with a scripted harness (docker-compose) to simplify local testing.
2. Use the newly published outputs in receipts (e.g., include artifact keys or presign URLs on ACCEPTED transactions).
3. Plan downstream wiring for parquet download endpoints and cartridge retrieval using the new object-store helpers.
4. Harden the GC tooling by exercising it against real uploads and ensuring outputs/cartridges remain referenced.
