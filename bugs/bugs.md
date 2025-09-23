# Sapflux Bugs & Issues

## 2025-09-22 — Transactions require a file even for metadata-only manifests
**Context:** While running `integration_tests/rileydata/smoke.sh`, posting `meta_tx.toml` (metadata-only) to `/transactions` failed with the message `transaction requires at least one file`. The transaction layer should allow metadata-only submissions so operators can bootstrap the hierarchy without ingesting data.

**Impact:** Blocks metadata-first workflows (e.g., seeding sites/zones/plants) and conflicts with the manifest design described in code review 12.

**Suggested fix:** Relax the guard in `execute_transaction_locked` (sapflux-core/src/transactions.rs) to permit empty file lists when the manifest includes entries.

**Status:** Reproduced; workaround is to include a placeholder file payload.

## 2025-09-23 — Pipeline fails when files are provided but no deployments exist yet
**Context:** Posting the real `meta_tx.toml` (metadata only) with a placeholder CR300 file returns HTTP 200 with `status: failed`. The receipt shows `standard_v1_dst_fix requires deployment/site metadata`. In this scenario the metadata insertions succeed, but because the file is present the ingestion pipeline runs immediately and aborts due to missing deployments.

**Impact:** Blocks incremental onboarding where metadata is staged via manifest and raw data is uploaded in the same transaction before deployments have been defined. Operators receive a failure even though metadata should have been accepted.

**Suggested fix:** Allow the pipeline step to short-circuit (or tolerate missing deployments) when metadata insertions succeed but required metadata for processing is absent, so the transaction can still return `ACCEPTED` with metadata-only progress.

**Status:** Reproduced; workaround is to upload metadata in a transaction without files (currently blocked by the issue above) or stage deployments before uploading data.

## 2025-09-23 — Plant code uniqueness enforced globally instead of per plot
**Context:** `meta_tx.toml` contains plants `5491` and `5495`, each under different plots. Submitting the manifest after a `docker compose down -v` still fails with `plant '5491' already exists`. The preflight duplicate detection is treating plant codes as globally unique instead of scoped to `(plot_id, code)` per the schema (`uq_plant_plot_code`).

**Impact:** Prevents loading real data where the same plant identifiers are reused across plots/sites.

**Suggested fix:** Adjust preflight to key uniqueness checks by `(plot_key, code)` rather than `code` alone.

**Status:** Reproduced.

## 2025-09-23 — Stem codes treated as globally unique
**Context:** Multiple plants have a child stem named `Stem1`. The manifest submission fails with duplicate errors even though schema-level constraint is `uq_stem_plant_code` (unique within plant). Preflight currently stores stems in a global map keyed by code, so it rejects legitimate duplicates across plants.

**Impact:** Blocks ingesting real data where stems share human-readable names within different plants.

**Suggested fix:** Scope the stem uniqueness check to `(plant_code, stem_code)` instead of a single string key.

**Status:** Reproduced.

## 2025-09-23 — Parsers emit raw `total_sap_flow_lph` column
**Context:** The CR300 table parser maps the `sapflwtot` column to `total_sap_flow_lph`, so the ingestion pipeline sees a “computed” total sap flow before any downstream calculations. When mixed with files that omit the column (or emit the raw thermistor series), this breaks the canonical schema and causes the pipeline to fail. More pressingly, this column contains incorrect information that conflicts with the correct information added later in the pipeline.

**Impact:** Keeps legacy “total sap flow” values alive and prevents the pipeline from stacking frames with and without that column.

**Suggested fix:** Drop `sapflwtot` during parsing (or emit it under a `raw_…` name) so canonical logger-level columns stay limited to timestamp, record, battery_voltage, optional panel temperature, and logger ID. Sensor/thermistor values should only live under their per-sensor/per-depth columns. Perform a more thorough investigation into the parsers and how they convert data into a dataformat. This should be reliable.

**Status:** Reproduced.

## 2025-09-23 — Ingest deduplication skips duplicates within the same transaction
**Context:** `ingest_files` only checks hashes against those already in the database (`existing_hashes`). New hashes collected within a single batch aren’t compared against each other, so submitting the same file twice in the same transaction marks both as “Parsed”.

**Impact:** Allows duplicate uploads in the same request; downstream, the pipeline sees the duplicate rows twice.

**Suggested fix:** Track hashes seen within `ingest_files` (seeded with `existing_hashes`) so duplicates in the same transaction return `FileStatus::Duplicate` immediately.

**Status:** Reproduced.
