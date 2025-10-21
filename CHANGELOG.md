# Changelog

## 2025-10-21

### Added
- New `formats::schema` module declaring the canonical SapFlow logger and thermistor schemas shared by all parsers.
- Parser tests that assert cross-format schema parity and verify derived columns are excluded from parsed output.
- Ingestion unit test that exercises duplicate detection within a single upload batch.
- Axum integration test covering metadata-only transactions (manifest without files) using multipart uploads.
- Runtime integration test ensuring transactions with missing deployments are accepted but pipeline execution is skipped with an explanatory receipt.
- Additional metadata manifest tests validating plant/stem uniqueness scopes and parameter override requirements.
- Timestamp fixer unit test that confirms chunks without deployments are skipped (and reported) rather than failing the pipeline.
- `PipelineBatchOutput` structure so pipeline implementations can return both processed dataframes and per-chunk skip metadata.
- `PipelineSummary.skipped_chunks` field in transaction receipts to surface skipped logger chunks back to API consumers.

### Changed
- SapFlow parsers now normalise logger column ordering, insert nullable logger columns when absent, and drop derived metrics such as `total_sap_flow_lph` / `sap_flux_density_cmh` to enforce a single measured-only schema.
- `SensorFrameBuilder` pads thermistor pair dataframes to the canonical column set, rejects unknown metrics, and omits derived density values.
- Parser binaries now depend on `tower`, `hyper`, and `http-body-util` (dev) to support the new API integration test utilities.
- CR300/SapFlowAll parser fixtures updated to match the new schema expectations.
- Ingestion batches seed an in-call hash set so identical files within a single request are immediately marked `Duplicate`.
- Transaction orchestrator no longer requires files when a manifest is provided; manifest-only transactions succeed and report a skipped pipeline.
- Transaction pipeline helper short-circuits to `PipelineStatus::Skipped` when timestamp site/deployment context is entirely missing.
- Manifest preflight loads plants keyed by `(plot_id, code)` and stems keyed by `(plant_id, code)`, enforcing uniqueness per scope during both preflight and apply.
- Timestamp fixer now filters out logger/file-set chunks lacking deployment coverage, collects structured skip reasons, and guarantees the returned dataframe has non-null `timestamp_utc` values.
- Standard pipeline consumes the revised timestamp fixer output and propagates skipped chunk metadata for reporting.
- Transaction receipts treat timestamp fixer misses as informational: the pipeline status remains `success` (or `skipped` when everything is filtered) and `skipped_chunks` describe any omitted data.
- Pipeline/unit tests updated to dereference `PipelineBatchOutput` instead of raw dataframes.

### Fixed
- Posting metadata manifests followed by SapFlow files now succeeds even when certain deployments are absent; affected rows are skipped but ingress succeeds.
- SapFlow parser outputs from different logger formats now align, preventing downstream flattening/stacking mismatches caused by derived columns or inconsistent schema ordering.
- Multiple identical files uploaded in a single transaction no longer double-count parsed rows.
- API endpoint `/transactions` no longer rejects manifest-only submissions, allowing metadata backfills without accompanying files.
- Metadata manifest ingestion now respects plant/stem uniqueness within their parent scopes and surfaces meaningful preflight errors when duplicates are provided.
