# Sapflux Pipeline Progress Report (Update 10)

## Highlights Since Last Update
- Repaired the pipeline regression test to execute `standard_v1_dst_fix` end-to-end, ensuring calculation and quality columns are asserted on actual pipeline output.
- Corrected the sapflow parser wrappers to call the format-specific parsers so parser attempt tracking and identities now reflect real behavior.
- Sorted per-logger records inside the quality filters before computing gap violations, eliminating spurious `record_gap` flags and adding a regression test for the edge case.
- Brought the calculator/quality modules and tests in line with the latest Polars APIs, clearing the outstanding compilation errors and restoring a clean `cargo check`.

## Next Focus
1. Extend receipt generation with pipeline row counts and quality summaries now that calculation/quality stages are solid.
2. Add DMA/Tmax coverage in unit tests and broaden pipeline integration cases (overlapping files, provenance checks).
3. Begin wiring the R2/S3 object store client and associated upload-first GC scaffolding once the above diagnostics land.
4. Plan the output parquet + reproducibility cartridge plumbing so it can follow immediately after the storage work.
