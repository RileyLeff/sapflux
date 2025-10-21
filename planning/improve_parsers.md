# Sapflux Parser Expansion Plan

_Target audience: a fresh LLM agent entering the Sapflux codebase._

## Repository snapshot

- Workspace root: `sapflux/`
  - `crates/sapflux-parser/` — Rust library that recognises raw logger formats and normalises them to the canonical in-memory schema (`sapflow_toa5_hierarchical_v1`).
  - `crates/sapflux-core/` — Core pipeline logic; assumes every parser produces the canonical schema.
  - `integration_tests/` — Smoke-test fixtures containing real logger exports (`rileydata/rawdata/**`).
  - `scripts/failed_data_checker.py` — Helper that summarises transaction receipts (useful for spot checking failed inputs).

## Current parser architecture

`sapflux-parser` exposes `parse_sapflow_file()` which tries each registered parser in order until one succeeds.

### Existing parsers

| Module | Files | Characteristics |
|--------|-------|-----------------|
| `formats/sapflow_all.rs` | `SapFlowAllParser` | Modern two-sensor SapFlow program (`S0_AlphaOut`, etc.). |
| `formats/cr300_table.rs` | `Cr300TableParser` | CR300 `Table2` outputs with `SdiAddress`, `vhouter`, etc. |

Both parsers feed measured columns into `SensorFrameBuilder` (defined in `formats/common.rs`), *skip* derived totals such as `SapFlwTot` and `Vh*`, and use `build_logger_dataframe` to emit a canonical logger dataframe with the columns declared in `formats/schema.rs`.

### Canonical schema rules

- Logger dataframe columns (in order): `timestamp`, `record`, `battery_voltage_v`, `panel_temperature_c`, `logger_id`.
- Sensor thermistor frames present only measured quantities: alpha/beta, temperature maxima, pre/post pulse temps. No velocity totals, no density estimates.
- Column naming convention: `S[Address]_[Metric]` (SapFlowAll) or derived from recognised CR300 identifiers; downstream flatten code expects these to match exactly.

## Gaps discovered

Transaction receipts show 1,295 files failing parsing. Two distinct ToA5 formats are not yet recognised:

1. **CR200 “Table” exports** (prefix `CR200Series_...`). Example header from `integration_tests/rileydata/rawdata/2024_08_29/CR200Series_304_H7_891.csv`:

   ```csv
   "TOA5","CR200Series_304","CR2xx",...,"Table2"
   "TIMESTAMP","RECORD","BattV_Min","id","SDI1","SapFlwTot1","VhOut1","VhIn1","AlphaOut1","AlphaIn1","BetaOut1","BetaIn1","tMaxTout1","tMaxTin1"
   "TS","RN","Volts","","","unit","unit","unit","unit","unit","unit","unit","unit","unit"
   "","","Min","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp"
   "2024-08-16 12:30:00",5769,13.22452,304,1,0.181,3.61,0.95,0.31976,0.08388,0.27951,-0.08197,58.096,41.434
   ```

   Notes:
   - `SapFlwTot1`, `VhOut1`, `VhIn1` are derived quantities (drop).
   - Measured columns use `Out/In` suffixes rather than the SapFlowAll `Outer/Inner` naming.
   - The SDI address appears as the trailing digit (`1`), so we can derive `Sdi12Address('1')`.

2. **Legacy CR300 outputs** (prefix `CR300Series_...`, but columns like `SapFlwTot0`, `VhOuter0`, etc.). Example from `integration_tests/rileydata/rawdata/2024_07_03/CR300Series_502_L2_5491.csv`:

   ```csv
   "TIMESTAMP","RECORD","BattV_Min","id","SDI0","SapFlwTot0","VhOuter0","VhInner0","AlphaOut0","AlphaIn0","BetaOut0","BetaIn0","tMaxTout0","tMaxTin0"
   ```

   These are essentially the same metrics as the modern CR300 Table parser expects, just with `Outer/Inner`, `Out/In`, and index suffixes rather than the SapFlowAll `S0_...` scheme.

## Goal

Extend `sapflux-parser` to recognise these legacy formats and emit the same canonical schema so the rest of the pipeline can ingest them without changes.

## Implementation sketch

### 1. CR200 parser (`formats/cr200_table.rs`)

1. Create a new module alongside the existing CR300/SapFlowAll modules. Mirror the structure of `cr300_table.rs` (helpers to classify columns, parse metadata, iterate rows) but with CR200-specific column patterns.
2. Column classification rules:
   - `TIMESTAMP`, `RECORD`, `BattV_Min`, `id`, `SDI*` — map to logger columns. Derive `logger_id` via `id` column if no explicit column is provided.
   - SDI column (`SDI1`, etc.) provides the address; use it to enforce expectations (`address == column suffix`).
   - Derived columns to drop: `SapFlwTot*`, `VhOut*`, `VhIn*`, `SapFluxDensity*` if present.
   - Measured columns to map:
     - `AlphaOut*` → `Alpha` (depth = Outer)
     - `AlphaIn*` → Inner
     - `BetaOut*`, `BetaIn*`
     - `tMaxTout*`, `tMaxTin*`
   - Additional metrics may appear (e.g., temperature rise `dT` columns); inspect the sample files and map all measured thermistor metrics supported by the canonical schema.
3. Reuse `SensorFrameBuilder` and `build_logger_dataframe` to produce the canonical dataframes.
4. Handle metadata header (`TOA5` row) similarly to other parsers (`parse_metadata`). If CR200 uses a different header structure (some `SapFlow` programs skip fields), add tolerant parsing with defaults.

### 2. Legacy CR300 support

Option A: extend `cr300_table.rs` column classifier to recognise both naming styles.

Steps:
1. Enhance `Cr300TableParser::classify_column`:
   - Detect patterns `SapFlwTot[0-9]+` and skip.
   - Map `VhOuter{idx}` / `VhInner{idx}` to `ThermistorMetric::SapFluxDensity` and skip (derived).
   - Map `AlphaOut{idx}`, `AlphaIn{idx}`, etc. to canonical metrics.
   - For modern columns (`SdiAddress`, `vhinner`, etc.) keep existing behaviour.
2. Ensure the `id` column is accepted (observed in these files) and used to enforce logger consistency.
3. Unit tests: add fixtures extracted from `integration_tests/rileydata` (e.g., `CR300Series_502_L2_5491.csv`) to `crates/sapflux-parser/tests/data/` and assert the parsed dataframes match those produced by the existing SapFlowAll/CR300 fixtures (column sets, order, absence of derived metrics).

Option B: add a dedicated `cr300_legacy.rs` module. This may keep the code simpler if the classification logic becomes complex. The module could share utilities with the main CR300 parser via helper functions in `formats/common.rs`.

### 3. Parser registry wiring

- Update `crates/sapflux-parser/src/formats/mod.rs` to expose the new CR200 (and optional CR300 legacy) parser modules.
- Update `crates/sapflux-parser/src/registry.rs` to include the new parser(s) in the `parsers` array tried by `parse_sapflow_file`.
- Mirror the additions in `crates/sapflux-core/src/parsers.rs` so the core layer can call them directly.

### 4. Tests and validation

1. **Parser unit tests** (`crates/sapflux-parser/src/tests.rs`):
   - Add new fixtures for CR200 and legacy CR300.
   - Assert derived columns (vh, sapflwtot) are absent.
   - Check that thermistor columns match the canonical set (use `LOGGER_COLUMNS` and `required_thermistor_metrics()` helpers).

2. **Smoketest receipt**: rerun `uv run scripts/failed_data_checker.py` to confirm the “other” bucket drops to zero and the new parsers’ outputs land in `accepted`. Even if it doesn't drop to zero, you should stop to report your progress to the user, so the user can inspect the changes.

3. **Clippy/tests**: run `cargo clippy` and `cargo test` to ensure build hygiene (existing workflows already guard this).

## Key files to study

- `crates/sapflux-parser/src/formats/common.rs` — shared utilities (`SensorFrameBuilder`, logger dataframe helpers).
- `crates/sapflux-parser/src/formats/cr300_table.rs` — reference implementation of a strict parser; adapt its structure for CR200/legacy CR300.
- `crates/sapflux-parser/src/formats/schema.rs` — canonical column declarations.
- `crates/sapflux-core/src/flatten.rs` & `timestamp_fixer.rs` — downstream consumers that assume the canonical schema (helpful to understand why certain columns must be present/absent).
- `scripts/failed_data_checker.py` — use to inspect receipts during development.
- Sample raw files under `integration_tests/rileydata/rawdata/*` — real-world inputs for parser testing.

With these additions, the parsing layer should cover all SapFlow ToA5 variants present in the repository, leaving only genuinely malformed files in the “other” bucket. Notes:

- CR200 and legacy CR300 files in the repository contain only the core thermistor metrics (no additional `dT` columns). Continue to blacklist derived totals (`SapFlwTot*`, `Vh*`) and forward measured thermistor values.
- Both formats always expose an `id` column; treat its absence as a hard error instead of falling back to metadata heuristics.
- Leave `oldfile*` duplicates untouched—transaction-level and database deduplicators already handle repeated hashes.
