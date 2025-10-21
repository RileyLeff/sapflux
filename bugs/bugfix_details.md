bugfix_details.md
Purpose
This document is a focused, implementation-ready guide for fixing the six open issues listed in bugs/bugs.md. It is written for an LLM agent that has not seen the codebase before. It explains:

What this repository is and how it is organized
Each bug: symptoms, root cause, and the exact files/functions to change
The intended behavior (requirements)
A concrete implementation plan with precise edits
The tests that must be added or updated
Ordering/dependencies between fixes and likely collisions
Repository quick map (what matters for these fixes)

crates/sapflux-parser/ … Versioned parsers for TOA5 formats; produce the canonical in-memory dataformat (“sapflow_toa5_hierarchical_v1”)
src/formats/cr300_table.rs – CR300 “Table” parser (strict)
src/formats/sapflow_all.rs – SapFlowAll parser (strict)
src/formats/common.rs – parser primitives and builders (DataFrame assembly)
src/tests.rs – parser tests
crates/sapflux-core/ … Core logic and pipeline
src/flatten.rs – converts hierarchical parsed files into observation rows
src/timestamp_fixer.rs – “implied visit” DST-aware UTC correction
src/metadata_enricher.rs – joins deployments/aliases/hierarchy
src/parameter_resolver.rs – default/overrides cascade + provenance
src/calculator.rs – DMA Péclet calculation (HRM/Tmax)
src/quality_filters.rs – canonical quality flags
src/ingestion.rs – batch hashing, parsing, reports (duplicate handling)
src/transactions.rs – transaction orchestration, receipts, outputs
src/metadata_manifest.rs – manifest TOML parse, preflight, apply (adds)
tests/*.rs – unit/integration tests for core pieces
crates/sapflux/ … API binary (Axum)
src/main.rs – HTTP endpoints: /transactions, /admin/, /outputs/
Project invariants (important for this work)

One canonical internal dataformat per parser family: “sapflow_toa5_hierarchical_v1.”
Strict schema: parsers for different raw formats MUST emit identical schemas for the canonical dataformat. If they diverge, that is a bug to fix at the parser layer.
Measured-only policy: no logger-derived or calculated columns (e.g., total_sap_flow_lph, sap_flux_density_cmh, “vh”) should ever appear in the canonical parsed schema. All calculations happen in calculator.rs.
Flatten is strict; we do not “pad with nulls” to paper over parser differences. Parsers must conform.
Bug 1: API rejects metadata-only transactions
Symptom

POST /transactions with only a manifest (no files) returns 400 “transaction requires at least one file.”
Root cause

crates/sapflux/src/main.rs handle_transaction enforces “files required.” Core already accepts manifest-only:
sapflux-core/src/transactions.rs only rejects when files.is_empty() AND manifest.is_empty().
Intended behavior

Accept metadata-only transactions (manifest present, zero files). Pipeline should run if it can, otherwise return Skipped. Receipt outcome ACCEPTED.
Implementation plan

Edit: crates/sapflux/src/main.rs, function handle_transaction
Remove the early guard: if files.is_empty() { return (StatusCode::BAD_REQUEST, "transaction requires at least one file").into_response(); }
No other changes; core logic handles the rest.
Tests

New API test (sapflux crate): “metadata_only_transaction_accepts”
Multipart with only “manifest” field, no file parts.
Expect 200 OK, receipt.pipeline.status == "skipped", outcome ACCEPTED, metadata_summary present.
Ordering/collisions

Independent; can be done first.
Bug 2: Pipeline fails when files present but deployments not seeded yet
Symptom

Transaction with files but no deployments in DB returns “failed” (REJECTED). Users expect metadata to be accepted and pipeline to be Skipped.
Root cause

Standard pipeline errors when deployment/site metadata is absent:
crates/sapflux-core/src/pipelines.rs StandardPipelineStub::run_batch: returns Err(anyhow!("standard_v1_dst_fix requires deployment/site metadata"))
transactions.rs treats pipeline errors as Failed and REJECTS.
Intended behavior

If context lacks deployments or sites, pipeline should be Skipped (not an error). Transaction remains ACCEPTED (metadata applied, raw_files persisted). No output artifacts.
Implementation plan (Option A: skip in orchestrator)

Edit: crates/sapflux-core/src/transactions.rs, function run_pipeline(context, batch) (helper already exists)
Before selecting a pipeline, short-circuit when: if context.timestamp_sites.is_empty() || context.timestamp_deployments.is_empty() { return PipelineRun { summary: PipelineSummary { pipeline: None, status: PipelineStatus::Skipped, row_count: None, error: None, quality_summary: None, provenance_summary: None, record_summary: None, }, dataframe: None, }; }
Do not modify the pipeline implementation; keep it strict.
Tests

New core integration test (sapflux-core/tests or sapflux/tests):
Seed minimal DB (no deployments), ingest a file (valid), non-dry-run.
Expect: receipt.pipeline.status == "skipped"; receipt.outcome ACCEPTED; raw_files row inserted; no output artifacts.
Ordering/collisions

Independent of bug 5, but testing is simpler after bug 5 is fixed because schema is consistent.
Bug 3: Plant code uniqueness enforced globally instead of per plot
Symptom

Preflight rejects a plant code reused in different plots, although DB allows unique (plot_id, code).
Root cause

Preflight caches plants by code only:
metadata_manifest.rs: PreflightContext.plants: HashMap<String, PlantRecord>
Check uses plant code alone.
DB constraint is uq_plant_plot_code (plot_id, code).
Intended behavior

Preflight mirrors DB: plant uniqueness scoped to (plot_id, code).
Implementation plan

Edit: crates/sapflux-core/src/metadata_manifest.rs
Change PreflightContext.plants from HashMap<String, PlantRecord> to HashMap<(Uuid, String), PlantRecord>.
load_plants:
Current query: SELECT plant_id, code FROM plants
Change to: SELECT plant_id, plot_id, code FROM plants
Insert entries keyed by (plot_id, code).
In preflight for [[plants.add]]:
Resolve plot_id via context.lookup_plot(site_code, zone_name, plot_name)?.id
Check context.plants.contains_key(&(plot_id, entry.code.clone())).
On success, when staging “new” rows into context during preflight, call insert with (plot_id, code).
Update helper insert_plant(code, record) → insert_plant(plot_id, code, record).
Tests

Update/extend sapflux-core/tests/metadata_manifest.rs:
Two plants with the same code under different plots → preflight OK.
Two plants with same code under same plot → preflight REJECTED.
Ordering/collisions

Independent; aligns with your manual metadata clean-up.
Bug 4: Stem codes treated as globally unique
Symptom

Preflight rejects a stem code reused under different plants, although DB allows unique (plant_id, code).
Root cause

Preflight caches stems keyed only by stem code:
PreflightContext.stems: HashMap<String, StemRecord> (StemRecord currently stores plant_code string)
Check uses code alone.
DB constraint uq_stem_plant_code (plant_id, code).
Intended behavior

Preflight mirrors DB: stem uniqueness scoped to (plant_id, code).
Implementation plan

Edit: crates/sapflux-core/src/metadata_manifest.rs
Change PreflightContext.stems to HashMap<(Uuid, String), StemRecord>.
load_stems:
Current query joins plants to get plant_code; change to use st.plant_id (no need for plant_code).
SELECT st.stem_id, st.code AS stem_code, st.plant_id FROM stems st;
Insert keyed by (plant_id, stem_code).
In preflight for [[stems.add]]:
Resolve plant_id = context.lookup_plant(entry.plant_code)?.id
Check context.stems.contains_key(&(plant_id, entry.code.clone())).
Insert into cache under (plant_id, code).
Tests

Extend metadata manifest tests similar to Bug 3 but at stem scope.
Ordering/collisions

Independent.
Bug 5: Parsers emit raw total_sap_flow_lph and vh/sap_flux_density columns; schemas differ across formats
Symptom

Derived columns appear in the canonical format (e.g., total_sap_flow_lph from CR300 sensor_df; sap_flux_density_cmh from CR300 thermistor pairs). When stacking frames from different parsers, flatten errors with SchemaMismatch. Even apart from that, these columns violate the project’s “measured-only” rule.
Root cause (exact code)

CR300 parser maps:
"sapflwtot" → SensorMetric::TotalSapFlow -> canonical name "total_sap_flow_lph"
"vhouter"/"vhin" → ThermistorMetric::SapFluxDensity -> canonical name "sap_flux_density_cmh" … see crates/sapflux-parser/src/formats/cr300_table.rs classify_column
SensorFrameBuilder builds sensor_df (for SensorMetric) and pair dfs (for ThermistorMetric) as-is: … crates/sapflux-parser/src/formats/common.rs SensorFrameBuilder::build
Flatten copies every sensor_df column and every thermistor pair column into the observation frame (and requires identical column sets to stack): … crates/sapflux-core/src/flatten.rs
Strict intent (your decision)

Remove all derived/“calculated” columns at parse time. Never emit them in the canonical dataformat.
Keep a single, tight canonical measured schema across SapFlowAll and CR300 parsers:
Logger: timestamp, record, logger_id, battery_voltage_v?, panel_temperature_c? (both always present as nullable)
Thermistor pairs (per depth): alpha, beta, time_to_max_temp_downstream_s, time_to_max_temp_upstream_s, pre_pulse_temp_downstream_c, max_temp_rise_downstream_c, post_pulse_temp_downstream_c, pre_pulse_temp_upstream_c, max_temp_rise_upstream_c, post_pulse_temp_upstream_c
Flatten remains strict (no padding during flatten).
Implementation plan
A) Declare canonical measured schema (single source of truth)

New module: crates/sapflux-parser/src/formats/schema.rs
pub const LOGGER_COLUMNS: [&str; 5] = ["timestamp","record","battery_voltage_v","panel_temperature_c","logger_id"];
pub fn required_thermistor_metrics() -> &'static [ThermistorMetric] = &[ ThermistorMetric::Alpha, ThermistorMetric::Beta, ThermistorMetric::TimeToMaxDownstream, ThermistorMetric::TimeToMaxUpstream, ThermistorMetric::PrePulseTempDownstream, ThermistorMetric::MaxTempRiseDownstream, ThermistorMetric::PostPulseTempDownstream, ThermistorMetric::PrePulseTempUpstream, ThermistorMetric::MaxTempRiseUpstream, ThermistorMetric::PostPulseTempUpstream, ];
Helpers to add missing nullable Float64 columns to DataFrames and to order columns exactly as canonical.
B) Logger DataFrame (build_logger_dataframe) must always output the same five columns in fixed order

Edit: crates/sapflux-parser/src/formats/common.rs build_logger_dataframe
Build timestamp (cast to Datetime Microseconds, None) and record as now.
Ensure/insert battery_voltage_v column (Float64, nullable) if absent; same for panel_temperature_c; ensure logger_id column (already present or derived).
Emit columns strictly in this order: timestamp, record, battery_voltage_v, panel_temperature_c, logger_id.
C) Drop derived metrics during SensorFrameBuilder::build

Edit: crates/sapflux-parser/src/formats/common.rs SensorFrameBuilder::build
When assembling sensor_df (sensor-level metrics), skip SensorMetric::TotalSapFlow entirely (do not include the column at all; if no sensor metrics remain, set sensor_df = None).
When assembling thermistor pair dfs, skip ThermistorMetric::SapFluxDensity entirely.
After building each thermistor pair df, call a helper that:
Adds any missing canonical measured columns as nullable Float64 with correct length.
Reorders columns to match the canonical order.
Asserts there are no extra (non-canonical) columns; if extras appear, return ParserError::Validation("unexpected thermistor columns: …").
D) CR300 parser classification remains strict but harmless

Edit: crates/sapflux-parser/src/formats/cr300_table.rs
Keep recognizing "sapflwtot" and "vhouter/vhin" to produce helpful FormatMismatch/DataRow errors when headers are wrong.
The builder will exclude them, so they never appear in parsed output.
E) No flatten changes

With measured-only and identical schema at the parser layer, crates/sapflux-core/src/flatten.rs remains strict and will stack frames without mismatch.
Tests to add/update (very important)

Update crates/sapflux-parser/src/tests.rs:
CR300: previously asserted presence of total_sap_flow_lph and sap_flux_density_cmh. Replace with negative assertions.
sensor.sensor_df is None OR does not contain "total_sap_flow_lph"
thermistor pair df does NOT contain "sap_flux_density_cmh"
SapFlowAll: assert all canonical measured thermistor columns present; no derived columns present.
Cross-format parity: Parse a SapFlowAll fixture and a CR300 fixture, pick (address, depth) pair, assert identical thermistor column sets/order; logger df has exactly LOGGER_COLUMNS in that order.
Pipeline cross-format test (crates/sapflux-core/tests/pipeline.rs already mixes files):
Ensure it continues to pass and no SchemaMismatch appears from flatten.
Optional sentinel: if someone adds a new ThermistorMetric in code but forgets schema.rs, a unit test should fail, forcing explicit decision.
Ordering/collisions

Fix this FIRST (or very early) to stabilize schema across the codebase and tests. It will change parser tests and make downstream tests more deterministic.
Do NOT add any “pad with nulls” logic to flatten; strictness remains a feature, as requested.
Bug 6: Ingest dedup misses duplicates within the same transaction batch
Symptom

Submitting the same file twice in one request: both are marked Parsed instead of Duplicate; parsed rows are duplicated in the batch.
Root cause

ingestion.rs compares only against existing_hashes from DB. It does not track newly seen hashes within the current call.
Intended behavior

Treat a second occurrence of the same content hash within a single ingest_files call as Duplicate immediately.
Implementation plan

Edit: crates/sapflux-core/src/ingestion.rs, function ingest_files(inputs, existing_hashes)
Add let mut seen: HashSet<String> = existing_hashes.clone();
For each input:
Compute hash; if seen.contains(&hash) => push FileReport { status: Duplicate } and continue; do not parse.
If parsed successfully => seen.insert(hash.clone()); push Parsed; add to new_hashes.
No changes elsewhere.
Tests

Extend crates/sapflux-core/tests/ingestion.rs:
New test “ingestion_marks_duplicate_within_same_batch”:
Two FileInput with identical contents in one call; expect first Parsed, second Duplicate.
Ordering/collisions

Independent.
Test impact summary (what changes)

Parsers: remove derived columns, enforce canonical schema; update parser tests accordingly.
New tests for metadata-only acceptance; Skipped pipeline when no deployments; in-batch duplicates detection.
Existing pipeline/timestamp/enrichment/parameter/calculator tests should keep passing (canonical columns stay the same or get stricter).
Change checklist (copy/paste to a PR description)

API

sapflux/src/main.rs: handle_transaction – remove “at least one file” requirement.
Transactions orchestrator

sapflux-core/src/transactions.rs: in run_pipeline(context, batch), return Skipped when context.timestamp_sites or context.timestamp_deployments is empty.
Manifest preflight scoping

sapflux-core/src/metadata_manifest.rs:
PreflightContext.plants: key by (plot_id, code); load_plants SELECT plant_id, plot_id, code FROM plants; adjust checks & insert_plant.
PreflightContext.stems: key by (plant_id, code); load_stems SELECT stem_id, plant_id, code FROM stems; adjust checks & insert_stem.
Ingestion dedup (in-batch)

sapflux-core/src/ingestion.rs: maintain seen HashSet seeded with existing_hashes; return Duplicate for repeated hashes in one call.
Parser strict schema + measured-only

sapflux-parser/src/formats/schema.rs: new module with LOGGER_COLUMNS and required_thermistor_metrics, helpers to add missing nullable columns and reorder.
sapflux-parser/src/formats/common.rs:
build_logger_dataframe: always output timestamp, record, battery_voltage_v, panel_temperature_c, logger_id in that order (insert nullables if absent).
SensorFrameBuilder::build: skip SensorMetric::TotalSapFlow; skip ThermistorMetric::SapFluxDensity; ensure canonical thermistor columns present and ordered; assert no extras.
sapflux-parser/src/formats/cr300_table.rs: keep recognizing sapflwtot/vh but rely on builder to drop them.
Tests

sapflux-parser/src/tests.rs: update assertions to reflect derived-columns removal; add cross-format parity assertions.
sapflux-core/tests/ingestion.rs: add “duplicate in same batch” test.
sapflux-core/tests (API/integration): add “metadata-only transaction” and “no-deployments => Skipped” tests if not already covered.
Risk notes and mitigations

Parser test expectations will change significantly (CR300 no longer exposes derived columns). Update tests in the same PR.
Enforcing identical logger column order may reveal minor order differences in existing tests; fix by asserting against LOGGER_COLUMNS order.
The “skip when no deployments” path changes receipts in those scenarios from Failed to Skipped; update any integration test baselines.
Suggested implementation order

Bug 5 (parsers measured-only + canonical schema) – stabilize schema for all other work.
Bug 6 (in-batch duplicate) – small, safe; reduces wasted parsing.
Bug 1 (metadata-only acceptance) – trivial API fix; unblocks workflows.
Bug 2 (no deployments => Skipped) – orchestrator tweak; receipt change.
Bug 3 and 4 (preflight scoping for plants/stems) – match DB constraints.
Acceptance criteria (definition of done)

Parsers produce identical measured-only schemas across SapFlowAll and CR300:
No total_sap_flow_lph or sap_flux_density_cmh anywhere in parsed data or flatten outputs.
Logger df columns: timestamp, record, battery_voltage_v, panel_temperature_c, logger_id in that exact order.
Thermistor pair dfs contain the agreed measured set; CR300 fills absent metrics with nulls during parse; SapFlowAll fills all.
Flatten stacks mixed-format batches without SchemaMismatch; the pipeline passes all unit tests.
/transactions accepts manifest-only payloads; returns ACCEPTED with pipeline.status=Skipped when no files parsed.
Transactions with files but no deployments are ACCEPTED (pipeline Skipped); raw_files rows persist; no artifacts published.
Preflight enforces plant uniqueness on (plot_id, code) and stem uniqueness on (plant_id, code); tests cover both.
Ingestion treats duplicates within the same request as Duplicate; test covers this.
