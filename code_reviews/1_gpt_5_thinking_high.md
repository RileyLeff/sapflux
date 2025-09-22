Great progress. You’ve set up a clean workspace, captured the schema and constraints, built core pipeline components, and you already have good test coverage for critical pieces. You’re close to an end-to-end “first light.”

High-level verdict

Architecture matches the plan (batch pipeline, timestamp fixer inside the pipeline, PENDING transaction row approach, object-store semantics, constraints).
Code quality is solid; modules are cleanly separated and tested.
One critical bug to fix now (file_hash propagation), a few correctness gaps (alias resolution, richer enrichment), and some integration glue (parameter defaults from codebook/DB) will get you to an end-to-end flow quickly.
Priority issues and fixes

Critical: file_hash is not propagated into ParsedFileData
Symptom: sapflux-parser returns ParsedFileData with file_hash = "" by design; ingestion.rs computes blake3 but never writes it back into the parsed object. flatten_parsed_files reads file.file_hash to produce the file_hash column; right now that column will be "", breaking the file_set_signature grouping for timestamp correction.
Fix:
Extend your ParsedData trait to expose a mutable Any:
add fn as_any_mut(&mut self) -> &mut dyn Any;
In sapflux-core::parsers, implement as_any and as_any_mut for ExternalParsedFileData.
In ingestion.rs after computing hash and parsing, downcast_mut to sapflux_parser::ParsedFileData and set file_hash = hash.clone(); keep the separate hash string too for convenience.
Add a regression test: ingest a batch of two identical content files with different paths and verify the flattened frame’s file_hash column shows different hashes (and the timestamp fixer groups by file_set_signature correctly).
2. Metadata enrichment doesn’t handle logger aliases, and it omits several IDs

Current enrich_with_metadata matches by (logger_id, sdi12_address) but assumes logger_id equals dataloggers.code. It does not consider datalogger_aliases over time.
Fix (incremental path):
Option A (simple for now): ensure the ExecutionContext.enrichment_deployments rows already contain canonical datalogger_id (resolved server-side before running the pipeline) so enrichment can stay simple.
Option B (full): pass an alias map (alias string → time-ranged canonical datalogger_id) into enrichment and resolve before probing deployment_map.
Add missing hierarchy IDs to DeploymentRow and output columns if you plan to cascade on them: plant_id, plot_id, zone_id, species_id. Your parameter_resolver already looks for those columns; today they’ll be None and those override levels can never match.
3. Parameter defaults and source of truth

Your ParameterResolver takes ParameterDefinition { code, kind, default_value } and a list of overrides. Good.
You seed parameters table (code/description/unit) but not defaults. The plan keeps defaults in the codebook (notes/parameter_info.toml).
Decide the authoritative source:
Easiest: build ExecutionContext.parameter_definitions from an in-code dictionary (or a compiled TOML) and ignore defaults in DB. Keep DB only for overrides. That matches the plan and your current design.
Ensure you add definitions for quality_* thresholds too (to populate those columns by default).
4. Timestamp fixer output

You’re currently keeping file_set_signature in the final DataFrame after the join. In the plan we drop intermediate scaffolding. Consider dropping file_set_signature after computing timestamp_utc unless you want it for diagnostics (fine to keep for now, but decide).
The algorithm itself matches the spec (dedupe by (logger_id, record), group by (logger_id, file_set_signature), anchor on min(record), resolve TZ per chunk).
5. Parser unit/header validation rigidity

You acknowledged this in docs; current reference parsers still validate strictly by position/length. That’s okay short-term, but plan to switch to pattern-based validation to tolerate program variants.
6. Seeders for parsers/pipelines

seed.rs looks good: it inserts or updates descriptors idempotently, wiring by code_identifier to the correct data_format_id.
7. Tests and structure

Good coverage so far:
flatten: checks presence and row counts.
timestamp_fixer: basic conversion and DST ambiguity.
metadata_enricher: populates deployment_id and installation_metadata keys.
parameter_resolver: precedence and default fallback.
Add tests after fixing file_hash propagation: verify file_set_signature changes with different source files and timestamp correction groups by set signature.
Add a test for enrichment + alias (once implemented) or at least a unit that demonstrates alias path selection.
Smaller improvements and nits

pipelines::run_batch interface: You’re correctly downcasting &[&dyn ParsedData] to &[&ParsedFileData] and running flatten → timestamps → enrich → parameters. Perfect.
Observation context IDs: parameter_resolver currently expects UUIDs as strings in columns like site_id, species_id, plot_id, plant_id, stem_id, deployment_id. Align enrichment to populate those IDs so overrides can match all levels.
Error labelling: parameter_resolver provenance uses “default”, “stem_override”, etc. That matches the plan.
API server: basic /health and admin endpoints are great. Next step is the /transactions endpoint with the “PENDING row” lifecycle and advisory lock.
Suggested immediate patch (pseudo-diff)

trait extension
in sapflux-core/src/parsers.rs:
trait ParsedData { fn as_any(&self) -> &dyn Any; fn as_any_mut(&mut self) -> &mut dyn Any; }
impl ParsedData for ExternalParsedFileData { fn as_any(&self)-> &dyn Any { self } fn as_any_mut(&mut self)-> &mut dyn Any { self } }
ingestion.rs after parsing:
let mut data = parser.parse(content_str)?;
if let Some(pfd) = data.as_any_mut().downcast_mut::<sapflux_parser::ParsedFileData>() { pfd.file_hash = hash.clone(); }
push ParsedFile { hash: hash.clone(), data }
flatten tests
Assert that df.column("file_hash") values are hash strings you expect (from ingestion).
What’s next (short, staged)

Fix file_hash propagation (blocker).
Expand enrichment IDs and alias resolution (or pre-resolve canonical logger IDs in context).
Build parameter_definitions in ExecutionContext from your canonical codebook (include quality_*).
Wire /transactions with the PENDING→ACCEPTED/REJECTED lifecycle, advisory lock, and detailed receipts (you already have structures to generate parser_attempt reports; reuse them in the receipt).
Add object-store adapters after transaction wiring (upload-first + GC).
You’re in a strong position. Address the file_hash propagation now, then enrich/alias and parameter defaults, and you can land the first end-to-end run quickly.