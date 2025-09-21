Verdict
You have an excellent, coherent design. I would green‑light it after you address the items below. I’ve focused on contradictions between documents and reference code, places where semantics are underspecified, and a few operational edge cases.

## Blocking to finalize (must clarify or change before implementation freeze)

### Transaction atomicity vs. partial file acceptance

In 03_transaction_workflow.md you promise atomic transactions, yet you also accept transactions with “partial file success” (some files rejected, others ingested). Please make this explicit:
Define outcome semantics: manifest/metadata validation errors → REJECTED; file-parse failures → ACCEPTED with PARTIAL_SUCCESS (files_rejected listed).
State that file-parse failures do not invalidate the whole transaction.
Persistence order: you say “if REJECTED we ROLLBACK except for inserting the transactions row.” That’s not possible inside a single DB transaction. Specify the mechanism:
Pattern A: validate first (no DB tx); then open a DB tx to apply changes; finally insert the transactions row in a separate autocommit step.
Pattern B: wrap only the mutating changes in a DB tx; write the transactions row outside that tx.
Document the exact steps for dry_run=true as well (no DB mutations; still insert a transaction record with outcome=REJECTED and a receipt).

#### RILEY RESPONSE
So my vision for this is that we attempt to validate everything in the transaction before we try adding the data. If the transaction puts the metadata into a permissible state, we let it run, even if all the data is rejected. Basically we want to ensure that the output from a given transaction produces an output "updated" with all of the the new stuff from the transaction, as long as the transaction is valid. I agree that there has to be a "check it out and validate" before actually running the thing, but my point is that the validation has to happen on metadata first since the data can be rejected or partially rejected and the transaction can still be valid. Plus we want any accepted data to run on the updated database state.

### Parser/file format contract mismatches (code vs. docs)

file_hash required but missing in code:
notes/file_hash_storage.md and timestamp_fixer.md require ParsedFileData.file_hash.
reference_implementations/sapflux-parser/src/model.rs ParsedFileData lacks file_hash, and both parsers build ParsedFileData without it.
Action: add file_hash: String to the struct, and ensure the ingestion engine (not the parser) assigns it before downstream use. Update archive (zip) serializer if needed.
logger_id standardization not implemented for SapFlowAll:
notes/logger_id.md requires deriving logger_id from the header when no id column exists.
SapFlowAllParser never derives/backs a constant logger_id column from FileMetadata.logger_name, nor enforces “single ID per file.”
Action: implement header-based extraction (e.g., split CR300Series_420 → "420"), insert a constant logger_id column in the logger-level DataFrame, and add a validation that all per-row id values (if present) are identical.
record monotonicity/continuity checks missing:
README note calls for rejecting files with non-sequential record numbers. The current parsers do not enforce this. Decide and implement: strictly +1 per row, or strictly increasing (no gaps) vs increasing (gaps allowed). Document it.

#### RILEY RESPONSE:
I agree, sounds good. Regarding record number, yeah, strictly +1 per row. Implementing agent, take careful note of these changes to the file hash storage, logger id, and parser steps.

### Timestamp chunking algorithm inconsistency

The prose says a chunk (an “implied visit”) is defined by logger_id AND the unique set of file_hashes a given measurement belongs to. The code in reference_implementations/timestamp_fixer.md groups by (logger_id, file_hash) only, which does not capture the “set of files per measurement” idea (it collapses each file separately).
Action: revise the algorithm to:
Deduplicate rows by (logger_id, record) across files.
For each (logger_id, record), compute the set of file_hashes containing that record (e.g., sorted list → serialized signature).
Group by (logger_id, file_set_signature), then anchor on the min(record) within that group.
Document the behavior for duplicated measurements and how you keep one canonical row.

#### RILEY RESPONSE
Sounds good. That makes sense.

### Parameter storage/type and naming consistency

Reviewer 1’s consolidation says parameter_overrides.value moves to JSONB with typed values. The schema in 02_database_and_storage.md still shows TEXT.
Action:
Update the DDL for parameter_overrides (e.g., value JSONB NOT NULL).
Define a canonical parameter codebook (names, units, types). notes/parameter_info.toml uses legacy, inconsistent, and informal codes (and language) that do not match calculations.md (e.g., parameter_wound_correction_a/b/c, parameter_thermal_diffusivity_k_cm2_s, etc.). Replace/normalize this file to the final codes expected by the calculator and document mapping rules in notes/columnnames.toml or a new parameters.toml.

#### RILEY RESPONSE
I agree. Implementing agent, take careful note of this. Thanks reviewer!

### Deployment disambiguation rules must be enforced by constraints

You’ve decided: no overlaps; even exact touching (end == start) should be rejected; at any given time a given logger ID (canonical or alias) can be active in only one deployment.
Action:
Add DB constraints (e.g., EXCLUDE USING gist on (datalogger_id WITH =, tstzrange(start_timestamp_utc, end_timestamp_utc) WITH &&) to prevent overlaps; and apply the same to aliases resolved to a canonical logger. If you rely on aliases, add a normalized table (datalogger_aliases) and enforce uniqueness across time windows.
Update transaction validation logic to catch boundary ties.

#### RILEY RESPONSE
I agree. Implementing agent, take careful note of this. Thanks reviewer!

### Flattening step for hierarchical parsed data is unspecified but required

The pipeline pseudocode operates on a single DataFrame with columns like alpha, beta, tm_seconds, etc., but these live in the thermistor-pair frames under logger.sensors[].thermistor_pairs[].
Action:
Define and implement Step 0 (before timestamp fixing): explode/flatten the hierarchical structure into “one observation per thermistor pair per timestamp,” with columns:
logger-level: timestamp, record, logger_id, battery_voltage_v, etc.
pair context: sdi12_address, thermistor_depth, and optionally sensor_type.
pair metrics: alpha, beta, tm_seconds (time_to_max_*), etc.
This aligns with notes/data_column_conventions.md.

#### RILEY RESPONSE
I agree. Implementing agent, take careful note of this. Thanks reviewer!

### API download semantics vs. pre-signed URLs

You’ve adopted pre-signed URLs (reviewer_2_clarifications), but 06_cli_and_api_reference.md still describes GET /outputs/{id}/download streaming the file.
Action: update API semantics. Typical options:
Return a short‑lived pre‑signed URL JSON ({ url, expires_at }) and let the client fetch from R2; or
302 redirect to the pre‑signed URL.
Note that R2 remains private and that assets/downloads flow through pre‑signed links.

#### RILEY RESPONSE
I agree. Implementing agent, take careful note of this. Thanks reviewer!

### Admin seeding story conflicts (two approaches described)

notes/admin_cli.md proposes a separate sapflux-admin binary; notes/db_seed.md proposes a hidden sapflux admin db-seed command.
Action: choose one approach and update both docs accordingly (the hidden command + Just recipe is simplest in your single-host deployment).

#### RILEY RESPONSE
Whoops that was a holdover from an old idea. I have deleted the reference to the hidden command. Thanks for the catch.

## Important clarifications (non‑blocking, but please resolve)
### Units/headers validation in parsers too rigid

Both Cr300TableParser and SapFlowAllParser validate units/characteristics with fixed-length, fixed-position arrays based on the provided fixtures. Real deployments vary by number of sensors and program changes.
Action: make validation pattern-based (e.g., per-column family checks) rather than requiring exact counts/positions. Otherwise you’ll reject valid data.

#### RILEY RESPONSE
I agree. Implementing agent, take careful note of this. Thanks reviewer!

### Concurrency model enforcement

You’ve said “transactions are necessarily one at a time. No concurrency.” Please state how this is enforced:
Single API instance with a global in‑process mutex, or
A database advisory lock taken for the duration of /transactions processing, or
A single worker/queue model behind the API.
Note future scaling implications.

#### RILEY RESPONSE
I envisioned it as a single instance with a global in-process mutex.

### “is_latest” maintenance policy

outputs.is_latest exists, but the rule to flip older outputs is unspecified.
Action: define the scope (latest per pipeline? per site/project?). Provide the update logic (e.g., in the same transaction that inserts outputs, set previous matching rows’ is_latest=false).

#### RILEY RESPONSE
There is only one output for the whole operation! All sites, projects etc return to the same output dataframe. That should simplify how latest is tracked, it's the latest of the only track that the application supports.

### Dry-run receipts and persistence

Clarify whether a dry-run writes a transaction row (your docs say yes) and how you make that discoverable (e.g., a dry_run flag in the receipt). Also specify retention and whether dry-run rows should be included in public logs.

#### RILEY RESPONSE
Whoops, that's a mistake. Dry run should not write a transaction. Dry run should be included in logs though, and users should get a receipt (that mentions it's a dry run).

### Quality filter parameters

Great to make thresholds parameterized. Please list the canonical parameter codes for these thresholds (e.g., quality_max_flux_cm_hr, quality_min_flux_cm_hr, quality_gap_years, etc.) and confirm they integrate with the same JSONB override cascade.

#### RILEY RESPONSE
Those all sound good, as long as they're semantically meaningful, clear, and unambiguous they will be ok. Those look good.

### Logger alias matching during enrichment

You decided to match on both canonical code and aliases. Document the exact join logic and precedence if the same alias appears on more than one datalogger historically (should be prevented by constraints, but the logic should state “fail fast if ambiguous”).

#### RILEY RESPONSE
Sounds good. Agent, take note of this please.

### Receipt fields for rejected files

You already include ingest_context.original_path and a reason. Consider adding: file_hash (when computable), parser_attempts (name + reason), and first_error_line to speed operator triage.

#### RILEY RESPONSE

Yep those all look good to me.

### Caddy + Cloudflare TLS

With Cloudflare proxying, HTTP‑01 challenges won’t work on origin. Call out DNS‑01 with Cloudflare (Caddy supports this via plugin) or use Cloudflare origin certificates. This removes surprise during first deploy.

#### RILEY RESPONSE
Sounds good. Let's do this.

## Testing coverage pointers

### Note explicit tests you plan to write:
- Parser: invalid SDI‑12 address, non‑sequential record numbers, header/multiple-sensor variations.
Timestamp fixer: DST fall/spring transitions (ambiguous/missing local times), duplicate records across files, mixed implied visits.
- Transaction engine: atomicity with partial file accept, dry-run persistence, and rollback of all metadata writes on validation failure.
- Cartridge: end‑to‑end hash‑match verification.

#### RILEY RESPONSE
Sounds good.

## Minor/documentation nits

### 04_ingestion_and_data_formats.md still shows ParsedFileData without file_hash; please align to the canonical struct.

#### RILEY RESPONSE
Noted, thanks for the heads up.

### reference_implementations/sapflux-parser/tests.rs references a CR300Series_401_Table1.dat fixture that isn’t in the repo snippet; ensure fixtures are coherent.

#### RILEY RESPONSE
Noted, thanks for the heads up.

### notes/parameter_info.toml contains informal language; replace with the canonical parameter dictionary and professional descriptions.

#### RILEY RESPONSE
Noted, thanks for the heads up.

### Clarify whether assets (icons) are public; if private, they also need pre‑signed access or API proxying.

#### RILEY RESPONSE
Noted, thanks for the heads up.

## Suggested concrete fixes (brief)

### Add file_hash to ParsedFileData in reference code; ingestion engine computes blake3, sets field post-parse; archive serializer includes it in manifest.json.

#### RILEY RESPONSE
Good catch, we'll address it.

### Implement SapFlowAll logger_id extraction from FileMetadata.logger_name and add logger_id column to logger.df; add a check that any existing per-row id column is constant across rows.

#### RILEY RESPONSE
Good catch, we'll address it.

### Enforce record monotonicity in parsers; add tests.

#### RILEY RESPONSE
Good idea, we'll do that.

### Revise timestamp_fixer to group by file‑set signature, not single file_hash, when determining chunks.

#### RILEY RESPONSE
Good idea, we'll do that.

### Update parameter_overrides DDL to value JSONB; publish the canonical parameter codes/units/types used by calculator and quality filters.

#### RILEY RESPONSE
Good idea, we'll do that.

### Add EXCLUDE constraint to deployments to prohibit overlap (and boundary equality if that’s the rule); document alias handling and add a normalized alias table if needed.

#### RILEY RESPONSE
Good idea, we'll do that.

### Update /outputs/{id}/download to return a pre‑signed URL or redirect; document expiry and retry behavior.

#### RILEY RESPONSE
Good idea, we'll do that.

### Pick one seeding approach (hidden command vs. separate binary) and update both notes.

#### RILEY RESPONSE
Already done.

## Can this plan be finalized and implemented after addressing the above?
Yes. Once you resolve the blocking items (1–8) and make the small clarifications, you’ll have a consistent, implementable, and auditable design that matches your stated principles. If helpful, I can turn the above into precise PR checklists for the codebase and docs.