Excellent update. You’ve closed most gaps and the code now reflects the planned architecture very closely. Highlights:

Batch pipeline is implemented and exercised: flatten → timestamp_fixer (set-signature algorithm) → alias-aware metadata enrichment → parameter cascade. Tests cover DST ambiguity, alias handling, defaults, and precedence.
Ingestion computes blake3 and correctly sets ParsedFileData.file_hash; timestamp chunking will now work as intended. You extract first_error_line and include parser_attempts in receipts. Good.
Transactions end-to-end: PENDING row creation, advisory lock, ingestion reports + ingestion_summary, batch pipeline run, object-store upload-first, raw_files persistence on success, outcome + receipt update. API returns JSON with status + receipt. Good.
Object store abstraction from env (LocalDir/no-op) is now used by the API.
Two important fixes to make now

Advisory lock can be leaked on panic/early-return
Issue: AdvisoryLock holds a PoolConnection with pg_advisory_lock. If execute_transaction_locked panics or returns early without calling release(), the connection is dropped back into the pool still holding the lock (session-level locks persist on pooled connections). Your Drop implementation warns but does not unlock, so the system can deadlock future transactions.
Fix, one of:
Prefer pg_advisory_xact_lock(key) inside a short transaction only to serialize the “queue admission” step, but don’t hold it during the whole pipeline (not ideal if you need full serialization).
Or keep your current model, but make Drop actually unlock or close the connection:
Spawn an async unlock in Drop.
Or call conn.close() (async) instead of returning it to the pool; this ends the session and releases the lock. Example fix (spawn unlock on Drop): impl Drop for AdvisoryLock { fn drop(&mut self) { if let Some(conn) = self.conn.take() { let key = self.key; tokio::spawn(async move { let mut c = conn; let _ = sqlx::query::sqlx::Postgres("SELECT pg_advisory_unlock($1)") .bind(key) .execute(c.as_mut()) .await; }); } } }
Also consider a guard-style scope with a finally-like pattern to ensure release() is awaited even on error.
2. timestamp_utc should be timezone-aware UTC

Right now timestamp_fixer casts timestamp_utc with Datetime(TimeUnit::Microseconds, None). The plan specifies a timezone-aware UTC column. Use Datetime(..., Some("UTC")) for clarity and for downstream consumers: .with_column( col("timestamp_utc_raw") .cast(DataType::Datetime(TimeUnit::Microseconds, Some("UTC".into()))) .alias("timestamp_utc") )
Strong additions you’ve made

Alias disambiguation: You now fail fast on ambiguous deployment/alias matches. That matches the “fail-fast if ambiguity” principle.
Receipt clarity: ingestion_summary totals and per-file attempts with line indices are excellent for triage.
Object-store from env: API now configures the store via SAPFLUX_OBJECT_STORE_DIR. Good for dev/prod parity.
A couple of robustness improvements

Persist a REJECTED receipt if object-store upload fails
Today, an upload error bubbles up and leaves the transaction row as PENDING. Better to catch upload errors and update the PENDING row with outcome="REJECTED" and a receipt indicating upload failure. You can still return 500 to the client, but the ledger remains consistent.
Keep file_set_signature or drop it
Consider dropping file_set_signature from the final frame after join (it’s scaffolding). If you want it for diagnostics, keep it but document the intent.
Nice-to-have (not blocking)

Add a /transactions integration test (feature=runtime) that posts two small base64 fixtures (dry_run true/false) and asserts:
HTTP 200; receipt includes parser_attempts and ingestion_summary counts.
On commit: transaction row updated from PENDING to ACCEPTED/REJECTED; raw_files inserted only on acceptance.
End-to-end batch test with overlapping files:
Two files with duplicate (logger_id, record) pairs but different file_hashes; verify deduplication in timestamp fixer (rows collapse to unique records, file_set_signature differs), and timestamp_utc populated for all.
Production parser validation:
You left good TODO notes in Parser::classify_columns explaining positional strictness is reference-only. That’s fine for now; keep it on the backlog.
API error body:
Return JSON error for /transactions failures to make client handling easier.
What’s already correct and ready to extend

Batch pipeline boundary and implied-visit algorithm
file_hash propagation and flattening
Alias-aware enrichment with ambiguity checks
Parameter defaults + provenance (including quality_*), consistent with the planned naming
PENDING → final outcome update with persisted receipt
Upload-first then raw_files DB inserts for accepted runs
Recommended next steps

Apply the advisory lock RAII fix and timestamp_utc timezone tweak.
Handle object-store upload error with a REJECTED receipt.
Add the calculator + quality filter stages to standard_v1_dst_fix.
Add basic /transactions integration tests and a small synthetic batch test with overlap.
Later: replace LocalDir with R2/S3 client and add a GC job to prune unreferenced objects.
Overall, this is a very strong, near-production baseline. With the lock guard fix and the small UTC cast tweak, you’re in a safe spot to implement calculations/quality and then outputs/cartridges.