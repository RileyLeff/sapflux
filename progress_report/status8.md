Added a filesystem-backed ObjectStore abstraction with environment-based configuration and integrated it into the transaction pipeline so uploaded raw files are persisted (or skipped) before database writes.
Updated /transactions API to construct and share the configured object store, returning consistent responses while reusing the new orchestration flow.
Logged progress in progress_report/status7.md and adjusted upload logic to remain idempotent by skipping duplicates.

Started working on initial part of these but then cut off by context window: 

Up next I’d tackle the items we parked:

Flesh out standard_v1_dst_fix: add the calculator stage plus quality filters so outputs/receipts gain real scientific columns and flags.
Once that’s solid, enrich transaction receipts with pipeline row counts, parser attempt summaries, and start plumbing whatever metadata we’ll need for reproducibility cartridges.
Finally, replace the noop object store with a real R2/S3 client and wire in error/GC handling so uploads match production semantics.