## Logger ID Resolution Strategy

### The Challenge

Different Campbell Scientific TOA5 formats surface a logger identifier in different places:

1. **Table formats** include a per-row `id` column.
2. **SapFlowAll formats** omit the column and only expose the identifier in the header (`file_metadata.logger_name`).

Downstream components require a single canonical column, so the parser must normalise these sources up front.

### Parser Responsibilities

* **Single-ID validation** – When an `id` column is present, every value must match. If more than one unique ID is encountered, the parser returns `ParserError::InconsistentLoggerId`.
* **Header extraction** – When the column is missing, derive the identifier from `file_metadata.logger_name` (e.g., `CR300Series_420` → `"420"`) and populate every row.
* **Canonical column** – The output logger-level `DataFrame` always includes `logger_id` as a string column. This value is stable within a file and across the entire processing pipeline.

### Relating IDs to Deployments

* The canonical ID is stored in `dataloggers.code`.
* Alternate identifiers observed in the field are stored in `datalogger_aliases` with an `active_during` range. Aliases cannot overlap in time for the same string.
* The metadata enricher first joins on `dataloggers.code`. Failing that, it searches for the alias whose active range covers the measurement timestamp.
* If more than one datalogger matches (which can only happen if the alias table is misconfigured), the transaction is rejected and the receipt surfaces an integrity error.

### Why This Matters

* **Timestamp fixer** – Uses the canonical `logger_id` to group implied visits across files.
* **Quality filters** – Need a deterministic way to map back to deployments when flagging rows.
* **Audit trail** – Having a single identifier per file simplifies the manifest receipts and reproducibility cartridges.
