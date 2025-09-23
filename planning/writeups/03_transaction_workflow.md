## The Transaction Workflow

The transaction is the fundamental unit of change for all data and metadata within the Sapflux pipeline. The architecture is designed to be **atomic**, **auditable**, **idempotent**, and **user-friendly**, supporting both command-line (CLI) and graphical (GUI) workflows.

### Core Principles

1.  **History is Immutable**: No records are ever truly deleted. The history of every state change is preserved forever.
2.  **Atomicity**: A transaction is an "all-or-nothing" operation for metadata and every successfully parsed file. If a file fails to parse it is excluded, but the metadata updates and the files that did succeed are committed atomically. If validation fails, no changes land.
3.  **Declarative Manifest**: Users define the *desired state* of changes in a human-readable "Transaction Manifest" (in TOML format). They describe *what* they want, not *how* to achieve it.
4.  **Auditability**: Every attempt to change the system state, successful or not, is recorded as a permanent transaction with a detailed receipt.
5.  **Serialized Execution**: Transactions are processed sequentially; the orchestrator runs exactly one manifest at a time to avoid race conditions.

---

### 1. The Transaction Manifest

The primary way a user initiates change is by creating a Transaction Manifest. This is a TOML file that declaratively lists all additions, updates, and new data files for a single, atomic operation.

#### Manifest Schema

The manifest consists of a top-level `message`, optional arrays of tables for `add` and `update` operations, and an optional list of `files`.

Supported `add` blocks now cover the full metadata graph: `projects`, `sites`, `zones`, `plots`, `species`, `plants`, `stems`, `datalogger_types`, `dataloggers`, `datalogger_aliases`, `sensor_types`, `sensor_thermistor_pairs`, the existing `deployments`, and `parameter_overrides`. Geometry-aware fields (site/zone/plot boundaries and plant locations) accept GeoJSON shapes, and site timezones are validated against the IANA catalogue. A transaction may include only metadata, only files, or both — at least one of those sections must be present.

**Example `new_site_setup.toml`:**
```toml
# A user-provided "commit message" that will be stored with the transaction record.
message = "Initial setup for the Blackwater NWR site and one associated raw data file."

# 'add' operations create new records.
# The system will reject the transaction if a record with the same unique key already exists.
[[sites.add]]
code = "BNWR"
name = "Blackwater NWR"
timezone = "America/New_York"
icon_path = "icons/sites/BNWR.png"
# Geospatial data can be represented using GeoJSON format.
boundary = { type = "Polygon", coordinates = [[ [-76.0, 38.5], [-76.0, 38.6], [-75.9, 38.6], [-75.9, 38.5], [-76.0, 38.5] ]] }

[[zones.add]]
site_code = "BNWR" # Human-readable codes are used to establish relationships.
name = "Upland Forest"

# Data addition is handled via a list of file paths relative to the manifest file.
# The CLI/GUI will read these files, hash them, and bundle their content for the API.
files = [
    "./rawdata/BNWR/CR300_501_2025_09_20.dat"
]
```

---

### 2. The API Endpoint and Lifecycle

A single API endpoint is the gateway for all changes. The logic behind this endpoint guarantees atomicity.

*   **Endpoint**: `POST /transactions`
*   **Body**: A payload containing the manifest content and the raw content of all referenced files.
*   **Query Parameter**: `?dry_run=true` (optional). If present, the API will perform a full validation and return a receipt, but guarantees a `ROLLBACK` at the end, leaving the database untouched.

#### API Processing (Atomic)

1.  **Acquire the Queue Lock**: The orchestrator guarantees serialized execution by taking an advisory lock before any work begins.
2.  **Register the Attempt**: A fresh `transaction_id` is generated and a row is inserted into `transactions` with `outcome = 'PENDING'` and a stub receipt (e.g., manifest digest). This happens outside of any explicit DB transaction so the ID is immediately usable as a foreign key.
3.  **Preflight Validation (Read-Only)**: The manifest is parsed and validated in dependency order without opening a database transaction.
    *   For each `add`/`update`: The system checks selectors, uniqueness, foreign keys, time-range overlap/adjacency (for deployments and datalogger aliases), GeoJSON validity, and timezone correctness. Failures short-circuit with an immediate `REJECTED` outcome and no database mutations.
    *   For each new `file`: The engine computes its `blake3` hash, checks for duplicates, and executes every active parser in memory. Parser failures are recorded per attempt. A file that never parses is marked for rejection but does not invalidate the manifest if metadata remains valid.
4.  **Mutating Phase**: If preflight succeeds, the engine opens a database transaction.
    *   All metadata inserts/updates execute first.
    *   For each successfully parsed file, the engine first ensures the object `raw-files/{blake3}` exists in R2 (uploads opportunistically in an idempotent fashion), then records the `raw_files` row referencing that hash. Object storage is not transactional with Postgres; if the DB transaction later rolls back the object remains as an orphan, and is reclaimed by a periodic garbage-collection task that deletes unreferenced hashes.
    *   Files that failed parsing are omitted entirely.
    *   After metadata succeeds, the orchestrator invokes the active processing pipeline with the entire batch of successfully parsed files so batch-aware steps (like timestamp fixing and deduplication across overlapping downloads) operate correctly.
5.  **Finalize Outcome**:
    *   On success, the database transaction commits and the initial `transactions` row is updated in autocommit mode with `outcome = ACCEPTED` plus the final receipt payload.
    *   If any mutation fails, the database transaction rolls back; the existing `transactions` row is updated to `outcome = REJECTED` with failure details. Any previously uploaded objects are harmlessly orphaned until the next GC run.
    *   Dry runs skip creating the `transactions` row entirely and simply return a receipt while writing a structured application log entry.
6.  **Return Receipt**: The detailed JSON receipt is returned to the client. It includes a `summary.status` of either `COMPLETE` or `PARTIAL_SUCCESS` and lists any rejected files.

---

### 3. The Transaction Receipt

Every transaction attempt, whether `ACCEPTED` or `REJECTED`, produces a detailed receipt. This receipt is the primary source of feedback for the user and a crucial artifact for auditing.

**Receipt for a `REJECTED` Transaction**
```json
{
  "outcome": "REJECTED",
  "transaction_id": "uuid-for-this-attempt",
  "error": {
    "type": "IntegrityViolation",
    "message": "Update to deployment failed: The specified selector did not uniquely identify one record.",
    "details": {
      "operation": "deployments.update",
      "selector": { "plant_code": "NON-EXISTENT-PLANT-01" },
      "records_found": 0
    }
  }
}
```

**Receipt for an `ACCEPTED` Transaction (with Partial File Success)**
```json
{
  "outcome": "ACCEPTED",
  "transaction_id": "uuid-for-this-attempt",
  "summary": {
    "status": "PARTIAL_SUCCESS",
    "message": "Transaction accepted, but 1 of 3 files were rejected.",
    "files_processed": 3,
    "files_accepted": 2,
    "files_rejected": 1
  },
  "changes_applied": [
    { "type": "INSERT", "resource": "sites", "count": 1 },
    { "type": "INSERT", "resource": "raw_files", "count": 2 }
  ],
  "rejected_files": [
    {
      "file_hash": "24d0...",
      "ingest_context": { "original_path": "/Users/riley/data/corrupt_file.dat" },
      "parser_attempts": [
        { "parser": "sapflow_all_v1", "reason": "DataRow invalid on line 112: expected 14 columns but found 13" },
        { "parser": "cr300_table_v1", "reason": "Format mismatch: table name \"SapFlowAll\"" }
      ],
      "first_error_line": 112,
      "reason": "ParserError: DataRow invalid on line 112: expected 14 columns but found 13"
    }
  ]
}
```

---

### 4. The "Reversal Transaction" Pattern

You cannot delete or deactivate a transaction, as it is a permanent historical record. Instead, to "undo" or reverse the *effects* of a previous transaction, you submit a **new transaction** that applies the opposite state change. This preserves a linear, auditable history.

The primary mechanism for this is the `include_in_pipeline` flag.

**Example `archive_deployment.toml`:**
```toml
message = "Decommission sensor on MBY-LF-P3-05 as of 2025-09-20."

# 'update' operations are used to change the state of existing records.
[[deployments.update]]
# The 'selector' uniquely identifies the record to be modified.
selector = { 
    site_code = "MBY", 
    zone_name = "Low Forest", 
    plot_name = "Plot 3", 
    plant_code = "MBY-LF-P3-05", 
    stem_code = "1" 
}
# The 'patch' contains only the fields that are being changed.
patch = { 
    include_in_pipeline = false, 
    end_timestamp_utc = "2025-09-20T12:00:00Z",
    notes = "Sensor hardware failed and was removed from the field." 
}
```

---

### 5. User Interaction Models

The CLI and GUI provide user-friendly ways to construct and submit manifests to the API.

#### The Command-Line Interface (CLI)

1.  **Stateful, Interactive Workflow**: Allows a user to build a transaction piece by piece.
    *   `sapflux transaction new --message "..."`: Creates a local pending transaction manifest.
    *   `sapflux sites add ...`: Appends a `[[sites.add]]` operation to the manifest.
    *   `sapflux data add /path/to/files/*`: Adds file paths to the manifest.
    *   `sapflux transaction push [--dry-run]`: Bundles and sends the completed manifest to the API.

2.  **Declarative, File-Based Workflow**: Ideal for large, repeatable, or version-controlled changes.
    *   The user manually authors a complete `manifest.toml` file.
    *   `sapflux transaction apply --file path/to/manifest.toml [--dry-run]`: Sends the manifest and its referenced files to the API in a single request.

#### The Graphical User Interface (GUI)

The web GUI provides a visual way to build transactions. A user might fill out a form to create a new site or use a file uploader to add data. Behind the scenes, the GUI constructs the same manifest structure in its state and submits it to the same `POST /transactions` endpoint.

---

### 6. Idempotency and Duplicate Handling

The system is designed to be robust against accidental re-submission of the same transaction.

*   **File Duplicates**: If a transaction attempts to add a file whose content hash already exists, this is treated as a success and noted in the receipt. No new `raw_files` record is created.
*   **Re-running an `update`**: An `update` operation is idempotent. Running it multiple times has the same effect as running it once.
*   **Re-running an `add`**: An `add` operation for a record that already exists will be **rejected** with a clear error to prevent accidental data conflicts. A user must explicitly use `update` to modify an existing record.

Active parsers and pipelines are still compiled into the binary. The `include_in_pipeline` flags stored in the database simply tell the orchestrator which compiled components should be considered at runtime.
