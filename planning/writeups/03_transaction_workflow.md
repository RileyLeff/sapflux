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
2.  **Preflight Validation (Read-Only)**: The manifest is parsed and validated in dependency order without opening a database transaction.
    *   For each `add`/`update`: The system checks selectors, uniqueness, foreign keys, and overlap constraints. Failures short-circuit with an immediate `REJECTED` outcome and no database mutations.
    *   For each new `file`: The engine computes its `blake3` hash, checks for duplicates, and executes every active parser in memory. Parser failures are recorded per attempt. A file that never parses is marked for rejection but does not invalidate the manifest if metadata remains valid.
3.  **Mutating Phase**: If preflight succeeds, the engine opens a database transaction.
    *   All metadata inserts/updates execute first.
    *   Each successfully parsed file is inserted into `raw_files` and uploaded to object storage as part of the same database transaction.
    *   Files that failed parsing are omitted entirely.
4.  **Finalize Outcome**:
    *   On success, the database transaction commits and a `transactions` row is inserted in autocommit mode with `outcome = ACCEPTED` and the final receipt payload.
    *   If any mutation fails, the database transaction rolls back; a `transactions` row is still written in autocommit mode with `outcome = REJECTED` and the failure details.
    *   Dry runs do not perform any mutations or inserts. They return a receipt and durably log the attempt via application logs only.
5.  **Return Receipt**: The detailed JSON receipt is returned to the client. It includes a `summary.status` of either `COMPLETE` or `PARTIAL_SUCCESS` and lists any rejected files.

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
