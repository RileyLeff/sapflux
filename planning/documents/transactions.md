# Transaction Architecture

This document describes the transaction model for the Sapflux pipeline. The transaction is the fundamental unit of change for all data and metadata within the system. The architecture is designed to be **atomic**, **auditable**, **idempotent**, and **user-friendly**, supporting both interactive command-line (CLI) and graphical user interface (GUI) workflows.

## Core Principles

1.  **History is Immutable**: No records are ever truly deleted. History is preserved by toggling inclusion flags. This ensures the state of the database at any previous transaction is fully reproducible.
2.  **Atomicity**: A transaction is an "all-or-nothing" operation. It is either **ACCEPTED**, and all its valid changes are committed to the database, or it is **REJECTED**, and no changes are committed.
3.  **Declarative Manifest**: Users define the desired state of changes in a human-readable "Transaction Manifest" (in TOML format). They describe *what* they want the end state to include, not *how* to achieve it.
4.  **Explicit Intent**: The manifest uses separate verbs (`add`, `update`) to force users to be clear about their intent, preventing common errors like accidentally creating a new record when a typo was made in a selector.
5.  **Auditability**: Every attempt to change the system state, whether successful or not, is recorded as a permanent transaction with a detailed receipt.

## 1. The Transaction Manifest

The primary way a user interacts with the system is by creating a Transaction Manifest. This is a TOML file that declaratively lists all the additions, updates, and new data files for a single transaction.

### Manifest Schema

The manifest consists of a top-level `message`, optional arrays of tables for `add` and `update` operations, and an optional list of `files`.

**Example `new_site_setup.toml`:**
```toml
# A user-provided "commit message" that will be stored with the transaction record.
message = "Initial setup for the new Blackwater National Wildlife Refuge site and decommissioning of an old sensor."

# 'add' operations are grouped by resource type using TOML arrays of tables.
# The system will reject the transaction if a record with the same unique key already exists.
[[sites.add]]
code = "BNWR"
name = "Blackwater NWR"
timezone = "America/New_York"
# Geospatial data can be represented using GeoJSON format.
boundary = { type = "Polygon", coordinates = [[ [-76.0, 38.5], [-76.0, 38.6], [-75.9, 38.6], [-75.9, 38.5], [-76.0, 38.5] ]] }

[[zones.add]]
site_code = "BNWR" # Human-readable codes are used to establish relationships.
name = "Upland Forest"

# 'update' operations are also grouped.
# They require a 'selector' to find the unique record and a 'patch' of fields to change.
[[deployments.update]]
# The 'selector' uniquely identifies the record to be modified using human-readable codes.
selector = { site_code = "MBY", zone_name = "Low Forest", plot_name = "Plot 3", plant_code = "MBY-LF-P3-05", stem_code = "1" }
# The 'patch' contains only the fields that are being changed.
patch = { include_in_pipeline = false, notes = "Sensor decommissioned on 2025-09-20." }

# Data addition is handled via a list of file paths relative to the manifest file.
# The CLI will read these files, hash them, and bundle their content for the API.
files = [
    "./rawdata/BNWR/CR300_501_2025_09_20.dat",
    "./rawdata/BNWR/CR300_502_2025_09_20.dat"
]
```

## 2. The Transaction Database Model

The transaction model is composed of a primary `transactions` table that serves as a permanent, auditable ledger of every attempt to change the system.

### PostgreSQL Schema
```sql
-- A binary outcome for the transaction attempt.
-- The details of "how" it was accepted (e.g., partial file ingest) are in the receipt.
CREATE TYPE transaction_outcome AS ENUM (
    'ACCEPTED', -- The transaction was valid and its changes were committed.
    'REJECTED'  -- The transaction was invalid and was rolled back. No changes were committed.
);

CREATE TABLE IF NOT EXISTS transactions (
    -- A unique identifier for this specific transaction attempt.
    transaction_id  UUID PRIMARY KEY,

    -- The unique identifier of the user (from the authentication system) who initiated the transaction.
    user_id         TEXT NOT NULL,

    -- A user-provided "commit message" describing the purpose of the transaction.
    message         TEXT,

    -- The timestamp when the transaction was processed by the API.
    attempted_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- The final, binary outcome of the transaction attempt.
    outcome         transaction_outcome NOT NULL,

    -- A comprehensive JSONB report detailing the configuration used, the operations performed,
    -- and the results of the attempt, whether it was accepted or rejected.
    receipt         JSONB
);
```

### Rust Struct (`sapflux-repository`)
This struct represents a single record from the `transactions` table.
```rust
use chrono::{DateTime, Utc};
use serde_json::Value; // Using serde_json::Value for the flexible receipt
use uuid::Uuid;

pub enum TransactionOutcome {
    Accepted,
    Rejected,
}

pub struct Transaction {
    pub transaction_id: Uuid,
    pub user_id: String,
    pub message: Option<String>,
    pub attempted_at: DateTime<Utc>,
    pub outcome: TransactionOutcome,
    pub receipt: Value,
}
```

## 3. The Transaction Receipt

Every transaction attempt, whether `ACCEPTED` or `REJECTED`, produces a detailed receipt. This receipt is the primary source of feedback for the user and a crucial artifact for auditing and debugging.

### Receipt for a REJECTED Transaction
This occurs if there is an unrecoverable error, such as a metadata integrity violation or a file parsing failure in strict mode.

```json
{
  "outcome": "REJECTED",
  "config_used": {
    "ingest": { "allow_partial_success": false },
    "processing": { "apply_timestamp_fix": true, "join_deployment_metadata": true },
    "calculations": { "wound_correction_model": "Burgess2001" }
  },
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

### Receipt for an ACCEPTED Transaction (with Partial Success)
This occurs in the default "forgiving ingest" mode, where some files are accepted and others are not.

```json
{
  "outcome": "ACCEPTED",
  "config_used": { /* ... */ },
  "summary": {
    "status": "PARTIAL_SUCCESS",
    "message": "Transaction accepted, but 1 of 3 files were rejected.",
    "files_processed": 3,
    "files_accepted": 2,
    "files_rejected": 1
  },
  "changes_applied": [
    { "type": "INSERT", "resource": "raw_files", "count": 2 }
  ],
  "rejected_files": [
    {
      "ingest_context": { "original_path": "/Users/riley/data/corrupt_file.dat" },
      "reason": "ParserError: DataRow invalid on line 112: expected 14 columns but found 13"
    }
  ]
}
```

## 4. The Transaction Lifecycle and API

A single API endpoint, `POST /transactions`, is the entry point for all changes. It accepts the transaction manifest and an optional `?dry_run=true` query parameter.

### API Endpoint: `POST /transactions`

*   **Body**: A payload containing the manifest content and the raw content of all referenced files.
*   **Query Parameter**: `?dry_run=true` (optional). If present, the API will perform a full validation and return a receipt, but will guarantee a `ROLLBACK` at the end, leaving the database untouched.

### API Processing (Atomic)

1.  **Begin DB Transaction**.
2.  **Validate All Operations**:
    *   For each `add` operation: Check if a record with the given unique key(s) already exists. If so, reject the transaction (prevents accidental duplicates). Check foreign key relationships.
    *   For each `update` operation: Use the `selector` to find the target record. If exactly one record is not found, reject the transaction (prevents updating the wrong record).
    *   For each new `file`: Calculate its content hash. Check if it already exists. If not, validate by parsing it in memory.
3.  **Determine Outcome**: If any validation step fails, the entire transaction is marked for rejection. Otherwise, it's marked for acceptance.
4.  **Record and Commit/Rollback**:
    *   A detailed **receipt** is generated.
    *   A new record is inserted into the `transactions` table with the outcome (`ACCEPTED` or `REJECTED`) and the receipt.
    *   If the transaction was marked for **rejection**, or if `?dry_run=true`, the database transaction is **ROLLED BACK**.
    *   If the transaction was marked for **acceptance** and it's not a dry run, the database transaction is **COMMITTED**.
5.  **Return Receipt**: The detailed receipt is returned to the client.

## 5. User Interaction Models

### The Command-Line Interface (CLI)
The CLI supports both quick, one-off changes and large, file-based batch updates.

#### A. Stateful, Interactive Workflow
This workflow allows a user to build a transaction piece by piece.
1.  **`sapflux transaction new --message "..."`**: Creates a local pending transaction and an empty `manifest.toml`.
2.  **`sapflux <resource> add/update ...`**: The user adds operations (e.g., `sapflux sites add ...`), which the CLI appends to the local `manifest.toml`.
3.  **`sapflux data add /path/to/files/*`**: The CLI adds file paths to the manifest.
4.  **`sapflux transaction push [--dry-run]`**: The CLI sends the complete manifest and associated file content to the API for processing.

#### B. Declarative, File-Based Workflow
This is ideal for large, repeatable, or version-controlled changes.
1.  The user manually creates a complete `manifest.toml` file describing all desired changes.
2.  **`sapflux transaction apply --file path/to/manifest.toml [--dry-run]`**: The CLI sends the manifest and its referenced files to the API in a single, self-contained request.

### The Graphical User Interface (GUI)
A web-based GUI provides a visual way to build and submit transactions. While the user experience is point-and-click, the underlying mechanism is identical to the CLI. The GUI constructs a JSON representation of the Transaction Manifest in the browser's state and submits it to the same `POST /transactions` endpoint.

## 6. Idempotency and Duplicate Handling

*   **File Duplicates**: If a transaction attempts to add a file whose content hash already exists in the `raw_files` table, this is treated as a success and noted in the receipt. No new `raw_files` record is created.
*   **Re-running an `update`**: An `update` operation can be run multiple times with the same result, as it sets fields to a specific state.
*   **Re-running an `add`**: An `add` operation for a record that already exists will be **rejected** with a clear error to prevent accidental data conflicts. A user must explicitly use `update` to modify an existing record.