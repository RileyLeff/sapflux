# Raw Data Architecture

This document describes the architecture for storing and managing raw data files within the Sapflux pipeline. The design is founded on the principle of **immutability** to ensure a permanent, auditable, and reproducible source of truth for all data ever ingested into the system.

## Core Principles

1.  **Immutability**: Raw data, once successfully ingested and validated, is considered a permanent asset. It is never modified or deleted from the system. This guarantees that any past state of the pipeline can be perfectly reproduced.
2.  **Content-Addressable Storage**: Every unique raw file is identified by a cryptographic hash of its content (`blake3`). This means that identical files, even if uploaded with different names or at different times, are recognized as the same entity.
3.  **Transaction-Based Ingestion**: All new raw files are introduced into the system as part of an auditable, atomic **Transaction**. The file is permanently linked to the transaction that ingested it.
4.  **Explicit Inclusion**: A raw file's participation in the processing pipeline is controlled by an explicit `include_in_pipeline` flag. "Deleting" a file is a non-destructive operation that simply toggles this flag in a new transaction, preserving the file's history.

## The Two-Part Storage System

The raw data storage system consists of two tightly-coupled components: a cloud-based object store for the file content and a PostgreSQL database table that acts as an index and metadata layer.

### 1. Object Storage (The "What")

This is the permanent repository for the physical file content. It is designed to be a simple, robust, write-once archive.

*   **Technology**: Any S3-compatible object store (e.g., Cloudflare R2 for production, MinIO for local development).
*   **Location**: A dedicated bucket or prefix, e.g., `raw-files/`.
*   **Object Key Schema**: `raw-files/{file_hash}`
    *   The "filename" of the object in the bucket is the `blake3` content hash of the file itself.
*   **Lifecycle**:
    *   **Write**: A file is written exactly once, the first time its unique content is seen.
    *   **Read**: Files are read on-demand by the processing pipeline.
    *   **Delete**: Objects in this store are **never deleted**.

### 2. Database Index (The "Where" and "Why")

The `raw_files` table in the PostgreSQL database serves as the canonical index of all known raw files. It stores the metadata *about* each file, its history, and its current status in the pipeline.

#### PostgreSQL Schema (`raw_files`)
```sql
CREATE TABLE IF NOT EXISTS raw_files (
    -- The blake3 hash of the file's raw text content. This is the primary key and
    -- the link to the file's content in the object store.
    file_hash           TEXT PRIMARY KEY,

    -- A foreign key to the transaction that first introduced this unique file content.
    -- This provides a complete audit trail for the origin of every piece of data.
    ingesting_transaction_id UUID NOT NULL REFERENCES transactions(transaction_id),

    -- A flexible JSONB field to store the context of the file's original ingest.
    -- This is crucial for user experience and debugging.
    -- e.g., {"original_absolute_path": "/path/to/file.dat", "hostname": "..."} from a CLI or GUI client,
    -- or {"original_filename": "file.dat", "origin_ip": "..."}. Ideally we keep as much of this sort of info as we can.
    ingest_context      JSONB,

    -- The "lightswitch" to control if this specific file's data is processed.
    -- This is the only mutable field and can only be changed via a new transaction.
    include_in_pipeline BOOLEAN NOT NULL DEFAULT TRUE
);
```

#### Rust Struct Representation (`sapflux-repository`)
```rust
use serde_json::Value;
use uuid::Uuid;

/// Represents a record from the `raw_files` table.
pub struct RawFileRecord {
    /// The blake3 content hash of the file.
    pub file_hash: String,

    /// The ID of the transaction that first introduced this file.
    pub ingesting_transaction_id: Uuid,

    /// The original context of the upload (path, IP address, etc.).
    pub ingest_context: Option<Value>,

    /// The flag controlling the file's inclusion in processing runs.
    pub include_in_pipeline: bool,
}
```

## The Lifecycle of a Raw File

A raw file follows a clear and auditable lifecycle within the system.

1.  **Ingestion**:
    *   A user submits a transaction containing one or more new data files.
    *   For each file, the system calculates its content hash.
    *   If a file's hash is not already present in the `raw_files` table, it is considered new.
    *   The file content is uploaded to the object store at `raw-files/{file_hash}`.

2.  **Validation**:
    *   During the transaction commit phase, the system performs a "dry run" parse on all new files to ensure they are valid.
    *   If a file fails to parse and the transaction is in "strict" mode, the entire transaction is rejected.
    *   If the transaction is in "forgiving" mode, the failed file is simply noted in the transaction receipt and is not added to the `raw_files` table.

3.  **Creation**:
    *   Upon a successful (`ACCEPTED`) transaction, a new record is inserted into the `raw_files` table for each valid, new file.
    *   The `ingesting_transaction_id` is set, and `include_in_pipeline` defaults to `true`.

4.  **"Soft Deletion" / Deactivation**:
    *   A user can choose to exclude a file from future processing runs.
    *   They do this by submitting a **new transaction** with an `[[raw_files.update]]` operation.
    *   This transaction does not delete the file. It performs an `UPDATE` on the existing `raw_files` record, setting `include_in_pipeline` to `false`.
    *   This change is recorded and auditable via the new transaction, preserving the full history of the file.

5.  **Usage in Pipeline**:
    *   When a processing run is initiated, the pipeline queries the database for all records in `raw_files` where `include_in_pipeline = TRUE`.
    *   For each of these active files, it uses the `file_hash` to retrieve the content from the object store and begin processing.