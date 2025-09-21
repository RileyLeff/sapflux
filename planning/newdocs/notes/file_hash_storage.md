## File Hash in the Parsed Data Format

### Principle

Every parsed data object must contain the `blake3` cryptographic hash of its original source file.

### Purpose

The `file_hash` is essential for the `timestamp_fixer`'s "chunking" algorithm. The algorithm identifies unique "implied visits" by grouping data based on the unique combination of `logger_id` and the set of `file_hash`es a measurement belongs to. Storing the hash with the parsed data makes this grouping possible.

### Storage Location

To avoid data redundancy and maintain a clean, normalized structure, the `file_hash` is **not** stored as a column in the `DataFrame`.

Instead, it is stored as a **top-level field on the `ParsedFileData` struct**. This positions it as a piece of metadata that describes the entire data object, alongside the `raw_text` and `file_metadata`.

#### Updated `ParsedFileData` Struct

The canonical `sapflow_toa5_hierarchical_v1` data format is defined by the following top-level struct:

```rust
pub struct ParsedFileData {
    /// The unique blake3 hash of the original raw file's content.
    pub file_hash: String,

    /// The original, unchanged text content of the file.
    pub raw_text: String,

    /// Information extracted from the file's header row.
    pub file_metadata: FileMetadata,

    /// The core hierarchical data, including the logger-level DataFrame and nested sensors.
    pub logger: LoggerData,
}
```

### Responsibility

The responsibility for populating the `file_hash` field lies with the **ingestion engine**, not the individual parsers. The engine will:
1.  Receive the raw file content.
2.  Calculate its `blake3` hash.
3.  Invoke the appropriate parser, which returns a `ParsedFileData` object (with `file_hash` temporarily empty).
4.  Populate the `file_hash` field on the returned object before passing it to the next stage of the pipeline.