Of course. Here is the fourth document in the series, `docs/04_ingestion_and_data_formats.md`.

This document details the crucial first stage of the pipeline: how raw, unstructured text files are validated, deduplicated, and transformed into a well-defined, in-memory data structure. It explains the relationship between Parsers and the Data Formats they produce.

---

# `docs/04_ingestion_and_data_formats.md`

## Ingestion and Data Formats

The ingestion stage is the entry point for all raw data into the Sapflux pipeline. Its responsibility is to take user-submitted text files and transform them into a standardized, structured, in-memory **Data Format** that the rest of the system can work with. This process is designed to be robust, versionable, and highly configurable.

### The Ingestion Flow

For each new file submitted as part of a transaction, the system follows a strict, sequential process:

1.  **Calculate Content Hash**: The system computes a `blake3` hash of the raw file's content. This hash serves as the file's unique, content-based identifier.
2.  **Deduplication Check**: The system queries the `raw_files` database table for the calculated hash.
    *   **If the hash exists**, the file is a duplicate. It is considered successfully "ingested" without further action and noted in the transaction receipt.
    *   **If the hash is new**, the file proceeds to the next step.
3.  **Parser Selection & Execution**: The system queries the `parsers` table for all records where `include_in_pipeline` is `TRUE`. It then iterates through this list of active parsers, attempting to parse the file with each one.
    *   The first parser that succeeds without error wins.
    *   The result is a structured, in-memory object that implements the `ParsedData` trait.
    *   If no active parser can successfully process the file, the file is rejected, and the reason is recorded in the transaction receipt.

---

### Parser Architecture

Parsers are the compiled-in Rust components responsible for understanding the structure of specific raw file types.

It is critical to understand that the relationship between a parser and the data format it produces is fixed in the code. For example, the SapFlowAllParserV1 will always produce a sapflow_toa5_hierarchical_v1 data format. The output_data_format_id field in the parsers database table is a declaration of this hard-coded fact, not a configuration. It serves as a manifest that informs the rest of the pipeline what to expect from a given parser, enabling the system to correctly select the appropriate downstream ProcessingPipeline.

#### The `SapflowParser` Trait

To enable the system to work with different parsers generically, all parser structs must implement the `SapflowParser` trait. This trait defines the "contract" for a parser.

**Rust Trait Definition**```rust
use crate::errors::ParserError;
use crate::model::ParsedData;

/// A trait that must be implemented by any struct that represents a versioned parser.
pub trait SapflowParser: Send + Sync {
    /// Returns the unique, hardcoded identifier for this parser.
    /// This string MUST exactly match the `code_identifier` in the `parsers` database table.
    fn code_identifier(&self) -> &'static str;

    /// Attempts to parse the raw file content.
    /// On success, returns a Boxed trait object of a struct that implements `ParsedData`.
    /// On failure, returns a `ParserError`.
    fn parse(&self, content: &str) -> Result<Box<dyn ParsedData>, ParserError>;
}
```

The application's ingest engine uses the `parsers` table in the database as a dynamic "control panel" to determine which compiled-in parsers are active for any given run.

---

### Data Format Architecture

A Data Format is a defined, versioned schema for the structured data that is passed between the major stages of the pipeline. It acts as a strict "contract" that a parser fulfills and a processing pipeline accepts.

The definition of a Data Format is a matter of code (Rust `struct`s), not database configuration. The `data_formats` table in the database is simply an **inventory** of the schemas that are known to exist in the compiled application.

#### The `ParsedData` Trait

All structs that represent a Data Format must implement the `ParsedData` trait. This trait provides the crucial link between the compiled Rust code and the database record via the `data_format_name()` function.

**Rust Trait Definition**
```rust
use std::any::Any;

/// A trait that must be implemented by any struct that represents a versioned data format.
pub trait ParsedData: Any + Send {
    /// Returns the unique, hardcoded identifier for this data format.
    /// This string MUST exactly match the `code_identifier` in the `data_formats` database table.
    fn data_format_name(&self) -> &'static str;
}
```

---

### Canonical Data Format: `sapflow_toa5_hierarchical_v1`

This is the primary (and initially, the only) Data Format in the system. It is designed to be a rich, hierarchical representation of the data contained in Campbell Scientific TOA5 datalogger files.

*   **Identifier**: `sapflow_toa5_hierarchical_v1`
*   **Description**: A structured format containing top-level file metadata, a primary logger-level `DataFrame`, and a nested hierarchy of `DataFrame`s for each sensor and thermistor pair.

#### Rust Struct Definition

This Data Format is defined by a set of collaborating Rust structs. The top-level struct, `ParsedFileData`, implements the `ParsedData` trait.

```rust
use polars::prelude::*;
use serde::{Deserialize, Serialize};

// This is the top-level container for the parsed data.
// It implements the `ParsedData` trait, declaring its format name.
pub struct ParsedFileData {
    pub file_hash: String,
    pub raw_text: String,
    pub file_metadata: FileMetadata,
    pub logger: LoggerData,
}

impl ParsedData for ParsedFileData {
    fn data_format_name(&self) -> &'static str {
        "sapflow_toa5_hierarchical_v1"
    }
}

// Metadata parsed from the TOA5 header row of the raw file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub file_format: String,
    pub logger_name: String,
    pub logger_type: String,
    pub serial_number: Option<String>,
    pub os_version: Option<String>,
    pub program_name: String,
    pub table_name: String,
    // ... other optional fields
}

// Contains the primary logger-level data and the list of associated sensors.
#[derive(Debug, Clone)]
pub struct LoggerData {
    /// A Polars DataFrame containing logger-level measurements like 'timestamp',
    /// 'record', 'battery_voltage_v', etc. The number of rows in this
    /// DataFrame defines the length of all nested DataFrames.
    pub df: DataFrame,

    /// A vector containing the data for each sensor attached to the logger.
    pub sensors: Vec<SensorData>,
}

// Contains data for a single sensor, identified by its SDI-12 address.
#[derive(Debug, Clone)]
pub struct SensorData {
    pub sdi12_address: Sdi12Address,

    /// An optional DataFrame for sensor-level metrics that are not specific
    /// to a thermistor (e.g., 'total_sap_flow_lph').
    pub sensor_df: Option<DataFrame>,

    /// A vector containing the data for each thermistor pair on this sensor.
    pub thermistor_pairs: Vec<ThermistorPairData>,
}

// Contains the DataFrame for a specific thermistor pair (e.g., "inner" or "outer").
#[derive(Debug, Clone)]
pub struct ThermistorPairData {
    pub depth: ThermistorDepth, // An enum, either Inner or Outer

    /// A DataFrame containing all measurements from this specific thermistor pair,
    /// such as 'alpha', 'beta', 'sap_flux_density_cmh', etc.
    pub df: DataFrame,
}
```

The ingestion engine is responsible for computing `file_hash` and populating it after the parser returns. Parsers must not attempt to hash their inputs.

### Parser Validation Guarantees

To make downstream processing deterministic, active parsers must provide the following guarantees:

*   **Sequential Records**: The `record` column must increment by exactly one for each row. Parsers enforce this and reject a file the moment the sequence breaks.
*   **Logger ID Normalisation**: Every output `logger.df` contains a `logger_id` column. When a raw file lacks an `id` column, the parser derives the identifier from `file_metadata.logger_name` (e.g., splitting `CR300Series_420` → `"420"`). If any per-row `id` values disagree with one another, parsing fails.
*   **All Columns Extracted**: Parsers hydrate every measurement available in the source file—even when the default calculator does not reference a column today—ensuring future pipelines require no historical re-parsing.
*   **Strict SDI-12 Validation**: Addresses must be single ASCII alphanumerics; any deviation ejects the file with a helpful error.
