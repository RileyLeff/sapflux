# Parser Architecture

Note that you can find the old parser implementation under reference_code/sapflux-parser. The reference code contains some test files in Please carefully consider the updates and improvements described here and be sure to implement them in your new implementation.

This document describes the architecture for managing the versioned, configurable `Parser` components within the Sapflux pipeline. Parsers are responsible for consuming raw datalogger text files and transforming them into a structured, in-memory **Data Format**.

## Core Concept: The Database as a Control Panel

The central design philosophy is the separation of **implementation** from **configuration**.

*   **Implementation (Code)**: The logic for all parsers is written in Rust and compiled directly into the application binary. This provides performance and compile-time safety. Each compiled parser has a unique, hardcoded `code_identifier` string (e.g., `"sapflow_all_v1"`).

*   **Configuration (Database)**: The database does not store logic. Instead, it acts as a dynamic "control panel" or "switchboard". The `parsers` table is an inventory of the available compiled-in components, and it contains flags that tell the running application which parsers are active and what `DataFormat` they produce.

This creates a deliberate two-step workflow for introducing new functionality, separating the role of a **Developer** from that of a **Data Manager**. For a detailed explanation of this workflow, see the `pipeline_components.md` document.

## The `SapflowParser` Trait

To enable the pipeline to work with different parsers in a generic way, all structs that represent a parser must implement the `SapflowParser` trait. This trait defines the "contract" for a parser component.

### Rust Trait Definition
```rust
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

## The Database Model

The `parsers` table is an inventory of all `Parser` components compiled into the application. It links a specific, versioned piece of code to the `DataFormat` it is guaranteed to produce.

### PostgreSQL Schema (`parsers`)
```sql
CREATE TABLE IF NOT EXISTS parsers (
    parser_id           UUID PRIMARY KEY,

    -- A unique, human-readable identifier that corresponds to a specific
    -- Rust struct compiled into the application.
    -- e.g., "sapflow_all_v1", "cr300_table_v1"
    code_identifier     TEXT UNIQUE NOT NULL,

    -- The semantic version of this parser's code (e.g., "1.0.0").
    version             TEXT NOT NULL,

    -- Foreign key to the DataFormat this parser is guaranteed to produce.
    output_data_format_id UUID NOT NULL REFERENCES data_formats(data_format_id),

    -- The "lightswitch" to enable or disable this parser during file ingest.
    -- Multiple parsers can be active simultaneously.
    include_in_pipeline BOOLEAN NOT NULL DEFAULT TRUE
);
```

---

## Reference Implementations

The following files represent the updated `sapflux-parser` crate, adapted to the new component architecture. The core parsing logic remains the same, but the structure has been updated to use the `SapflowParser` trait and unique `code_identifier`s.

### `src/lib.rs` (Updated)
The `registry.rs` module has been removed, as the logic for selecting and running active parsers now lives in the main application engine.
```rust
pub mod errors;
pub mod formats;
pub mod model;

// Re-export the core components for use by the main application.
pub use errors::{ParserAttempt, ParserError};
pub use model::{
    ParsedData, ParsedFileData, FileMetadata, LoggerData, Sdi12Address, SensorData,
    ThermistorDepth, ThermistorPairData,
};
pub use formats::{SapflowParser, Cr300TableParserV1, SapFlowAllParserV1};

#[cfg(test)]
mod tests;
```

### `src/formats/mod.rs` (Updated)
This file now exports the `SapflowParser` trait and the versioned parser structs.```rust
mod common;
mod cr300_table_v1;
mod sapflow_all_v1;

// The generic trait that all parsers implement.
pub trait SapflowParser: Send + Sync {
    fn code_identifier(&self) -> &'static str;
    fn parse(&self, content: &str) -> Result<Box<dyn crate::model::ParsedData>, crate::errors::ParserError>;
}

// The concrete parser implementations.
pub use cr300_table_v1::Cr300TableParserV1;
pub use sapflow_all_v1::SapFlowAllParserV1;

// Internal helpers used by the parsers.
pub(crate) use common::{
    ColumnRole, LoggerColumnKind, LoggerColumns, SensorFrameBuilder, SensorMetric,
    ThermistorMetric, build_logger_dataframe, make_logger_data, parse_metadata, parse_optional_f64,
    parse_optional_i64, parse_required_i64, parse_sdi12_address, parse_timestamp,
};
```

### `src/model.rs` (Updated)
The `ParsedFileData` struct now implements the `ParsedData` trait, declaring the `DataFormat` it represents.
```rust
// ... (all other structs and enums remain the same) ...
use crate::dataformats::ParsedData; // Assuming ParsedData is defined in a shared crate now

// ... (FileMetadata, LoggerData, etc.) ...

#[derive(Debug, Clone)]
pub struct ParsedFileData {
    pub raw_text: String,
    pub file_metadata: FileMetadata,
    pub logger: LoggerData,
}

// Implementation of the ParsedData trait to declare its format.
impl ParsedData for ParsedFileData {
    fn data_format_name(&self) -> &'static str {
        "sapflow_toa5_hierarchical_v1"
    }
}
```

### `src/formats/sapflow_all_v1.rs` (Updated Reference Implementation)
This is your `sapflow_all.rs` adapted to the new architecture.

```rust
use csv::StringRecord;
use crate::errors::ParserError;
use crate::model::{ParsedFileData, ParsedData, Sdi12Address, ThermistorDepth};
use super::SapflowParser; // Import the trait from the parent module.
// ... (other use statements)

// The struct name is now versioned.
pub struct SapFlowAllParserV1;

impl SapFlowAllParserV1 {
    pub fn new() -> Self { Self }

    // All internal logic remains the same, but now uses `self.code_identifier()` for errors.
    fn classify_columns(&self, columns: &StringRecord) -> Result<Vec<ColumnRole>, ParserError> {
        // ... (logic is identical)
    }
    // ... (all other helper functions like `split_sensor_column`, `validate_units`, etc. are identical)

    fn parse_with_builder(&self, builder: csv::ReaderBuilder, content: &str) -> Result<ParsedFileData, ParserError> {
        // This logic is identical to your original, but error messages now use the dynamic identifier.
        // For example:
        // Err(ParserError::FormatMismatch {
        //     parser: self.code_identifier(), // <-- Updated from Self::NAME
        //     reason: "..."
        // })
        // ...
        let logger_df = build_logger_dataframe(self.code_identifier(), logger_columns)?;
        // ...
        Ok(ParsedFileData { /* ... */ })
    }
}

// The implementation of the public trait.
impl SapflowParser for SapFlowAllParserV1 {
    fn code_identifier(&self) -> &'static str {
        // The unique, hardcoded identifier for this specific parser version.
        "sapflow_all_v1"
    }

    fn parse(&self, content: &str) -> Result<Box<dyn ParsedData>, ParserError> {
        // Calls the internal logic and wraps the result in a Box<dyn ParsedData>.
        let parsed_data = self.parse_with_builder(Self::reader_builder(), content)?;
        Ok(Box::new(parsed_data))
    }
}
```

### `src/formats/cr300_table_v1.rs` (Updated Reference Implementation)
This is your `cr300_table.rs` adapted to the new architecture.

```rust
use csv::StringRecord;
use crate::errors::ParserError;
use crate::model::{ParsedFileData, ParsedData, Sdi12Address, ThermistorDepth};
use super::SapflowParser;
// ... (other use statements)

pub struct Cr300TableParserV1;

impl Cr300TableParserV1 {
    pub fn new() -> Self { Self }

    // All internal logic (classify_columns, split_address, validation, etc.) is identical
    // to your original implementation, but uses `self.code_identifier()` for errors.
}

// The implementation of the public trait.
impl SapflowParser for Cr300TableParserV1 {
    fn code_identifier(&self) -> &'static str {
        // The unique, hardcoded identifier for this specific parser version.
        "cr300_table_v1"
    }

    fn parse(&self, content: &str) -> Result<Box<dyn ParsedData>, ParserError> {
        // The core parsing logic from your original file goes here.
        // On success, it returns:
        // let parsed_data: ParsedFileData = ...;
        // Ok(Box::new(parsed_data))
    }
}
```