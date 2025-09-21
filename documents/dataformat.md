# Data Format Architecture

This document describes the architecture and role of Data Formats within the Sapflux pipeline. A Data Format is a defined, versioned schema for the structured data that is passed between the major stages of the pipeline, such as from a `Parser` to a `ProcessingPipeline`.

## Core Concept: In-Code Definition, Database Registration

The definition of a Data Format is a matter of code, not database configuration. This provides critical benefits:

1.  **Compile-Time Safety**: The structure of the data is defined by Rust `struct`s. The Rust compiler guarantees that any component producing or consuming a given Data Format adheres to its structure, eliminating an entire class of runtime errors.
2.  **The "Contract"**: A Data Format serves as a strict contract. A `Parser` promises to produce a specific, versioned Data Format. A `ProcessingPipeline` promises to accept a specific, versioned Data Format as input.
3.  **Database as Inventory**: The `data_formats` table in the database is not the definition of the schema itself. It is an **inventory** of the schemas that are known to exist in the compiled application, along with human-readable metadata about them.

## Role in the Pipeline Ecosystem

A Data Format is the central "hub" or "interface" that enables the decoupling of the pipeline's stages. The entire processing flow is dictated by the relationships between components and the Data Formats they produce or consume.

This flow can be visualized as:

`(Raw File)` -> `[Parser]` -> **`(DataFormat)`** -> `[Processing Pipeline]` -> `(Final Dataset)`

1.  A **Parser**'s job is to consume a raw file and, if successful, produce an in-memory instance of a specific, versioned **Data Format**. The `parsers` table in the database explicitly declares which Data Format each parser produces.
2.  The **Processing Pipeline**'s job is to accept an in-memory instance of a specific, versioned **Data Format** as its input. The `processing_pipelines` table explicitly declares which Data Format each pipeline is designed to consume.

This design allows for a flexible, many-to-many relationship. Multiple different parsers (e.g., `SapflowAllV1` and `SapflowAllV2_Optimized`) can target the same output Data Format. Similarly, multiple different processing pipelines (e.g., `StandardPipelineV1` and `ExperimentalCorrectionV2`) could be designed to operate on the same input Data Format.

## The `ParsedData` Trait

To enable the pipeline to work with different Data Formats in a generic way, all structs that represent a Data Format must implement the `ParsedData` trait.

This trait serves two purposes:
1.  It acts as a "marker" that identifies a struct as a valid Data Format.
2.  It provides the crucial link between the compiled Rust code and the database record via the `data_format_name()` function.

### Rust Trait Definition
```rust
use std::any::Any;

/// A trait that must be implemented by any struct that represents a versioned data format.
/// This allows the pipeline to handle different data structures dynamically.
pub trait ParsedData: Any + Send {
    /// Returns the unique, hardcoded identifier for this data format.
    /// This string MUST exactly match the `code_identifier` in the `data_formats` database table.
    fn data_format_name(&self) -> &'static str;
}
```

## How to Add a New Data Format

Adding a new Data Format to the system is a deliberate, two-step process that involves both a Developer and a Data Manager.

### Step 1: The Developer's Role (Code Implementation)
1.  **Define Structs**: Create the new Rust `struct`(s) that define the schema of the new format.
2.  **Implement Trait**: Implement the `ParsedData` trait for the top-level struct, providing a new, unique `code_identifier` string.
    ```rust
    // Example of a new, simpler data format
    pub struct SimpleRowBasedV1 { /* ... fields ... */ }

    impl ParsedData for SimpleRowBasedV1 {
        fn data_format_name(&self) -> &'static str {
            "simple_row_based_v1" // The new, unique identifier
        }
    }
    ```
3.  **Update Application Registry**: Add the new format to the application's internal list of known components so it is aware of its own capabilities.
4.  **Deploy**: Deploy a new version of the application binary. The new code is now present in the running application but is dormant.

### Step 2: The Data Manager's Role (Database Registration)
1.  **Create Manifest**: Create a Transaction Manifest to tell the database about the new Data Format that the developer just deployed.
    ```toml
    message = "Register new data format: simple_row_based_v1"

    [[data_formats.add]]
    # This MUST exactly match the string from the Rust code.
    code_identifier = "simple_row_based_v1"
    schema_definition = { description = "A simple, non-nested format for basic logger data.", version = "1.0.0" }
    ```
2.  **Apply Transaction**: Apply the manifest using the CLI or GUI (`sapflux transaction apply ...`). This inserts a new row into the `data_formats` table.

The new Data Format is now "live" and can be used as the target for new Parsers or the input for new Processing Pipelines.

---

## Canonical Data Format: `sapflow_toa5_hierarchical_v1`

This is the primary (and currently, only) Data Format in the system. It is designed to be a rich, hierarchical representation of the data contained in Campbell Scientific TOA5 datalogger files.

*   **Identifier**: `sapflow_toa5_hierarchical_v1`
*   **Description**: A structured format containing top-level file metadata, a primary logger-level `DataFrame`, and a nested hierarchy of `DataFrame`s for each sensor and thermistor pair.

### Database Record Example
The corresponding record in the `data_formats` table would look like this:
```sql
INSERT INTO data_formats (data_format_id, code_identifier, schema_definition)
VALUES (
    'some-uuid-here',
    'sapflow_toa5_hierarchical_v1',
    '{
        "description": "Hierarchical format for Campbell Scientific TOA5 files, with nested DataFrames for logger, sensors, and thermistors.",
        "version": "1.0.0"
    }'::jsonb
);
```

### Rust Struct Definition
This Data Format is defined by a set of collaborating Rust structs. The top-level struct is `ParsedFileData`.

```rust
use polars::prelude::*;
use serde::{Deserialize, Serialize};

// The top-level container for the parsed data.
// This struct implements the `ParsedData` trait.
pub struct ParsedFileData {
    pub raw_text: String,
    pub file_metadata: FileMetadata,
    pub logger: LoggerData,
}

impl ParsedData for ParsedFileData {
    fn data_format_name(&self) -> &'static str {
        "sapflow_toa5_hierarchical_v1"
    }
}

// Metadata parsed from the TOA5 header row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub file_format: String,
    pub logger_name: String,
    pub logger_type: String,
    pub table_name: String,
    // ... other optional fields
}

// Contains the primary logger-level data and the list of associated sensors.
#[derive(Debug, Clone)]
pub struct LoggerData {
    /// A DataFrame containing logger-level measurements like 'timestamp',
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
    /// to a thermistor (e.g., 'total_sap_flow_lph' from CR300 Table files).
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