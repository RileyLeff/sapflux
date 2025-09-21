# Pipeline Component Architecture

This document describes the architecture for managing the versioned, configurable components of the Sapflux processing pipeline: **Parsers**, **Data Formats**, and **Processing Pipelines**. This architecture allows the system's logic to evolve over time while maintaining full historical reproducibility.

## Core Concept: The Database as a Control Panel

The central design philosophy is the separation of **implementation** from **configuration**.

*   **Implementation (Code)**: The logic for all parsers, data format structures, and processing pipelines is written in Rust and compiled directly into the application binary. This provides performance and compile-time safety. Each compiled component has a unique, hardcoded `code_identifier` string (e.g., `"sapflow_all_v2_optimized"`).

*   **Configuration (Database)**: The database does not store logic. Instead, it acts as a dynamic "control panel" or "switchboard". The tables in the database are an inventory of the available compiled-in components, and they contain flags that tell the running application which components are active and how they connect to each other.

This creates a deliberate two-step workflow for introducing new functionality, separating the role of a **Developer** from that of a **Data Manager**.

## The "Developer Implements, Manager Activates" Workflow

Introducing a new component (e.g., a new parser) follows a clear, auditable process:

1.  **Developer Implements**: A developer writes the new Rust code for the component. They assign it a unique, permanent `code_identifier` string. They add this component to the application's internal registry (e.g., a `HashMap`) and deploy a new version of the application. At this point, the new code exists in the running application but is completely dormant and unused.

2.  **Manager Activates**: A data manager, using the CLI or GUI, submits a **Transaction Manifest** to the API. This manifest contains an `[[parsers.add]]` operation that creates a new row in the `parsers` table. This new row's `code_identifier` field **must exactly match** the string the developer hardcoded. This transaction effectively tells the running application: "A component with this name now exists in the configuration, and you are authorized to use it."

This process ensures that no new code can run until it is explicitly and auditably activated through a transaction.

## 1. Data Formats

This table defines the "schemas" or data structures that are passed between the major stages of the pipeline. A `DataFormat` is the contract that a `Parser` fulfills and a `ProcessingPipeline` accepts.

**PostgreSQL Schema (`data_formats`)**
```sql
CREATE TABLE IF NOT EXISTS data_formats (
    data_format_id      UUID PRIMARY KEY,

    -- A unique, human-readable identifier for the data schema. This name
    -- corresponds to a specific Rust struct compiled into the application.
    -- e.g., "sapflow_toa5_hierarchical_v1"
    code_identifier     TEXT UNIQUE NOT NULL,

    -- A JSONB field containing a human-readable description of the schema,
    -- useful for documentation and for clients inspecting available formats.
    schema_definition   JSONB
);
```

**Rust Representation (Conceptual)**
A `DataFormat` is represented by a Rust struct that implements a common trait. The database `code_identifier` directly maps to the value returned by `data_format_name()`.
```rust
pub trait ParsedData {
    fn data_format_name(&self) -> &'static str; // e.g., returns "sapflow_toa5_hierarchical_v1"
}

pub struct SapflowToa5HierarchicalV1 { /* ... fields ... */ }
impl ParsedData for SapflowToa5HierarchicalV1 {
    fn data_format_name(&self) -> &'static str { "sapflow_toa5_hierarchical_v1" }
}
```

## 2. Parsers

This table is an inventory of all `Parser` components compiled into the application. It links a specific, versioned piece of code to the `DataFormat` it is guaranteed to produce.

**PostgreSQL Schema (`parsers`)**
```sql
CREATE TABLE IF NOT EXISTS parsers (
    parser_id           UUID PRIMARY KEY,

    -- A unique, human-readable identifier that corresponds to a specific
    -- Rust struct compiled into the application.
    -- e.g., "sapflow_all_v1", "sapflow_all_v2_optimized"
    code_identifier     TEXT UNIQUE NOT NULL,

    -- The semantic version of this parser's code (e.g., "1.0.0", "1.1.0").
    version             TEXT NOT NULL,

    -- Foreign key to the DataFormat this parser is guaranteed to produce.
    output_data_format_id UUID NOT NULL REFERENCES data_formats(data_format_id),

    -- The "lightswitch" to enable or disable this parser during file ingest.
    -- Multiple parsers can be active simultaneously.
    include_in_pipeline BOOLEAN NOT NULL DEFAULT TRUE
);
```

**Rust Representation (Conceptual)**
```rust
pub trait SapflowParser {
    fn code_identifier(&self) -> &'static str; // e.g., returns "sapflow_all_v1"
    fn parse(&self, content: &str) -> Result<Box<dyn ParsedData>, ParserError>;
}
```

## 3. Processing Pipelines

This table defines the end-to-end processing chains. It links an input `DataFormat` to a specific, versioned workflow composed of multiple processing steps (e.g., timestamp fixing, calculations).

**PostgreSQL Schema (`processing_pipelines`)**
```sql
CREATE TABLE IF NOT EXISTS processing_pipelines (
    pipeline_id         UUID PRIMARY KEY,

    -- A unique, human-readable name for the pipeline that corresponds
    -- to a specific, compiled-in sequence of processing steps.
    -- e.g., "standard_v1_dst_fix", "experimental_v2_new_correction"
    code_identifier     TEXT UNIQUE NOT NULL,

    -- The semantic version of this pipeline's logic (e.g., "1.0.0", "2.0.0").
    version             TEXT NOT NULL,

    -- The DataFormat this pipeline is designed to accept as input.
    input_data_format_id UUID NOT NULL REFERENCES data_formats(data_format_id),

    -- The "lightswitch" to control this pipeline's activation.
    -- Business logic should enforce that only one pipeline per
    -- input_data_format_id can be active at a time.
    include_in_pipeline BOOLEAN NOT NULL DEFAULT TRUE
);
```

## 4. Runtime Data Flow

This diagram illustrates how the components and their database configurations work together at runtime.

```
+----------------+      +-------------------+      +----------------------+
|   Raw File     |----->|   Ingest Engine   |----->|   Parsed Data V1     |
+----------------+      | (App Logic)       |      | (In Memory)          |
                        +--------^----------+      +-----------^----------+
                                 |                             |
                       1. Query DB for active                 3. Query DB for active
                          parser code_identifiers                pipeline for this
                                 |                             DataFormat
                                 |                             |
+----------------------+         |              +--------------v-------------+
| Parsers DB Table     |<--------+              | ProcessingPipelines Table  |
| code_id | include    |                       | code_id | include | input   |
|---------|------------|                       |---------|---------|---------|
| "p_v1"  | TRUE       |                       | "pipe_v1" | FALSE   | df_v1   |
| "p_v2"  | TRUE       |                       | "pipe_v2" | TRUE    | df_v1   |
+----------------------+                       +----------------------------+
         ^                                                   ^
         | 2. Use code_id to select                          | 4. Use code_id to select
         |    compiled-in Parser                             |    compiled-in Pipeline
         |    from internal registry                         |    from internal registry
         |                                                   |
+--------v----------------------+              +-------------v--------------+      +------------------+
|      Application Code         |              |      Application Code      |      |  Final Dataset   |
|                               |              |                            |----->| (Parquet File)   |
| HashMap<"p_v1", ParserV1>     |              | HashMap<"pipe_v2", PipeV2> |      +------------------+
| HashMap<"p_v2", ParserV2>     |              |                            |
+-------------------------------+              +----------------------------+
```

1.  **Ingest**: The Ingest Engine queries the `parsers` table for all `code_identifier`s where `include_in_pipeline` is `TRUE`.
2.  **Parse Attempt**: It tries each active parser (looked up from its internal, compiled-in registry) against the raw file.
3.  **Pipeline Selection**: The first parser that succeeds returns the parsed data, which is tagged with its `DataFormat`. The engine then queries the `processing_pipelines` table to find the one active pipeline for that specific `DataFormat`.
4.  **Execution**: The engine looks up the selected pipeline from its internal registry and executes its steps, producing the final dataset.