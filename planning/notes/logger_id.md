## Logger ID Resolution Strategy

### The Challenge

Different Campbell Scientific TOA5 file formats store the datalogger's unique identifier in different locations.
1.  **"Table" Formats** (e.g., `CR300Series_402_Table2.dat`) typically include an `id` column where every row contains the logger's ID.
2.  **"SapFlowAll" Formats** (e.g., `CR300Series_420_SapFlowAll.dat`) often omit this per-row `id` column and instead store the ID as part of the `logger_name` string in the file header.

For the pipeline to function reliably, it must operate on a single, canonical data format where the logger ID is always present in a consistent column.

### The Solution: Parser-Level Standardization

It is the **non-negotiable responsibility of each `Parser` implementation** to resolve this inconsistency. Every parser, regardless of the file format it targets, must produce a `DataFrame` that contains a standardized `logger_id` column.

This approach ensures that all downstream components, such as the `timestamp_fixer` and `metadata_enricher`, can operate on the firm guarantee that this column exists and is correctly formatted.

#### Case 1: Handling Per-Row `id` Columns

*   **Source File Example**: `CR300Series_402_Table2.dat`
*   **Raw Data Contains**: A column named `id` with a value like `402` in every row.
*   **Parser Action**:
    1.  Read the `id` column from the raw data.
    2.  As a validation step, the parser should confirm that all values in this column are identical. If they are not, it should return a `ParserError`.
    3.  Rename the `id` column to the canonical name: **`logger_id`**. The value is already in the desired standardized format.

#### Case 2: Handling Header-Based `logger_name`

*   **Source File Example**: `CR300Series_420_SapFlowAll.dat`
*   **Raw Data Contains**: No `id` column. The TOA5 header contains a `logger_name` field with a value like `"CR300Series_420"`.
*   **Parser Action**:
    1.  Recognize that the per-row `id` column is missing.
    2.  Read the `logger_name` string from the parsed `FileMetadata`.
    3.  Apply a string extraction rule to isolate the numeric identifier. The standard rule is to split the string by the `_` character and take the last segment. For `"CR300Series_420"`, this yields `"420"`.
    4.  Create a **new column** in the output `DataFrame` with the canonical name **`logger_id`**.
    5.  Populate every row of this new column with the extracted, standardized ID (`"420"`).

### The Final Guarantee

By enforcing this logic at the parser level, the system guarantees that every `ParsedFileData` object entering the processing stage has a logger-level `DataFrame` with a consistent `logger_id` column containing the simple, numeric ID as a string. This dramatically simplifies the entire pipeline.


Logger ID Resolution Strategy
The Challenge
Different Campbell Scientific TOA5 file formats store the datalogger's unique identifier in different locations.
"Table" Formats typically include an id column where every row contains the logger's ID.
"SapFlowAll" Formats often omit this per-row id column and instead store the ID as part of the logger_name string in the file header.
For the pipeline to function reliably, it must operate on a single, canonical data format where the logger ID is always present in a consistent column.
The Solution: Parser-Level Standardization
It is the non-negotiable responsibility of each Parser implementation to resolve this inconsistency. Every parser must produce a DataFrame that contains a standardized logger_id column.
Case 1: Handling Per-Row id Columns
Parser Action:
Read the id column from the raw data.
Perform the mandatory validation step described below.
Rename the validated id column to the canonical name: logger_id.
Case 2: Handling Header-Based logger_name
Parser Action:
Recognize that the per-row id column is missing.
Read the logger_name string (e.g., "CR300Series_420") from the parsed FileMetadata.
Extract the numeric identifier (e.g., "420").
Create a new column in the output DataFrame named logger_id and populate every row with the extracted ID.
Validation: Enforcing a Single ID per File
Because the raw data can be unreliable, the parser must act as a data quality gatekeeper.
Principle: A single raw data file must only contain data from a single datalogger.
Implementation:
When a parser encounters a file with a per-row id column, it must verify that all values in that column are identical.
If more than one unique ID is found, the file is considered corrupt. The parser must immediately fail and return a specific, informative error (e.g., ParserError::InconsistentLoggerId { found_ids: ["402", "42"] }).
This strict validation prevents inconsistent data from ever entering the pipeline.
The Final Guarantee
This parser-level standardization and validation guarantees that every ParsedFileData object entering the processing stage has a logger_id column where the value is consistent for every row.