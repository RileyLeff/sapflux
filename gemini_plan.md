Excellent. This is the most critical step. A clear, detailed plan prevents confusion, ensures we build things in the right order, and serves as our roadmap for the entire project.

Based on our discussion, here is a comprehensive, multi-phase implementation plan. We will execute this step-by-step.

### Guiding Principles

1.  **Library-First Architecture:** All core logic (database interaction, validation, Polars processing) will live in a `sapflux-core` library crate. The CLI and API will be thin wrappers that call this library.
2.  **Database as the Golden Source:** The PostgreSQL database is the single source of truth. Raw data is ingested once, validated, and becomes immutable. All processing reads from this trusted source.
3.  **Lazy All the Way:** Polars operations will be constructed as a `LazyFrame` computation graph. Data will only be materialized when absolutely necessary (e.g., sinking to a final Parquet file).

---

## The Implementation Plan

### Phase 0: Environment and Project Scaffolding

This phase creates the complete project structure and local development environment.

*   **Definition of Done:** A developer can clone the repository, run `docker-compose up`, and `cargo build` successfully.

*   **Task 0.1: Create Monorepo Structure**
    *   Action: Create the root `sapflux-monorepo` directory.
    *   Action: Create subdirectories: `db/`, `crates/`, `frontend/`.

*   **Task 0.2: Initialize Cargo Workspace**
    *   Action: Create the root `crates/Cargo.toml` workspace manifest.
    *   Action: Define `members` for `sapflux-core`, `sapflux-cli`, and `sapflux-api`.
    *   Action: Define all shared dependencies in `[workspace.dependencies]`.

*   **Task 0.3: Initialize Individual Crates**
    *   Action: Inside `crates/`, run `cargo new --lib sapflux-core`, `cargo new sapflux-cli`, `cargo new sapflux-api`.
    *   Action: Update the `Cargo.toml` of each sub-crate to inherit dependencies from the workspace and (for `cli` and `api`) declare a `path` dependency on `sapflux-core`.

*   **Task 0.4: Set up Local Database Environment**
    *   Action: Create the `docker-compose.yml` file in the project root to run PostgreSQL 16.
    *   Action: Create the `db/schema.sql` file.
    *   Action: Define the initial table structures in `schema.sql`:
        *   `file_schemas` (`id`, `name`, `description`)
        *   `raw_files` (`id`, `file_hash` (UNIQUE), `file_content` (BYTEA), `detected_schema_id`, `ingested_at`)
        *   `deployments` (all fields from your metadata CSV)
        *   `sensors` (all fields from your sensor metadata)

### Phase 1: Core Library Foundation (`sapflux-core`)

This phase builds the essential, non-processing logic and types.

*   **Definition of Done:** The `sapflux-core` library can connect to the database and map database rows to strongly-typed Rust structs.

*   **Task 1.1: Implement Centralized Error Handling**
    *   Action: Create `src/error.rs` with a `thiserror` enum for `PipelineError`, including variants for `sqlx::Error`, `io::Error`, etc.

*   **Task 1.2: Implement Core Data Types**
    *   Action: Create `src/types.rs`.
    *   Action: Define Rust structs that mirror the database tables (e.g., `Deployment`, `Sensor`).
    *   Action: Add `#[derive(sqlx::FromRow)]` to these structs to enable automatic mapping from query results.
    *   Action: Define a `FileSchema` enum (e.g., `enum FileSchema { CR300New, CR200Old, ... }`) that represents the different known CSV formats.

*   **Task 1.3: Implement Database Connection Module**
    *   Action: Create `src/db.rs`.
    *   Action: Write a `pub async fn connect() -> Result<sqlx::PgPool>` function.
    *   Action: This function will read the `DATABASE_URL` from an environment variable (we will use a `.env` file for local development).

### Phase 2: Ingestion Pipeline (`sapflux-core` & `sapflux-cli`)

This phase builds the "write path" to get validated data into our database.

*   **Definition of Done:** The `sapflux-cli ingest` command can successfully scan a directory of raw files, validate them, and load new, unique files into the `raw_files` table.

*   **Task 2.1: Implement File Validation and Schema Detection**
    *   Action: In a new `src/ingestion.rs` module, create a function `detect_schema(file_content: &[u8]) -> Result<FileSchema>`.
    *   Action: This function will contain the logic to inspect the header lines of the file content to determine which `FileSchema` it matches. It will return an error if no schema matches.

*   **Task 2.2: Implement Core Ingestion Logic**
    *   Action: In `src/ingestion.rs`, create the main library function: `pub async fn ingest_file(db_pool: &PgPool, file_content: &[u8]) -> Result<i64>`.
    *   Action: This function will orchestrate the full ingestion process:
        1.  Calculate the SHA-256 hash of `file_content`.
        2.  Query the DB to see if the hash already exists. If yes, return success (idempotent).
        3.  Call `detect_schema()` to validate the file.
        4.  `INSERT` the hash, content, and schema ID into the `raw_files` table.
        5.  Return the `id` of the newly inserted row.

*   **Task 2.3: Build the `ingest` CLI Tool**
    *   Action: Implement `crates/sapflux-cli/src/main.rs`.
    *   Action: Use `clap` to define a subcommand, e.g., `ingest`, which takes a `--directory` argument.
    *   Action: The `main` function will connect to the DB, recursively find files in the specified directory, read each file's bytes, and call `sapflux_core::ingestion::ingest_file` for each one, printing the status.

### Phase 3: Processing Pipeline (`sapflux-core` & `sapflux-cli`)

This phase builds the "read path" to transform the trusted data into the final scientific output.

*   **Definition of Done:** The `sapflux-cli process` command can successfully read all data from the database, run it through a full (stubbed) Polars pipeline, and save a Parquet file.

*   **Task 3.1: Implement LazyFrame Creation from Database Blobs**
    *   Action: In a new `src/processing.rs` module, create a function `create_lazyframe_from_blob(bytes: &[u8], schema: &FileSchema) -> Result<LazyFrame>`.
    *   Action: This function will use `std::io::Cursor` to wrap the byte slice and pass it to Polars' `LazyCsvReader`. It will use the `schema` argument to configure the reader correctly (e.g., skip rows, provide column names).

*   **Task 3.2: Implement Lazy Transformations**
    *   Action: In `src/processing.rs`, create a chain of functions that each take and return a `LazyFrame`.
    *   Action: `clean_and_normalize_schema(lf: LazyFrame) -> LazyFrame`: Renames columns, casts types, and converts `-99` to nulls.
    *   Action: `apply_dst_correction(lf: LazyFrame) -> LazyFrame`: Implements the file-origin-based chunking using Polars window functions. **(This will be the most complex single transformation).**
    *   Action: `apply_sap_flux_calculations(lf: LazyFrame) -> LazyFrame`: Implements the DMA_Péclet math.

*   **Task 3.3: Implement the Core Processing Orchestrator**
    *   Action: In `src/processing.rs`, create the main library function: `pub async fn run_processing_pipeline(db_pool: &PgPool) -> Result<LazyFrame>`.
    *   Action: This function will:
        1.  Fetch all `(file_content, detected_schema_id)` rows from the DB.
        2.  For each row, call `create_lazyframe_from_blob` to get a `LazyFrame`.
        3.  `concat` all the `LazyFrame`s into a single master `LazyFrame`.
        4.  Pass the master `LazyFrame` through the chain of transformation functions.
        5.  Return the final, fully-planned `LazyFrame`.

*   **Task 3.4: Build the `process` CLI Tool**
    *   Action: Add a `process` subcommand to `crates/sapflux-cli/src/main.rs`.
    *   Action: This command will call `sapflux_core::processing::run_processing_pipeline`, take the resulting `LazyFrame`, and `sink` it to a versioned Parquet file.

### Phase 4 & 5: Application and Deployment (Future)

*   **Task 4.1: Build the Axum API Server** (`sapflux-api`)
    *   Action: Create an HTTP endpoint `POST /files` that receives a file upload, and calls `sapflux_core::ingestion::ingest_file`.
    *   Action: Create an HTTP endpoint `GET /datasets/latest` that calls the core processing logic and serves the resulting Parquet file.

*   **Task 5.1: Build SvelteKit Frontend**
    *   Action: Create a simple UI with a file dropzone for uploads and a download button.

*   **Task 5.2: Deploy to Render.io**
    *   Action: Create a `render.yaml` "Blueprint" file to define the services (Postgres, API, Static Site) for one-click deployment.

---

This plan is detailed, structured, and directly addresses your long-term goals. We are ready to begin. Our first action item is **Task 0.1** through **Task 0.4**: setting up the project structure and the database environment.