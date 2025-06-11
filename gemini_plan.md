Of course. Taking stock of progress is a critical part of any successful project. Here is a detailed report on the current state of the `sapflux` project, outlining our accomplishments, architectural decisions, and the clear path forward.

---

### **Project Status Report: `sapflux` Data Platform**

**Date:** June 11, 2025

**Overall Goal:** To build a robust, high-performance, and maintainable data platform for ingesting, processing, and serving sap flux data. The platform is designed to be a long-term "golden source" of truth, with a clear separation between raw data, metadata, and scientific processing logic.

---

### **I. Executive Summary: Architecture and Philosophy**

We have successfully pivoted from a simple script-based approach to a professional, multi-crate monorepo architecture designed for long-term stability and future expansion. The foundational principle is the **separation of concerns**: the system is divided into a core library, application clients (CLI, API), and a declarative database schema.

**Key Architectural Decisions:**

1.  **Monorepo with Cargo Workspace:** The entire project (backend, CLI, future API, future frontend) lives in a single repository, managed by a Cargo Workspace. This ensures dependency consistency and unified build tooling.
2.  **Database as the Golden Source:** A PostgreSQL database serves as the single, authoritative source of truth for all data and metadata. This decouples data storage from processing and guarantees data integrity.
3.  **Library-First Design:** All core business logic resides in a `sapflux-core` library crate. The CLI and future API are thin clients that call this library, ensuring that validation and processing logic is centralized and reusable.
4.  **Two-Tier Validation Strategy:**
    *   **Tier 1 (Ingestion Gate):** Raw sensor files are subject to strict *structural* validation upon ingestion to ensure they conform to a known, parsable schema. Invalid files are rejected.
    *   **Tier 2 (Application Layer):** Metadata (Deployments, Rules) is subject to *business logic* validation within the core library whenever it is created or modified.
5.  **Data-Driven Configuration:** Critical logic, such as DST transition rules, is stored as queryable data in the database rather than being hardcoded in the application.

---

### **II. Accomplishments to Date (What We've Completed)**

We have successfully built the complete foundational infrastructure and the entire "write path" for ingesting data.

**✅ Phase 0: Environment and Project Scaffolding**
*   **Monorepo Structure:** The full `sapflux/` directory structure with `crates/`, `db/`, `initial_metadata/`, and `scripts/` is in place.
*   **Cargo Workspace:** The workspace is fully configured with three member crates (`sapflux-core`, `sapflux-cli`, `sapflux-api`) and a centralized dependency manifest.
*   **Local Database Environment:** A `docker-compose.yml` provides a one-command setup for a local PostgreSQL 17 database.
*   **Database Schema:** The `db/schema.sql` file defines the initial tables for `raw_files` and `dst_transitions`.

**✅ Phase 1: Core Library Foundation**
*   **Centralized Error Handling:** A robust `PipelineError` enum has been implemented in `sapflux-core/src/error.rs` using `thiserror`.
*   **Core Data Types:** Rust structs representing database entities (`Deployment`, `DstTransition`, `SdiAddress`) are defined in `types.rs`, complete with database mapping (`sqlx::FromRow`) and domain-specific validation (the `SdiAddress` newtype).
*   **Database Connectivity:** The `sapflux-core/src/db.rs` module provides a reliable, async connection pool to the database, configured via a `.env` file.

**✅ Phase 2: Ingestion Pipeline**
*   **Schema Validation Framework:** A flexible `SchemaValidator` trait has been designed and implemented. We have created two concrete validators:
    *   `LegacySingleSensorValidator`: Correctly validates and accepts both CR200 and older CR300 single-sensor file formats.
    *   `CR300MultiSensorValidator`: Correctly validates the generic multi-sensor format for any number of sensors.
*   **Idempotent Ingestion Logic:** The `sapflux-core/src/ingestion.rs` module can ingest a file's raw byte content. It uses a SHA-256 hash to guarantee that identical files are never inserted twice, making the process robustly idempotent.
*   **Metadata Seeding:** A PEP 723-compliant Python script (`scripts/seed_database.py`) has been created to populate the `dst_transitions` table from a human-readable `dst_transitions.toml` file, establishing a clear pattern for managing initial metadata.
*   **Functional CLI:** The `sapflux-cli` application provides a working `ingest` command that successfully uses the core library to scan a directory and populate the database, gracefully handling and reporting files that fail validation.

**Current Status:** All validated raw data files can be successfully and reliably ingested into the PostgreSQL database, creating an immutable, auditable archive of our source data. The foundation is complete and stable.

---

### **III. Remaining Work (What We Have Left to Do)**

The remaining work focuses on building the "read path" for processing the data and implementing the management logic for our other metadata entities.

**🔜 Phase 3: Metadata Management**
*   **Goal:** Fully implement the database schema, seeding, and validation logic for `deployments`, `sensors`, and `parameters`.
*   **Tasks:**
    1.  **Finalize Schemas:** Update `db/schema.sql` with the final table designs for `projects`, `sensors`, and `deployments`, including foreign key relationships.
    2.  **Create Initial Data Files:** Create `deployments.csv`, `sensors.csv`, and `parameters.toml` in the `initial_metadata/` directory.
    3.  **Expand Seeding Script:** Add functions to `scripts/seed_database.py` to parse these new files and populate their respective tables.
    4.  **Implement Rust Types:** Add Rust structs to `types.rs` for `Project`, `Sensor`, and the `DeploymentAttributes` enum.
    5.  **Implement Validation Rules:** Create `rules/deployment_rules.rs` etc. in the core library to house the business logic for validating new or updated deployment records.

**🚀 Phase 4: The Processing Pipeline**
*   **Goal:** Implement the full data processing logic to transform the raw data from the database into a clean, corrected, and scientifically valuable Parquet file.
*   **Tasks:**
    1.  **Implement the "Read Path":** Write the `sapflux-core/src/processing.rs` logic to fetch blobs from the DB and create a master `LazyFrame`.
    2.  **Schema-Specific Parsing:** Make the `lazyframe_from_blob` function "smart" by using the `detected_schema_name` to apply the correct Polars parsing options (column names, types, etc.) for each file.
    3.  **Implement DST Correction Algorithm:** This is the most complex single task. Implement the three-pass strategy we designed:
        *   Pass 1: Use SQL to discover file relationships for each data point.
        *   Pass 2: Use Polars to assign a unique `chunk_id` to each row.
        *   Pass 3: Use the `dst_transitions` LUT to calculate and apply the correct timezone offset for each chunk.
    4.  **Implement Scientific Calculations:** Add the Polars expressions to calculate heat velocity, apply wound correction, and compute final sap flux density.
    5.  **Implement `process` CLI Command:** Flesh out the `sapflux-cli process` command to orchestrate the pipeline and save the final `LazyFrame`.

**未来 (Future): Application Layer**
*   **Goal:** Build user-facing applications on top of the core library.
*   **Tasks:**
    1.  **Build Axum API:** Create `sapflux-api` with endpoints for CRUD operations on metadata and for triggering/downloading processed datasets.
    2.  **Build Svelte Frontend:** Create a web interface for non-technical users to upload data and download versioned results.
    3.  **Deploy to Render.io:** Package the entire system for production deployment.