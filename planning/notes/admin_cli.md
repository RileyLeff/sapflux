### 1. Database Seeding via a Dedicated Admin CLI

To initialize the pipeline's configuration tables (`parsers`, `data_formats`, `processing_pipelines`), a dedicated, non-public administrative tool will be used. This approach was chosen to satisfy two core principles:

1.  **Architectural Purity**: The primary `sapflux` end-user CLI is a pure client of the API and contains no stack administration logic.
2.  **Single Source of Truth**: The database is seeded directly from the list of components compiled into the application, preventing configuration drift and manual synchronization errors.

**Implementation Plan:**

*   **Rust Workspace Structure**: The project will be structured as a Rust workspace with a shared core library and two separate binaries:
    *   `sapflux-core` (library): Contains the core application logic, database models, and the registries of all compiled-in `Parser` and `ProcessingPipeline` components.
    *   `sapflux` (binary): The primary, end-user CLI and API server.
    *   `sapflux-admin` (binary): A small, separate CLI tool for administrative tasks.

*   **`sapflux-admin` Functionality**: The `sapflux-admin` binary will have a single command, `db-seed`. When executed, its logic will:
    1.  Read database connection details from environment variables.
    2.  Import the component registries from the shared `sapflux-core` library.
    3.  Connect to the database.
    4.  Iterate through the compiled-in components and insert records for any that are not yet present in the database.

This standalone tool provides a robust, programmatic way to initialize and update the system's configuration based on the exact capabilities of the compiled code.