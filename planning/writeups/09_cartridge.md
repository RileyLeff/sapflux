## The Reproducibility Cartridge

The single most important promise of the Sapflux pipeline is not just reproducibility, but true, independent **verifiability**. The Reproducibility Cartridge is the feature that delivers on this promise. It is a self-contained, downloadable package that provides a user with everything they need to re-run the exact data processing pipeline that produced a specific output, on their own machine, and get a bit-for-bit identical result.

This transforms the pipeline from a "black box" into a transparent, auditable, and trustworthy scientific instrument.

### Core Components: What's Inside a Cartridge?

Each cartridge is a compressed archive (`.zip`) associated with a single `output_id`. It contains three categories of assets: orchestration files, application state, and data manifests.

**1. Orchestration Files (The "How-To")**
*   `docker-compose.yml`: A pre-configured file that defines the complete execution environment:
    *   `app`: The Sapflux application service.
    *   `db`: A PostgreSQL + PostGIS service.
    *   `minio`: An S3-compatible object store for local raw file access.
*   `Dockerfile`: A Dockerfile that uses a multi-stage build to check out the exact Git commit of the application and compile the Rust binary.
*   `run_repro.sh`: A simple, executable shell script that acts as the user's single entry point. It automates the entire verification process.

**2. Application State (The "Logic & History")**
*   `git_commit_hash.txt`: A text file containing the full Git commit SHA of the application code that performed the original run. This is used by the `Dockerfile` to ensure the exact same logic is recompiled.
*   `db_state_N-1.sql`: A `pg_dump` SQL file of the entire database state *immediately before* the triggering transaction (`N`) was applied. This file represents the complete historical context for the run.
*   `transaction_N.toml`: The original, complete Transaction Manifest file that triggered the run. This is the "event" that will be replayed.

**3. Data Manifests (The "Ingredients")**
*   `raw_files.manifest`: A simple text file listing the `file_hash` of every raw data file that was included in the pipeline run (`include_in_pipeline = true`).
*   `download_data.sh`: A helper script that reads `raw_files.manifest`, calls the Sapflux API to request pre-signed URLs for each `file_hash`, and downloads the objects into a local `data/` directory (volume-mounted into the MinIO container). This script reuses the user's CLI auth token to keep R2 private.

### Automated Generation Workflow

A new Reproducibility Cartridge is automatically generated and stored after every successful pipeline run.

1.  **Trigger**: The process begins after a pipeline `run` record is successfully marked as `'SUCCESS'`.
2.  **Gather Assets**: A backend process (e.g., a background job or CI/CD step) gathers all necessary components:
    *   It retrieves the `run_id`, `triggering_transaction_id`, and `git_commit_hash` from the `runs` table.
    *   It fetches the original manifest for transaction `N` from the `receipt` in the `transactions` table.
3.  **Reconstruct State**: This is the most critical step.
    *   The system programmatically reconstructs the state of the database at transaction `N-1`. This is achieved by replaying the entire transaction history from the beginning up to, but not including, transaction `N`.
    *   A `pg_dump` is performed on this reconstructed state to produce `db_state_N-1.sql`.
    *   It then queries this historical state to find all `raw_files` where `include_in_pipeline = true` to generate `raw_files.manifest`.
4.  **Package and Store**:
    *   The static files (`Dockerfile`, `docker-compose.yml`, etc.) are combined with the dynamically generated files (`db_state_N-1.sql`, etc.).
    *   The collection is packaged into a zip archive: `repro-cartridge-{output_id}.zip`.
    *   The archive is uploaded to the `repro-cartridges/` prefix in the object store.
    *   The `outputs` table is updated, setting the `reproducibility_cartridge_path` for the corresponding output.

### The User Experience: Verifying a Result

A scientist who wants to verify an output follows a simple, foolproof process:

1.  **Download**: Using the CLI or GUI, they download the data product with the cartridge.
    *   `sapflux outputs download <output_id> --with-cartridge`
2.  **Unpack**: They unzip the downloaded `repro-cartridge-{output_id}.zip` file.
3.  **Run**: They navigate into the directory and execute the single command: `bash run_repro.sh`.
4.  **Observe**: The script orchestrates the entire verification:
    *   It runs `docker compose up --build -d`. The `Dockerfile` checks out the correct commit and builds the application. The database container starts and automatically imports `db_state_N-1.sql`.
    *   It calls `bash download_data.sh` to populate the local data store. This step requires outbound internet access to reach the Sapflux API and Cloudflare R2 and uses the same Clerk-backed auth flow as the CLI.
    *   It executes the re-run command: `docker compose exec app sapflux transaction apply --file transaction_N.toml`.
5.  **Verify**: The process is complete. The user can now find the newly generated `.parquet` file in a local `output/` directory and perform a hash comparison (e.g., `sha256sum`) to prove it is bit-for-bit identical to the one they originally downloaded. They can also inspect the logs and the final database state to observe the entire process.

### Trust & Access Considerations

The cartridge never exposes R2 to the public internet. Instead, `download_data.sh` authenticates with the Sapflux API (using the user's Clerk session) and exchanges each `file_hash` for a short-lived pre-signed URL. Users must therefore possess active credentials and an internet connection when running `run_repro.sh`. If long-term offline verification is required, operators can pre-stage the raw files into the cartridge before distributing it.
