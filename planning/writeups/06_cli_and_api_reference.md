# `docs/06_cli_and_api_reference.md`

## CLI and API Reference

This document provides a high-level reference for the two primary user interaction surfaces of the Sapflux pipeline: the Application Programming Interface (API) and the Command-Line Interface (CLI).

**Note:** This is an architectural overview, not a final, exhaustive specification. Detailed request/response schemas, error codes, and CLI command flags will be finalized after the core architecture has been implemented.

### Core Concepts

All interactions that modify the state of the pipeline are performed through the **Transaction** model. Users construct a **Transaction Manifest** (a `.toml` file) that declaratively describes the desired changes. This manifest is then submitted to the API, which processes it as a single, atomic operation. For more detail, see the `03_transaction_workflow.md` document.

### Authentication

The entire authentication and authorization model for the Sapflux pipeline is built upon and delegated to Clerk. Clerk serves as the single source of truth for user identity, session management, and role-based access control. The Sapflux application itself never handles or stores user passwords or other sensitive credentials. The API acts as a protected resource server that requires a short-lived JSON Web Token (JWT) issued by Clerk in the Authorization: Bearer <TOKEN> header of every authenticated request. Upon receiving a request, the API cryptographically verifies the token and uses Clerk's backend SDK to confirm its validity, thereby authenticating the user. Both the Web GUI and the CLI act as clients in this model. The Web GUI integrates Clerk's frontend components for a seamless sign-in experience, while the CLI uses a browser-based flow initiated by the sapflux login command to securely acquire a token.

---

### API Reference

The API is the unified entry point for both the CLI and the web GUI.

#### **Primary Endpoint: Transactions**

This is the single, powerful endpoint for all data and metadata changes.

*   **Endpoint**: `POST /transactions`
*   **Description**: Submits a Transaction Manifest for processing. The entire operation is atomic: it will either be fully `ACCEPTED` (possibly with partial file success) or fully `REJECTED`. The API handles file ingestion, validation of all operations, and committing the changes.
*   **Query Parameters**:
    *   `?dry_run=true` (optional): Runs the entire validation pipeline (including parsing) but skips all database mutations. Dry runs return a full receipt and structured log entries, yet they do **not** insert records into the immutable `transactions` table.
*   **Request Body**: The request must be `multipart/form-data` to handle both the manifest and associated file uploads.
    *   `manifest`: The content of the `manifest.toml` file. Add blocks currently exist for `projects`, `sites`, `zones`, `plots`, `species`, `plants`, `stems`, `datalogger_types`, `dataloggers`, `datalogger_aliases`, `sensor_types`, `sensor_thermistor_pairs`, plus the existing `deployments` and `parameter_overrides`. Geometry-bearing fields expect GeoJSON; site timezones must be valid IANA identifiers.
    *   `file_1`, `file_2`, ...: The raw content of each data file referenced in the manifest. These parts are optional — metadata-only transactions simply omit file fields. The CLI maps the relative file paths declared in the manifest to these form entries.

**Example manifest payload (abridged):**

```toml
message = "Seed TEST site and register CR300 logger"

[[projects.add]]
code = "TEST"
name = "Example Project"

[[sites.add]]
code = "TEST_SITE"
timezone = "America/New_York"
boundary = { type = "Polygon", coordinates = [[[ -105.0, 39.0 ], [ -105.0, 39.1 ], [ -104.9, 39.1 ], [ -104.9, 39.0 ], [ -105.0, 39.0 ]]] }

[[plants.add]]
site_code = "TEST_SITE"
zone_name = "Zone A"
plot_name = "Plot 1"
species_code = "SPEC"
code = "PLANT1"
location = { type = "Point", coordinates = [ -104.98, 39.02 ] }

[[datalogger_aliases.add]]
datalogger_code = "LOGGER42"
alias = "ALIAS42"
start_timestamp_utc = "2024-01-01T00:00:00Z"
end_timestamp_utc = "2024-12-31T00:00:00Z"

[[parameter_overrides]]
parameter_code = "parameter_heat_pulse_duration_s"
value = 3.0
site_code = "TEST_SITE"
```
*   **Success Response**: Returns `200 OK` with a JSON body containing the detailed Transaction Receipt. The `outcome` field in the receipt will be `ACCEPTED`. When individual files fail, the receipt's `summary.status` is `PARTIAL_SUCCESS`, and each entry in `rejected_files` includes the `file_hash`, `parser_attempts`, and `first_error_line` fields to aid triage.
*   **Error Response**: Returns a `400 Bad Request` (for validation errors) or `500 Internal Server Error`. The response body will still be the JSON Transaction Receipt, but the `outcome` field will be `REJECTED`, and an `error` field will provide details.

---

#### **Data Access Endpoint**

*   **Endpoint**: `GET /outputs/{output_id}/download`
*   **Description**: Downloads a final, processed data product.
*   **Path Parameters**:
    *   `{output_id}`: The UUID of the output to download.
*   **Query Parameters**:
    *   `?include_cartridge=true` (optional): If `true`, the response will be a `.zip` archive containing both the `.parquet` data file and its corresponding "Reproducibility Cartridge." If `false` or omitted, only the `.parquet` file is returned.
*   **Authentication**: Requires Bearer Token.
*   **Success Response**:
    *   Returns `200 OK` with a JSON payload describing the pre-signed URL, or a `302 Found` redirect to Cloudflare R2. The link expires after 15 minutes and can be regenerated on demand.
    *   Clients download the object directly from R2, keeping the bucket private and avoiding credential distribution.

---

#### **Utility Endpoints**

These endpoints will be included for system health and GUI support.

*   `GET /health`: An unauthenticated endpoint that checks the health of the API and its connection to the database. Returns `200 OK` if healthy.
*   `GET /assets/{...}`: An authenticated endpoint for serving static assets like icons from the object storage bucket, used by the web GUI.

---

### CLI Reference

The CLI provides a powerful and scriptable way to interact with the API. The command structure follows a `sapflux <NOUN> <VERB>` pattern.

#### **Authentication**

*   `sapflux login`: Initiates the authentication process by opening a web browser for the user to sign in via Clerk. Securely stores the resulting token for subsequent commands.
*   `sapflux logout`: Clears the stored authentication token.

#### **Transaction Workflows**

The CLI supports two primary workflows for submitting transactions.

**1. Declarative, File-Based Workflow (Recommended for Reproducibility)**
```bash
# Apply a complete, self-contained transaction manifest.
sapflux transaction apply --file path/to/manifest.toml [--dry-run]
```

**2. Interactive, Stateful Workflow**
```bash
# 1. Start a new transaction and create a local manifest file.
sapflux transaction new --message "Initial setup for the BNWR site."

# 2. Add operations to the manifest.
sapflux sites add --code "BNWR" --name "Blackwater NWR" --timezone "America/New_York"
sapflux data add ./rawdata/BNWR/*.dat

# 3. Push the completed manifest and its files to the API.
sapflux transaction push [--dry-run]
```

#### **Data & Metadata Management (Examples)**

This is a representative list. A command will exist for every resource type (`sites`, `zones`, `plots`, `projects`, etc.).
```bash
# Add a new project.
sapflux projects add --code "MONITORING" --name "Long-Term Forest Monitoring"

# Update an existing site (uses a selector to find the record).
sapflux sites update --selector.code "BVL" --patch.name "Bivens Arm, Updated Name"

# List all known species.
sapflux species list [--format json]
```

#### **Data & Output Management**

```bash
# List all available outputs.
sapflux outputs list [--latest]

# Download an output file to the current directory.
sapflux outputs download <output_id> [--with-cartridge] [--output-path ./data/]
```

#### **Utility Commands**

```bash
# View the transaction history.
sapflux log [--limit 20]

# Check the status of the remote API.
sapflux status
```
