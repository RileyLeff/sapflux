## Processing and Calculations

Once a raw file has been successfully ingested and transformed into a structured **Data Format**, it enters the processing stage. This stage is responsible for applying all the necessary corrections, enrichments, and scientific calculations to produce the final, analysis-ready dataset.

This entire process is managed by a **Processing Pipeline**, a versioned, compiled-in Rust component that executes a defined sequence of steps.

### The Processing Pipeline Architecture

A Processing Pipeline is a self-contained component that accepts a specific `DataFormat` as input and, if successful, produces a final Polars `DataFrame`. The system is designed to be modular, allowing internal steps like timestamp correction and calculation to be reused across different pipeline implementations.

#### The Execution Context

To avoid passing numerous arguments and to provide shared resources, the pipeline is provisioned with an **`ExecutionContext`**. This simple struct holds long-lived services, primarily the database connection pool, that the pipeline needs to perform its work.

**Rust Code (`execution_context.rs`)**
```rust
use sqlx::PgPool; // Or your database connection pool type

/// Contains shared resources needed for a pipeline run.
pub struct ExecutionContext {
    pub db_pool: PgPool,
    // Other resources like a logger could be added here.
}
```

#### The Updated `ProcessingPipeline` Trait

The `ProcessingPipeline` trait is updated to accept this context, giving it the tools it needs to interact with the database.

**Rust Trait Definition**
```rust
use polars::prelude::*;
use crate::errors::ProcessingError;
use crate::model::ParsedData;
use crate::execution_context::ExecutionContext;

pub trait ProcessingPipeline: Send + Sync {
    fn code_identifier(&self) -> &'static str;

    /// Runs the pipeline for the entire batch of successfully parsed files
    /// belonging to a single transaction.
    fn run_batch(
        &self,
        context: &ExecutionContext,
        parsed_batch: &[&dyn ParsedData],
    ) -> Result<DataFrame, ProcessingError>;
}
```

#### The Orchestrator's Role

The main application engine contains an "orchestrator" function that manages the high-level execution. It is responsible for creating the `ExecutionContext`, looking up the correct pipeline from the database, and provisioning the context to it. The orchestrator knows *what* to run, but the pipeline itself knows *how* to run it.

---

### Canonical Pipeline: `standard_v1_dst_fix`

The default processing pipeline for the `sapflow_toa5_hierarchical_v1` data format is named `standard_v1_dst_fix`. Its `run` method orchestrates a sequence of calls to its internal, reusable components.

**Conceptual Pipeline Implementation (`standard_v1_dst_fix.rs`)**```rust
fn run_batch(
    &self,
    context: &ExecutionContext,
    parsed_batch: &[&dyn ParsedData],
) -> Result<DataFrame, ProcessingError> {
    // Cast each ParsedData object to the concrete type this pipeline expects.
    let parsed_files: Vec<&ParsedFileData> = parsed_batch
        .iter()
        .map(|parsed| {
            parsed
                .as_any()
                .downcast_ref::<ParsedFileData>()
                .ok_or_else(|| ProcessingError::IncorrectDataFormat)
        })
        .collect::<Result<_, _>>()?;

    // --- Execute Components Sequentially ---

    // 0. Flatten the hierarchical data into a single observation frame.
    let flattened = flatten::thermistor_observations_batch(&parsed_files)?;

    // 1. Correct timestamps using the database for timezone info.
    let mut df = timestamp_fixer::correct_timestamps(context, &parsed_files, flattened)?;

    // 2. Enrich data by joining with deployment metadata from the database.
    df = metadata_enricher::enrich_with_metadata(context, &df)?;

    // 3. Resolve all calculation parameters using the database and the cascade logic.
    df = parameter_resolver::resolve_parameters(context, &df)?;

    // 4. Perform the final calculations on the fully prepared DataFrame.
    df = calculator::perform_dma_peclet_calculations(&df)?;

    // 5. Run canonical quality filters and flag suspect records.
    df = quality_filters::apply(&df)?;

    Ok(df)
}
```

#### Step 0: Flatten Hierarchical Data

`ParsedFileData` contains a logger-level DataFrame plus nested sensor/thermistor tables. The first pipeline operation normalises the entire batch into one wide DataFrame where each row represents `(timestamp, record, logger_id, sdi12_address, thermistor_depth)`. The flattening logic:

* Carries forward logger-level columns (timestamp, record, battery voltage, etc.).
* Adds sensor metadata (SDI-12 address) and thermistor metadata (inner/outer depth) per row.
* Joins all thermistor metrics (`alpha`, `beta`, `time_to_max_*`, temperature readings, etc.) into the same row.

This satisfies the row-level convention defined in `notes/data_column_conventions.md` and gives every subsequent component a uniform table to work with.

#### Step 1: Timestamp Correction (Component)

*   The timestamp fixer receives the full batch of `ParsedFileData` references plus the flattened frame so it can deduplicate by `(logger_id, record)` across files, compute the sorted file-set signature, and apply timezone correction using deployment metadata. The resulting DataFrame has a single, consistent `timestamp_utc` column and no duplicate measurements.

#### Step 2: Metadata Enrichment (Component)

*   This component uses the `ExecutionContext` to perform a temporal join between the measurement data (using `timestamp_utc`) and the `deployments` database table. It attaches the full metadata context (`deployment_id`, `stem_id`, etc.) to each row.

#### Step 3: Parameter Resolution (Component)

This component is responsible for implementing the Parameter Cascade. It uses the database to find all applicable parameters for every row in the dataset.

*   **Input**: The wide, enriched `DataFrame` containing full metadata keys for each row.
*   **Process**:
    1.  **Fetch Overrides**: The component performs a single, efficient query using the `ExecutionContext` to fetch *all* records from the `parameter_overrides` table.
    2.  **Prepare for Joins**: It transforms this list of overrides into several Polars `DataFrames`, one for each context level (e.g., `site_overrides_df`, `stem_overrides_df`).
    3.  **Join Overrides**: It performs a series of left joins, attaching the override values for every parameter at every level to the main `DataFrame`. This results in many new, nullable columns (e.g., `wound_diameter_cm_site`, `wound_diameter_cm_stem`). JSONB payloads are deserialized into strongly-typed scalars before they are projected into the output frame.
    4.  **Apply Cascade**: For each required parameter, it uses a Polars `when/then/otherwise` expression to coalesce these columns in the correct order of precedence (deployment -> stem -> ... -> global default). Calculation parameters are emitted with the `parameter_*` prefix (e.g., `parameter_wood_density_kg_m3`). Quality thresholds retain their own names (e.g., `quality_max_flux_cm_hr`) because they are surfaced directly to operators.
    5.  **Record Provenance**: A second `when/then/otherwise` expression is used to create provenance columns. Calculation parameters receive `parameter_source_*` companions, while quality thresholds receive `parameter_source_quality_*` columns.
*   **Output**: A `DataFrame` where all necessary parameters—including quality thresholds—are now present as distinct columns, each with provenance.

#### Step 4: Calculation (Component)

This final component is "pure" and decoupled from the database. It performs the scientific calculations.

*   **Input**: The `DataFrame` from the previous step, which now contains all measurements and all resolved parameters.
*   **Process**: It applies the DMA_Péclet mathematical formulas using Polars expressions. It reads from columns like `alpha`, `beta`, and the `parameter_*` columns and produces the final output columns.
*   **Output**: The final, fully calculated `DataFrame`.

#### Step 5: Quality Filtering (Component)

The last step applies declarative quality rules and surfaces them to end users.

*   **Input**: The fully calculated `DataFrame` (which includes `parameter_*` columns).
*   **Process**:
    *   Each canonical threshold is parameterised via the same override cascade and uses JSONB values with agreed codes such as `quality_max_flux_cm_hr`, `quality_min_flux_cm_hr`, and `quality_gap_years`.
    *   Checks include deployment window bounds, time-travel gaps, and physiologically implausible flux magnitudes.
    *   Rows that fail any rule receive `quality = "SUSPECT"` and a human-readable `quality_explanation` (e.g., `"sap_flux_density_j_dma_cm_hr > 40 cm/hr"`). Multiple failures are concatenated in a deterministic order.
*   **Output**: The same `DataFrame` with `quality` and `quality_explanation` columns populated (left null when data is good).

---

### The Final Output DataFrame

To ensure full transparency, the final `DataFrame` produced by the pipeline contains not only the result but also crucial intermediate values and provenance information.

**Key Output Columns:**
*   `timestamp_utc`: The corrected, timezone-aware timestamp.
*   All metadata keys: `deployment_id`, `project_code`, `site_code`, `plant_code`, etc.
*   `sap_flux_density_cm_hr`: The final calculated sap flux density.
*   `calculation_method_used`: A string (`"HRM"` or `"Tmax"`) indicating which branch of the algorithm was used.
*   `beta`: The primary decision metric for the algorithm.
*   `heat_velocity_corrected_vc`: The wound-corrected heat velocity.
*   `peclet_number`: The calculated Péclet number.
*   `parameter_*`: A column for each parameter used (e.g., `parameter_wound_diameter_cm`).
*   `parameter_source_*`: A corresponding column for each parameter indicating its origin in the cascade (e.g., `parameter_source_wound_diameter_cm: "stem_override"`).

This rich output allows a scientist to not only see the result but to understand exactly *how* it was derived.
