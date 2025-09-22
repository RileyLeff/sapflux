### A Thorough Description of the Timestamp Problem

The primary challenge in processing the raw datalogger files is that the `TIMESTAMP` column, while appearing straightforward, is fundamentally unreliable for three distinct and compounding reasons. The goal is to produce accurate, timezone-aware UTC timestamps, but a simple conversion is impossible without first solving these underlying data integrity issues.

The first and most critical problem is that **the timestamps do not represent a reliable chronological sequence**. The datalogger's internal clock is prone to erratic behavior. A battery replacement or power loss can cause the clock to reset to a default value, often the Unix epoch (`1970-01-01`), creating massive, incorrect jumps in the data. Furthermore, during field visits, the clock is manually synchronized to a technician's watch or phone, which can cause the timeline to jump forward or backward by hours or even days, breaking the continuity of the time series. This makes sorting the data by its `TIMESTAMP` column a flawed approach that is guaranteed to produce an incorrect ordering of events. The only reliable source of truth for the actual sequence of measurements is the `RECORD` number, a simple integer that the logger increments for every measurement cycle.

The second problem is that **a single raw data file does not represent a single, contiguous field visit**. Data download practices create ambiguity. A technician might perform a "download new" operation, which only retrieves data since their last visit. At a later date, they might perform a "download all" operation, which retrieves the logger's entire history, including the data from the previous visit. This results in a situation where a single measurement (e.g., `RECORD` number 500) can exist in multiple raw data files. This makes it impossible to treat a file as a "block" of time. The true "block" of data corresponding to an "implied visit" is defined by the unique signature of source files a measurement belongs to. For example, data belonging only to `file_B.dat` is from a different visit than older data that belongs to both `file_A.dat` *and* `file_B.dat`.

Finally, even if the clock were perfectly stable and the file origins were clear, the timestamps are **timezone-naive and do not account for Daylight Savings Time (DST)**. The logger's clock is set to the local time (e.g., Eastern Time) but has no concept of whether EST (UTC-5) or EDT (UTC-4) is in effect. It will continue recording in EST even after the local time has sprung forward to EDT, resulting in a silent, one-hour error in the data until the clock is manually adjusted on a subsequent visit. This requires a sophisticated correction that can determine the correct historical UTC offset for any given date and time in a specific geographic location.

## The Timestamp Correction Algorithm

### Goal

The primary goal of this algorithm is to convert the unreliable, timezone-naive timestamps recorded by dataloggers into true, fully-corrected, timezone-aware UTC timestamps. This is a critical, foundational step that enables all subsequent temporal analysis and metadata joining.

### The Problem (Detailed)

The core challenge stems from the nature of the datalogger hardware and data download practices:

1.  **Unreliable Timestamps for Ordering**: The logger's internal clock is not a reliable source for determining the chronological order of measurements. It can be reset during a battery change (often to the Unix epoch, `1970-01-01`), drift significantly, or jump forward or backward when manually synchronized during a field visit. A simple sort by timestamp is guaranteed to produce an incorrect sequence.
2.  **Timezone Naivete**: The clock does not account for Daylight Savings Time (DST). A logger set to EST (UTC-5) will continue recording in EST after the local time has changed to EDT (UTC-4), making the raw data inaccurate.
3.  **Ambiguous File Origins**: A single raw data file does not necessarily represent a single field visit. A "download all" operation will create a file containing data from many previous visits, overlapping with data in older "download new" files.

### The Solution: The "Implied Visit" Chunking Algorithm

The solution is a robust algorithm that uses a more reliable sequencing key and a clever grouping strategy to isolate data from a single "implied field visit."

**Core Principles:**

*   **`RECORD` is the True Sequence Key**: The `RECORD` number, a monotonically increasing integer from the logger, is the only reliable source for the true chronological order of measurements. All sorting must be performed on this column.
*   **The "First" Record is the Anchor**: The timestamp associated with the **lowest `RECORD` number** within a contiguous block of data from one visit is our anchor. This timestamp is the most likely to be correct, as it represents the moment the logger was configured during that visit.
*   **The "Chunk" is the Implied Visit**: We identify a unique "chunk" (an implied visit) by its signature: the combination of its **`logger_id`** and the sorted **set of `file_hash`es** that its measurements belong to. This elegantly untangles overlapping data from "download all" and "download new" files.

#### The Algorithm Steps

1.  **Gather & Combine**: The process begins after the ingest stage. The algorithm takes a list of all successfully parsed `ParsedFileData` objects for the current run. It iterates through them, creating a single, large logger-level `DataFrame` by vertically stacking the `df` from each object. As it does this, it adds the top-level `file_hash` from each object as a new column, ensuring every row is tagged with its origin file.
2.  **Identify Unique Chunks**: The combined `DataFrame` is deduplicated by `(logger_id, record)` so that each measurement appears exactly once. During this pass we gather the list of source `file_hash`es, sort them, and join them into a stable `file_set_signature` string.
3.  **Find the Anchor Timestamp for Each Chunk**: Within each chunk group, the algorithm sorts the rows by the `record` column in ascending order and takes the `timestamp` value from the very first row. This is the chunk's **anchor timestamp**.
4.  **Determine UTC Offset for Each Chunk**: For each unique chunk and its anchor timestamp, the algorithm performs a one-time lookup:
    *   It uses the `logger_id` and the `anchor_timestamp` to query the `deployments` and `sites` metadata tables.
    *   It retrieves the authoritative IANA timezone string (e.g., `"America/New_York"`).
    *   It uses the `chrono-tz` library to resolve the naive anchor timestamp within that timezone, which correctly handles all DST rules and returns the precise UTC offset in seconds (e.g., `-18000`).
5.  **Apply Offset**: This single calculated offset is now known to be valid for every row belonging to that chunk. The algorithm joins the `(chunk -> offset)` mapping back to the main `DataFrame` and performs a simple subtraction to calculate the final, corrected `timestamp_utc` for every row.

---

### Rust Reference Implementation

This code represents the `timestamp_fixer` component. It precisely follows the algorithm described above.

```rust
// filename: src/processing/timestamp_fixer.rs

use polars::prelude::*;
use chrono::NaiveDateTime;
use chrono_tz::Tz;
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

// ===================================================================
// Data Structures (Representing Inputs to the Component)
// ===================================================================

#[derive(Debug, Error)]
pub enum TimestampFixerError {
    #[error("Polars operation failed: {0}")]
    Polars(#[from] PolarsError),
    #[error("No active deployment found for logger '{logger_id}' at time '{timestamp}'")]
    NoActiveDeployment { logger_id: String, timestamp: NaiveDateTime },
    #[error("Site with ID '{0}' not found in provided metadata")]
    SiteNotFound(Uuid),
}

// Represents the canonical `sapflow_toa5_hierarchical_v1` data format.
pub struct ParsedFileData {
    pub file_hash: String,
    pub logger: LoggerData,
    // raw_text and file_metadata are omitted for this example's clarity.
}

pub struct LoggerData {
    pub df: DataFrame,
    // sensors vector is omitted for this example's clarity.
}

// Stubs for authoritative metadata queried from the database.
#[derive(Debug, Clone)]
pub struct Site {
    pub site_id: Uuid,
    pub timezone: Tz,
}

#[derive(Debug, Clone)]
pub struct Deployment {
    pub datalogger_id: String, // Corresponds to the standardized logger_id column
    pub site_id: Uuid,
    pub start_timestamp_utc: NaiveDateTime,
    pub end_timestamp_utc: Option<NaiveDateTime>,
}

// ===================================================================
// The Timestamp Fixer Component
// ===================================================================

/// Orchestrates the timestamp correction process. This is the primary entry point.
///
/// # Arguments
/// * `parsed_files` - A slice of all `ParsedFileData` objects from the current run.
/// * `sites` - A slice of all authoritative `Site` metadata.
/// * `deployments` - A slice of all authoritative `Deployment` metadata.
///
/// # Returns
/// A single, combined DataFrame with a corrected `timestamp_utc` column.
pub fn correct_timestamps(
    parsed_files: &[ParsedFileData],
    sites: &[Site],
    deployments: &[Deployment],
) -> Result<DataFrame, TimestampFixerError> {

    // 1. Gather & Combine: Create one large DataFrame from all parsed files,
    //    adding the `file_hash` as a column to tag each row with its origin.
    let combined_df = combine_parsed_files(parsed_files)?;

    // 2. Deduplicate by (logger_id, record) and compute each record's file-set signature.
    let annotated_df = annotate_with_file_sets(&combined_df)?;

    // 3. Identify chunks by grouping on (logger_id, file_set_signature) and
    //    take the timestamp associated with the lowest record number.
    let chunks = annotated_df.group_by(["logger_id", "file_set_signature"])?
        .agg([
            col("timestamp")
                .sort_by([col("record")], SortOptions::default())
                .first()
                .alias("anchor_timestamp"),
        ])?;

    // 4. Determine the UTC offset for each unique chunk.
    let site_map: HashMap<Uuid, &Site> = sites.iter().map(|s| (s.site_id, s)).collect();
    let deployment_map = build_deployment_map(deployments);
    
    let offsets_df = calculate_chunk_offsets(&chunks, &site_map, &deployment_map)?;

    // 5. Apply the offsets back to the main DataFrame.
    let result_with_offsets = annotated_df.join(
        &offsets_df,
        &[col("logger_id"), col("file_set_signature")],
        &[col("logger_id"), col("file_set_signature")],
        JoinArgs::new(JoinType::Left),
    )?;

    // Calculate the final UTC timestamp and clean up intermediate columns.
    let final_df = result_with_offsets.lazy()
        .with_column(
            (col("timestamp").cast(DataType::Int64) - col("utc_offset_seconds") * lit(1_000_000))
                .cast(DataType::Datetime(TimeUnit::Microseconds, Some("UTC".into())))
                .alias("timestamp_utc")
        )
        .drop_columns(["utc_offset_seconds", "file_set_signature"])
        .collect()?;

    Ok(final_df)
}

/// Helper to perform Step 1: Combine multiple ParsedFileData objects into one DataFrame.
fn combine_parsed_files(parsed_files: &[ParsedFileData]) -> Result<DataFrame, PolarsError> {
    if parsed_files.is_empty() {
        return Ok(DataFrame::default()); // Handle case with no input files
    }
    
    let dfs_with_hash: Result<Vec<DataFrame>, PolarsError> = parsed_files.iter().map(|pf| {
        pf.logger.df.clone().lazy()
            .with_column(lit(pf.file_hash.clone()).alias("file_hash"))
            .collect()
    }).collect();

    let mut combined_df = dfs_with_hash?[0].clone();
    for df in dfs_with_hash?.iter().skip(1) {
        combined_df.vstack_mut(df)?;
    }
    Ok(combined_df)
}

fn annotate_with_file_sets(df: &DataFrame) -> Result<DataFrame, PolarsError> {
    let aggregated = df
        .clone()
        .lazy()
        .groupby([col("logger_id"), col("record")])
        .agg([
            all().first(),
            col("file_hash")
                .unique()
                .alias("file_hashes"),
        ])
        .with_column(
            col("file_hashes")
                .arr
                .sort(SortOptions::default())
                .alias("file_hashes")
        )
        .with_column(
            col("file_hashes")
                .arr
                .join(lit("+"))
                .alias("file_set_signature")
        )
        .collect()?;

    aggregated.drop("file_hashes")
}

/// Helper to build a map for efficient deployment lookups.
fn build_deployment_map(deployments: &[Deployment]) -> HashMap<String, Vec<&Deployment>> {
    let mut map: HashMap<String, Vec<&Deployment>> = HashMap::new();
    for d in deployments {
        map.entry(d.datalogger_id.clone()).or_default().push(d);
    }
    map
}

/// Helper to perform Step 4: Calculate offsets for all chunks.
fn calculate_chunk_offsets(
    chunks: &DataFrame,
    site_map: &HashMap<Uuid, &Site>,
    deployment_map: &HashMap<String, Vec<&Deployment>>,
) -> Result<DataFrame, TimestampFixerError> {
    let mut logger_ids = StringChunkBuilder::new("logger_id", chunks.height());
    let mut file_signatures = StringChunkBuilder::new("file_set_signature", chunks.height());
    let mut offsets = Int32ChunkBuilder::new("utc_offset_seconds", chunks.height());

    let logger_id_ca = chunks.column("logger_id")?.str()?;
    let signature_ca = chunks.column("file_set_signature")?.str()?;
    let anchor_time_ca = chunks.column("anchor_timestamp")?.datetime()?;

    for i in 0..chunks.height() {
        let logger_id = logger_id_ca.get(i).unwrap();
        let file_signature = signature_ca.get(i).unwrap();
        let anchor_time = anchor_time_ca.get(i).unwrap();

        let offset = find_offset_for_chunk(logger_id, anchor_time, site_map, deployment_map)?;

        logger_ids.append_value(logger_id);
        file_signatures.append_value(file_signature);
        offsets.append_value(offset);
    }

    df!(
        "logger_id" => logger_ids.finish(),
        "file_set_signature" => file_signatures.finish(),
        "utc_offset_seconds" => offsets.finish(),
    ).map_err(TimestampFixerError::from)
}


/// Finds the correct UTC offset for a single chunk using its anchor timestamp.
fn find_offset_for_chunk<'a>(
    logger_id: &str,
    anchor_time: NaiveDateTime,
    site_map: &HashMap<Uuid, &'a Site>,
    deployment_map: &HashMap<String, Vec<&'a Deployment>>,
) -> Result<i32, TimestampFixerError> {
    let active_deployment = deployment_map
        .get(logger_id)
        .and_then(|deps| {
            deps.iter().find(|d| {
                anchor_time >= d.start_timestamp_utc
                    && d.end_timestamp_utc.map_or(true, |end| anchor_time < end)
            })
        })
        .ok_or_else(|| TimestampFixerError::NoActiveDeployment {
            logger_id: logger_id.to_string(),
            timestamp: anchor_time,
        })?;

    let site = site_map.get(&active_deployment.site_id)
        .ok_or(TimestampFixerError::SiteNotFound(active_deployment.site_id))?;
    
    use chrono::offset::LocalResult;
    let local_result = site.timezone.from_local_datetime(&anchor_time);

    let offset = match local_result {
        LocalResult::Single(dt) => dt.offset().fix().local_minus_utc(),
        LocalResult::Ambiguous(dt1, dt2) => std::cmp::max(dt1, dt2).offset().fix().local_minus_utc(),
        LocalResult::None => 0, // Fallback for the rare case of hitting the DST gap.
    };

    Ok(offset)
}
```
