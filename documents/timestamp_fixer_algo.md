# Timestamp Correction Algorithm

This document describes the algorithm for converting the timezone-naive timestamps recorded by dataloggers into true, timezone-aware UTC timestamps. This correction is a critical processing step that ensures all data in the pipeline is standardized and accurate.

This is intended to be a component available for processing pipelines that are compatible with the data format we're starting with.

## 1. The Problem: Timezone-Naive Dataloggers

The core problem stems from the datalogger hardware:
*   **No DST Awareness**: The loggers' internal clocks do not automatically adjust for Daylight Savings Time (DST).
*   **Sporadic Synchronization**: The clocks are only synchronized to the local time during infrequent field visits.

This means a datalogger set to Eastern Time in the winter (EST, UTC-5) will continue to record timestamps in EST even after the local time has sprung forward to EDT (UTC-4). The raw data is therefore "timezone-naive"—it's a series of timestamps without the crucial context of which UTC offset was in effect when they were recorded.

## 2. The Solution: Metadata-Driven Correction with `chrono-tz`

The solution is to reconstruct the missing timezone context for every data point by combining the raw data with our rich metadata. The algorithm is based on two key components:

#### A. The "Chunk" Concept

A "chunk" is a contiguous block of timestamps from a single logger that was recorded between two field visits. Since the logger's clock is only set during a visit, we can make a critical assumption: **all timestamps within a single chunk were recorded with the same, consistent UTC offset.**

The system cleverly identifies these chunks by finding unique sets of source files. For every timestamp, it determines which original raw data file(s) it appeared in (identified by `file_hash`). Each unique combination of file hashes for a given logger represents a distinct data "chunk."

#### B. The Role of `chrono-tz`

Instead of relying on a fragile, hardcoded table of DST transition dates, the algorithm uses the **`chrono-tz`** crate. This library provides a Rust interface to the official **IANA Time Zone Database**, which is the global standard for time zone information. It contains all historical and future DST rules for locations around the world.

**`chrono-tz` completely replaces the need for a `dst_transitions` table.**

Its core function is to take two pieces of information:
1.  A **timezone-naive timestamp** (a `chrono::NaiveDateTime`).
2.  An **IANA timezone name** (e.g., `"America/New_York"`, a `chrono_tz::Tz`).

...and correctly determine the true UTC offset for that exact moment in time in that specific location. It automatically knows whether EDT or EST was in effect, and it gracefully handles the tricky edge cases of the "skipped hour" in the spring and the "ambiguous hour" in the fall.

## 3. Algorithm Steps

The algorithm uses `chrono-tz` in conjunction with the metadata tables to determine the correct offset for each chunk and apply it.

1.  **Fetch Authoritative Metadata**:
    *   Load all `sites` from the database to get the IANA `timezone` for each site.
    *   Load all active `deployments` to establish the link between a `logger_id`, a `site_id`, and a time range.

2.  **Identify Unique Chunks**:
    *   For the incoming dataset, group the data by `logger_id` and the unique set of `file_hash`es each timestamp belongs to.
    *   Assign a unique `chunk_id` to each of these groups.

3.  **Determine UTC Offset for Each Chunk**: This is the core of the logic. For each unique `chunk_id`:
    *   Find the **earliest timestamp** in the chunk. This represents the approximate time of the field visit when the clock was synchronized.
    *   Find the `logger_id` for the chunk.
    *   Using the `logger_id` and the chunk's start time, look up the active `deployment` record.
    *   From the `deployment` record, get the `site_id`.
    *   Using the `site_id`, look up the IANA `timezone` string (e.g., `"America/New_York"`).
    *   Parse this string into a `chrono_tz::Tz` object.
    *   **Use `chrono-tz` to find the offset**: Pass the naive start timestamp and the `Tz` object to `chrono-tz`. The library resolves the local time to a fully timezone-aware `DateTime` and reveals the correct UTC offset (e.g., -14400 seconds for EDT, -18000 seconds for EST).
    *   Store this offset. It is now considered valid for every timestamp within this chunk.

4.  **Apply Offset and Convert to UTC**:
    *   For every timezone-naive timestamp in the main dataset, find the UTC offset associated with its `chunk_id`.
    *   Subtract the offset from the naive timestamp to produce the final, corrected `timestamp_utc`.

## 4. Implementation Sketch

This sketch reflects the logic found in `crates/sapflux-processing/src/timestamp_fix.rs`.

```rust
use chrono::{Duration as ChronoDuration, NaiveDateTime};
use chrono_tz::Tz;
use polars::prelude::*;
use sapflux_repository::metadata::{Deployment, Site};
use std::collections::HashMap;

// The main entry point.
pub async fn apply(
    parsed_data: ParsedFileData,
    sites: &[Site],
    deployments: &[Deployment],
) -> Result<ParsedFileData, Error> {
    // Step 1: Build lookup maps from the authoritative metadata.
    let site_timezones = build_site_timezone_map(sites)?; // HashMap<SiteId, Tz>
    let logger_deployments = build_logger_deployments(deployments); // HashMap<LoggerId, Vec<Deployment>>

    // ... Polars logic to perform Step 2 (Identify Chunks) ...
    // This involves grouping by logger_id and file_hash sets to create chunk_ids.

    // Step 3: Determine the offset for each chunk.
    let mut chunk_offsets: HashMap<u32, i32> = HashMap::new();
    for chunk_id in all_chunk_ids {
        let chunk_start_naive: NaiveDateTime = get_first_timestamp_for_chunk(chunk_id);
        let logger_id: &str = get_logger_id_for_chunk(chunk_id);

        let offset_seconds = determine_offset(
            logger_id,
            chunk_start_naive,
            &logger_deployments,
            &site_timezones,
        )?;
        chunk_offsets.insert(chunk_id, offset_seconds);
    }

    // Step 4: Apply the offset to all timestamps in the DataFrame.
    // This uses Polars expressions to apply the `chunk_offsets` map to the timestamp column.
    // final_df = df.with_column(
    //    (col("timestamp_naive") - col("chunk_id").map(chunk_offsets)).alias("timestamp_utc")
    // );

    Ok(final_df)
}

// This function finds the correct timezone for a given logger at a given time.
fn determine_offset(
    logger_id: &str,
    chunk_start: NaiveDateTime,
    logger_deployments: &HashMap<String, Vec<Deployment>>,
    site_timezones: &HashMap<Uuid, Tz>,
) -> Result<i32, Error> {
    // Find the deployment that was active for this logger at the start of the chunk.
    if let Some(deployment) = find_active_deployment(logger_deployments, logger_id, chunk_start) {
        // Get the IANA timezone for that deployment's site.
        if let Some(tz) = site_timezones.get(&deployment.site_id) {
            // Use chrono-tz to get the correct offset. This is the key step.
            return offset_from_timezone(tz, chunk_start);
        }
    }
    // Fallback if no matching deployment is found.
    Ok(0)
}

// This is where chrono-tz does the heavy lifting.
fn offset_from_timezone(tz: &Tz, naive: NaiveDateTime) -> Result<i32, Error> {
    use chrono::offset::LocalResult;

    // Ask chrono-tz to resolve the naive timestamp.
    let local_result = tz.from_local_datetime(&naive);

    let datetime = match local_result {
        // The timestamp is unambiguous (most of the year).
        LocalResult::Single(dt) => dt,
        // The timestamp occurred during the fall-back "ambiguous hour".
        // We choose one based on a consistent rule (e.g., prefer standard time).
        LocalResult::Ambiguous(dt1, dt2) => std::cmp::max_by_key(dt1, dt2, |d| d.offset().fix()),
        // The timestamp occurred during the spring-forward "skipped hour".
        // chrono-tz correctly reports that this local time never existed. We handle it.
        LocalResult::None => { /* ... logic to handle the gap ... */ }
    };

    // Return the determined offset in seconds (e.g., -14400 for EDT).
    Ok(datetime.offset().fix().local_minus_utc())
}
```

## 5. Critical Assumptions

This improved algorithm relies on the quality of the metadata rather than hardcoded rules. The key assumptions are:
*   The `timezone` field in the `sites` table contains a valid IANA timezone name.
*   The `deployments` table accurately reflects which logger was at which site during a given time period.
*   Field personnel synchronize the logger's clock to the correct local time during every site visit.