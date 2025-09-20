Here is a description and code sketch of the daylight savings timestamp fixing algorithm based on the provided source code.

### Description of the Algorithm

The core purpose of the daylight savings time (DST) fixing algorithm is to convert the timezone-naive timestamps recorded by the dataloggers into proper UTC timestamps. This is necessary because the loggers are not aware of DST and only have their clocks synchronized during sporadic field visits. The algorithm correctly interprets whether a given period of data was recorded in Eastern Standard Time (EST, UTC-5) or Eastern Daylight Time (EDT, UTC-4) and applies the appropriate offset.

The logic is primarily implemented in Rust using the Polars data frame library, as seen in `crates/sapflux-core/src/processing/correction.rs`, and is described in plain English in `README.md`.

#### The Problem

As outlined in the `README.md`, the dataloggers do not automatically adjust for DST. Their clocks are set during field visits, and they continue to record in that timezone until the next visit. This means a data file might contain timestamps recorded in EST even after the local time has shifted to EDT, or vice-versa.

#### The Solution: "Chunk"-Based Correction

The algorithm's central concept is the "chunk." A chunk is defined as a contiguous block of data recorded between two field visits. The critical assumption is that all data within a single chunk was recorded in the same, consistent time zone.

The algorithm identifies these chunks, determines the correct timezone for each one based on its starting time, and then applies the proper UTC offset.

#### Algorithm Steps:

1.  **Identify Unique Chunks:** The system cleverly identifies data chunks without relying on inconsistent file start times. As detailed in `README.md` and implemented in `correction.rs`, it does this by finding unique sets of source files. For every recorded timestamp, it determines which original raw data file(s) it appeared in (identified by `file_hash`). Each unique combination of file hashes represents a distinct data "chunk," which corresponds to the data downloaded during a single field visit. A unique `chunk_id` is assigned to each of these chunks.

NOTE FROM RILEY: remember that timestamps are logger-level data. A chunk is not just a contiguous set of timesteps with a shared, unique set of origin files -- it's a contiguous set of timesteps with a shared, unique set of origin files FROM THE SAME LOGGER.

2.  **Determine Timezone for Each Chunk:** For each chunk, the algorithm finds the earliest timestamp (`chunk_start_time`). It then uses a predefined list of DST transition dates (stored in the `dst_transitions` table, populated from `initial_metadata/dst_transitions.toml`) to determine the correct timezone.


NOTE FROM RILEY: You said there was a better way to do this with chrono-tz?

Using a powerful Polars feature called an "as-of join," it finds the most recent DST transition that occurred *before* the chunk's start time.

3.  **Assign UTC Offset:** Based on the last transition event, the algorithm assigns the correct UTC offset to the entire chunk:
    *   If the last event was a DST "start" (spring forward), the offset is **-04:00** (EDT).
    *   If the last event was a DST "end" (fall back), the offset is **-05:00** (EST).

4.  **Convert to UTC:** Finally, the algorithm joins the calculated UTC offset back to the main dataset and uses it to convert every naive timestamp within the chunk into a fully qualified UTC timestamp. This creates the final, corrected `timestamp_utc` column.

### Code Sketch

This sketch is a simplified representation of the Polars logic found in `crates/sapflux-core/src/processing/correction.rs`.

```
// Load the necessary data into lazy DataFrames
unified_lf <- load_all_parsed_data_from_db()  // Columns: logger_id, timestamp_naive, file_hash, ...
dst_rules_lf <- load_dst_transitions_from_db() // Columns: ts_local, transition_action

// 1. Identify and define unique data "chunks"
chunk_definitions_lf = unified_lf
    // For each unique timestamp, get a sorted list of the files it came from
    .group_by(["logger_id", "timestamp_naive"])
    .agg(
        col("file_hash").list().sort().alias("file_hashes")
    )
    // Each unique list of file hashes defines a chunk
    .group_by(["file_hashes"])
    .agg([]) // No aggregation needed, just want the unique groups
    .with_row_index("chunk_id") // Assign a unique ID to each chunk

// Join the chunk_id back to the main dataset
data_with_chunks_lf = unified_lf.join(
    chunk_definitions_lf,
    on=["logger_id", "timestamp_naive"]
)

// 2. Determine the UTC offset for each chunk
chunk_offsets_lf = data_with_chunks_lf
    // Find the earliest timestamp for each chunk
    .group_by(["chunk_id"])
    .agg(
        col("timestamp_naive").min().alias("chunk_start_time")
    )
    // Find the last DST transition that occurred before the chunk started
    .join_asof(
        dst_rules_lf,
        on="chunk_start_time",
        by_left="chunk_start_time",
        by_right="ts_local",
        strategy="backward" // 'backward' finds the last rule before the timestamp
    )
    // Assign the correct UTC offset based on the transition type
    .with_column(
        when(col("transition_action") == "start")
        .then(lit("-04:00")) // EDT
        .otherwise(lit("-05:00")) // EST
        .alias("utc_offset")
    )
    .select(["chunk_id", "utc_offset"])

// 3. Apply the correction to generate final UTC timestamps
final_corrected_lf = data_with_chunks_lf
    // Join the calculated offset back to the full dataset
    .join(
        chunk_offsets_lf,
        on=["chunk_id"]
    )
    // Create the final timestamp_utc column
    .with_column(
        // Concatenate naive time string with offset string and parse as a datetime
        (col("timestamp_naive").to_string() + col("utc_offset"))
        .to_datetime()
        .alias("timestamp_utc")
    )

// The `final_corrected_lf` now contains the corrected timestamps.
```