This is an impressive and well-documented project. The `README.md` is exceptionally thorough and provides excellent context for the scientific domain, the hardware limitations, and the data processing goals. This level of documentation is rare and commendable. The Rust code is also well-structured and tackles a complex set of problems (especially the DST correction and varied data formats) in a robust way.

Here are my suggestions for improvement, organized by topic.

### High-Level Strategic Suggestions

1.  **Centralize Calculation Logic:** The "source of truth" for sap flux calculations is currently implemented within `data_pipeline.rs` using Polars expressions. The separate `calculations/sap_flux.rs` file contains a slightly different, row-by-row implementation that seems unused by the main pipeline and has some confusing logic (e.g., wound correction coefficients).
    *   **Recommendation:** Either remove `calculations/sap_flux.rs` to avoid confusion, or refactor it to be a "pure" calculation library that is then called by the Polars pipeline (e.g., via `map` or `apply` UDFs). This would centralize the scientific formulas in one place. My preference would be to keep the Polars expressions in the pipeline for performance but ensure they are the *only* implementation.

2.  **Improve Deployment Matching Performance:** The temporal deployment matching in `data_pipeline.rs` is implemented by iterating over every data row and searching through the available deployments. This is a classic `O(N*M)` operation that will become very slow with large datasets.
    *   **Recommendation:** Re-implement this using a Polars `join_asof`. This is *exactly* the kind of operation it's designed for and will be orders of magnitude faster.

    Here's a sketch of how you could do it:

    ```rust
    // In data_pipeline.rs -> apply_temporal_matching

    // 1. Create a DataFrame from your deployments
    let mut deployment_df = df!{
        "logger_id" => self.deployments.iter().map(|d| d.hardware.datalogger_id).collect::<Vec<_>>(),
        "sdi_address" => self.deployments.iter().map(|d| d.hardware.sdi_address.0.clone()).collect::<Vec<_>>(),
        "start_time_utc" => self.deployments.iter().map(|d| d.start_time_utc.timestamp_millis()).collect::<Vec<_>>(),
        // ... other deployment metadata ...
        "tree_id" => self.deployments.iter().map(|d| d.measurement.tree_id.clone()).collect::<Vec<_>>(),
    }?;
    deployment_df = deployment_df.lazy()
        .with_column(col("start_time_utc").cast(DataType::Datetime(TimeUnit::Milliseconds, Some("UTC".to_string()))))
        .sort(["logger_id", "sdi_address", "start_time_utc"], Default::default())
        .collect()?;

    // 2. Perform the asof join
    let matched_df = df.lazy()
        .sort(["logger_id", "sdi_address", "timestamp_utc_corrected"], Default::default())
        .join_asof(
            deployment_df.lazy(),
            col("timestamp_utc_corrected"), // left_on
            col("start_time_utc"),        // right_on
            ["logger_id", "sdi_address"], // by
            AsofStrategy::Backward,       // Find the last deployment that started before or at the data timestamp
            None,
        )?;

    // 3. You'll need to handle the end_time_utc logic, likely by filtering out rows
    //    where timestamp is after the deployment's end_time post-join.
    //    This is still vastly more efficient.
    
    Ok(matched_df)
    ```

### Code and Logic Refinements

#### 1. Sap Flux Calculation Accuracy

**CRITICAL:** There appears to be a bug in the Tmax heat velocity (`Vh`) calculation in `data_pipeline.rs`.

The README formula is: `Vh = √[...] / (tm(tm - t0))`
The code implements: `let vh = sqrt_arg.sqrt() / (tm * (tm - t0_val));`

This looks like it has an extra `tm` in the denominator. A more common formulation for Tmax does not have this `tm` multiplier. For example, some sources use `Vh = (xd / (tm - t0/2))`.

**Recommendation:** Please double-check the formula from your source (Forster, 2020). If the formula in your README is `... / (tm - t0)`, then the code is incorrect. If the formula is indeed `... / (tm * (tm-t0))`, then the code is correct but the formula is unusual and worth verifying. This could significantly impact your results.

#### 2. Clarity in Polars `map` for Calculations

In `apply_sap_flux_calculations`, you use `map` on a string-ified concatenation of columns to perform the Tmax calculation. This is a bit of a "hack" to pass multiple arguments and is not type-safe or clear.

**Recommendation:** Use `map_many` (or `apply` in older Polars versions) on a `struct` of the required columns. This is the idiomatic way to apply a UDF to multiple columns.

```rust
// In data_pipeline.rs -> apply_sap_flux_calculations

// Instead of concat_str([...]).map(...)

// Create a struct of the columns you need
.with_column(
    as_struct(vec![
        col("t_max_out"), 
        lit(k), 
        lit(t0), 
        lit(probe_distance)
    ]).apply(|s| {
        // s is a Series of Structs. Deconstruct it.
        let ca = s.struct_()?;
        let tm_series = &ca.fields()[0];
        let k_series = &ca.fields()[1];
        // ... and so on

        // Your calculation logic here, operating on series.
        // This is much cleaner and type-safe.

        Ok(Some(result_series))
    }, GetOutput::from_type(DataType::Float64))
    .alias("vh_tmax_outer")
)
```

#### 3. Daylight Savings Time Correction

The DST correction logic in `src/processing/dst_correction.rs` is excellent and follows the robust algorithm laid out in the `README`.

A minor point of confusion is in `DstTransitionTable::determine_timezone_offset`. The comment says the input `DateTime<Utc>` should be treated as naive local time. This is a potential source of bugs.

**Recommendation:** Make the function signature explicit about its expectation.

```rust
// In src/types/dst.rs
// Change the signature to enforce the contract
pub fn determine_timezone_offset(&self, naive_local_timestamp: NaiveDateTime) -> i32 {
    // ... use naive_local_timestamp directly without .naive_utc()
}

// In src/processing/dst_correction.rs where you call it:
// let first_datetime = DateTime::<Utc>::from_timestamp_millis(first_ts).unwrap();
// let chunk_timezone_offset = self.determine_chunk_timezone(first_datetime.naive_utc());
```
This change makes the code's intent clearer and prevents accidental misuse of the function with a true UTC timestamp.

#### 4. Project Structure and Duplication

You have multiple, slightly different `test_parser.rs` files (`/test_parser.rs`, `src/bin/test_parser.rs`, `examples/test_parser.rs`).

**Recommendation:** Consolidate these. The idiomatic place for this kind of runnable example is `examples/`.
1.  Keep `examples/test_parser.rs`.
2.  Delete `test_parser.rs` from the root directory.
3.  Delete `src/bin/test_parser.rs`.
4.  Remove the `[[bin]]` entries for them from `Cargo.toml` if they exist (they are currently inferred).
You can then run the example with `cargo run --example test_parser`.

Also, the `test_parser.rs` binary re-implements `detect_sensor_count` logic. It should call the function from the library (`crate::parsers::csv_parser::...`) to avoid code duplication and test the actual library code.

#### 5. Type System and Domain Modeling

Your use of the type system is good (e.g., `SdiAddress`, `FirmwareVersion`). You could take it a step further for even more safety.

**Recommendation:** Introduce a `LoggerId` newtype struct.

```rust
// In a suitable types file
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LoggerId(pub u32);

// You can then use LoggerId(401) instead of just 401.
// This prevents accidentally mixing up a logger ID with another u32 value.
```

### Minor Code Suggestions

-   In `src/types/deployment.rs`, the `validate_deployments` function has a section for checking overlaps. The nested loops are fine, but for a very large number of deployments for a single key, it could be `O(n^2)`. Sorting the deployments by start time first and then checking each one against only the previous one would make it `O(n log n)`. This is a minor optimization.
-   In `src/parsers/firmware_detector.rs`, the logic is a series of `if/else` checks. This is a perfect use case for a `match` statement on a tuple of the boolean conditions, which can make the logic more explicit and easier to verify.
-   Run `cargo clippy --all-features --all-targets` and address the lints. It often has great suggestions for making code more idiomatic and sometimes more performant.

This is a very strong foundation for a scientific data pipeline. The attention to detail in the `README` and the robust handling of complex real-world data issues are standout features. The recommendations above are primarily focused on improving performance, increasing clarity, and ensuring long-term maintainability. Fantastic work