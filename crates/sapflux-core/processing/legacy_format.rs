use crate::error::Result;
use chrono::NaiveDateTime;
use polars::prelude::*;
use std::io::Cursor;
use super::schema::get_full_schema_columns;

/// Processor for the old, single-sensor-per-file formats (CR200, old CR300).
pub fn process_legacy_format(
    content: &[u8],
    start_date: NaiveDateTime,
    end_date: NaiveDateTime,
) -> Result<LazyFrame> {
    let cursor = Cursor::new(content);
    let mut lf = CsvReadOptions::default()
        .with_has_header(false)
        .with_skip_rows(4)
        .with_ignore_errors(true)
        .into_reader_with_file_handle(cursor)
        .finish()?
        .lazy();

    let legacy_cols = &[
        "timestamp_naive", "record_number", "batt_volt", "logger_id",
        "sdi_address", "sap_flow_total", "vh_outer", "vh_inner",
        "alpha_out", "alpha_in", "beta_out", "beta_in",
        "tmax_tout", "tmax_tinn",
    ];

    let old_names: Vec<String> = lf.collect_schema()?.iter_names().map(|s| s.to_string()).collect();
    let new_names: Vec<&str> = legacy_cols.iter().take(old_names.len()).copied().collect();
    
    lf = lf.rename(old_names, new_names, false);

    // Parse timestamp and filter
    let lf = lf
        .with_column(
            col("timestamp_naive")
                .str()
                .strptime(
                    DataType::Datetime(TimeUnit::Milliseconds, None),
                    StrptimeOptions {
                        format: Some("%Y-%m-%d %H:%M:%S".into()),
                        strict: false,
                        exact: false,
                        cache: true,
                    },
                    lit("raise"),
                )
                .alias("timestamp_naive")
        )
        .filter(
            col("timestamp_naive")
                .gt_eq(lit(start_date))
                .and(col("timestamp_naive").lt_eq(lit(end_date))),
        )
        // Cast record_number to Int64
        .with_column(
            col("record_number").cast(DataType::Int64).alias("record_number")
        )
        // Add all the missing columns as NULLs
        .with_columns(&[
            lit(NULL).cast(DataType::Float64).alias("ptemp_c"),
            lit(NULL).cast(DataType::Float64).alias("tp_ds_out"),
            lit(NULL).cast(DataType::Float64).alias("dt_ds_out"),
            lit(NULL).cast(DataType::Float64).alias("ts_ds_out"),
            lit(NULL).cast(DataType::Float64).alias("tp_us_out"),
            lit(NULL).cast(DataType::Float64).alias("dt_us_out"),
            lit(NULL).cast(DataType::Float64).alias("ts_us_out"),
            lit(NULL).cast(DataType::Float64).alias("tp_ds_inn"),
            lit(NULL).cast(DataType::Float64).alias("dt_ds_inn"),
            lit(NULL).cast(DataType::Float64).alias("ts_ds_inn"),
            lit(NULL).cast(DataType::Float64).alias("tp_us_inn"),
            lit(NULL).cast(DataType::Float64).alias("dt_us_inn"),
            lit(NULL).cast(DataType::Float64).alias("ts_us_inn"),
            lit(NULL).cast(DataType::Float64).alias("tmax_tus_o"),
            lit(NULL).cast(DataType::Float64).alias("tmax_tus_i"),
        ])
        // Replace -99.0 with NULL for existing columns
        .with_columns(
            ["alpha_out", "alpha_in", "beta_out", "beta_in", "tmax_tout", "tmax_tinn"]
            .map(|name| {
                when(col(name).eq(lit(-99.0)))
                    .then(lit(NULL))
                    .otherwise(col(name))
                    .alias(name)
            })
        )
        .filter(col("sdi_address").str().contains(lit("^[a-zA-Z0-9]$"), false))
        .select(&get_full_schema_columns());
        
    Ok(lf)
}