// crates/sapflux-core/src/processing/legacy_format.rs

use crate::error::Result;
use chrono::NaiveDateTime;
use polars::prelude::*;
use std::io::Cursor;

pub fn process_legacy_format(
    content: &[u8],
    _start_date: NaiveDateTime,
    _end_date: NaiveDateTime,
) -> Result<LazyFrame> {
    let cursor = Cursor::new(content);

    let parse_options = CsvParseOptions::default()
        .with_null_values(Some(NullValues::AllColumns(vec!["-99".into(), "-99.0".into()])));

    let df = CsvReadOptions::default()
        .with_has_header(false)
        .with_skip_rows(4)
        .with_ignore_errors(true)
        .with_infer_schema_length(Some(10))
        .with_parse_options(parse_options)
        .into_reader_with_file_handle(cursor)
        .finish()?;

    let legacy_cols = &[
        "timestamp_naive", "record_number", "batt_volt", "logger_id",
        "sdi_address", "sap_flow_total", "vh_outer", "vh_inner",
        "alpha_out", "alpha_in", "beta_out", "beta_in",
        "tmax_tout", "tmax_tinn",
    ];
    
    let mut df_renamed = df.clone();
    
    // --- FIX IS HERE ---
    // The .copied() method dereferences the `&&str` to `&str`, which `set_column_names` can handle.
    let new_names: Vec<&str> = legacy_cols.iter().copied().take(df.width()).collect();
    df_renamed.set_column_names(new_names)?;

    // This is the crucial block. It enforces the full, final schema.
    let final_lf = df_renamed.lazy().with_columns(vec![
        // Ensure core types are correct and consistent
        col("timestamp_naive").cast(DataType::String),
        col("record_number").cast(DataType::Int64),
        col("batt_volt").cast(DataType::Float64),
        col("logger_id").cast(DataType::Int64),
        col("sdi_address").cast(DataType::String),
        col("sap_flow_total").cast(DataType::Float64),
        col("vh_outer").cast(DataType::Float64),
        col("vh_inner").cast(DataType::Float64),
        col("alpha_out").cast(DataType::Float64),
        col("alpha_in").cast(DataType::Float64),
        col("beta_out").cast(DataType::Float64),
        col("beta_in").cast(DataType::Float64),
        col("tmax_tout").cast(DataType::Float64),
        col("tmax_tinn").cast(DataType::Float64),
        // Add null columns for data this format doesn't have, to match the multi-sensor schema
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
    ]);

    Ok(final_lf)
}