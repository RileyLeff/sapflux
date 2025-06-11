// crates/sapflux-core/processing/legacy_format.rs

use crate::error::Result;
use chrono::NaiveDateTime;
use polars::prelude::*;
use std::io::Cursor;
use super::schema::get_full_schema_columns;

pub fn process_legacy_format(
    content: &[u8],
    start_date: NaiveDateTime,
    end_date: NaiveDateTime,
) -> Result<LazyFrame> {
    let cursor = Cursor::new(content);

    let null_values = NullValues::AllColumns(vec!["-99".into(), "-99.0".into()]);
    // Your correction was right: CsvParseOptions
    let parse_options = CsvParseOptions::default().with_null_values(Some(null_values));

    let mut lf = CsvReadOptions::default()
        .with_has_header(false)
        .with_skip_rows(4)
        .with_ignore_errors(true)
        .with_parse_options(parse_options) // Your correction was right: with_parse_options
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

    let lf = lf
        // --- THE FINAL FIX IS HERE ---
        // Explicitly cast the sdi_address column to String immediately after it's created.
        .with_column(col("sdi_address").cast(DataType::String))
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
        .filter(col("sdi_address").str().contains(lit("^[a-zA-Z0-9]$"), false));
        //.select(&get_full_schema_columns());
        
    Ok(lf)
}