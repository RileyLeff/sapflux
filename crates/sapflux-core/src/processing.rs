// crates/sapflux-core/src/processing.rs

use crate::error::{PipelineError, Result};
use crate::types::FileSchema;
use chrono::{NaiveDate, NaiveDateTime, Utc};
use polars::prelude::*;
use sqlx::PgPool;
use std::io::Cursor;

/// Internal struct to hold data fetched from the database.
struct RawFileRecord {
    file_hash: String,
    file_content: Vec<u8>,
    detected_schema_name: FileSchema,
}

/// **The Orchestrator**: The main public function.
pub async fn get_unified_lazyframe(pool: &PgPool) -> Result<LazyFrame> {
    println!("   -> Fetching all raw files from the database...");
    let raw_files = fetch_all_raw_files(pool).await?;
    println!("      -> Found {} files to process.", raw_files.len());

    let mut lazyframes: Vec<LazyFrame> = Vec::with_capacity(raw_files.len());

    println!("   -> Parsing, cleaning, and unifying files...");
    for (i, record) in raw_files.iter().enumerate() {
        print!("\r      -> Processing file {}/{}...", i + 1, raw_files.len());
        match parse_and_clean_file(record) {
            Ok(lf) => lazyframes.push(lf),
            Err(e) => eprintln!(
                "\n      -> WARNING: Could not parse file with hash {}. Reason: {}",
                &record.file_hash[..8],
                e
            ),
        }
    }

    if lazyframes.is_empty() {
        return Err(PipelineError::Processing(
            "No valid raw files could be parsed.".to_string(),
        ));
    }

    println!("\n   -> Concatenating all files into a single dataset...");
    let unified_lf = concat(&lazyframes, UnionArgs::default())?;

    println!("✅ Unified LazyFrame created successfully.");
    Ok(unified_lf)
}

/// **The Fetcher**: Fetches all raw file records from the database.
async fn fetch_all_raw_files(pool: &PgPool) -> Result<Vec<RawFileRecord>> {
    sqlx::query_as!(
        RawFileRecord,
        r#"
        SELECT
            file_hash,
            file_content,
            detected_schema_name as "detected_schema_name!: FileSchema"
        FROM raw_files
        "#
    )
    .fetch_all(pool)
    .await
    .map_err(PipelineError::from)
}

/// **The Parser & Cleaner**: Dispatches to the correct format-specific processor.
fn parse_and_clean_file(record: &RawFileRecord) -> Result<LazyFrame> {
    let start_date =
        NaiveDate::from_ymd_opt(2021, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
    let end_date = Utc::now().naive_utc();

    let mut lf = match record.detected_schema_name {
        FileSchema::CRLegacySingleSensor => {
            process_legacy_format(&record.file_content, start_date, end_date)?
        }
        FileSchema::CR300MultiSensor => {
            process_multi_sensor_format(&record.file_content, start_date, end_date)?
        }
    };

    lf = lf.with_column(lit(record.file_hash.clone()).alias("file_hash"));

    Ok(lf)
}

/// Processor for the old, single-sensor-per-file formats (CR200, old CR300).
fn process_legacy_format(
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

    // FIX: `.collect_schema()` returns a `Result<Arc<Schema>>`.
    let old_names: Vec<String> = lf.collect_schema()?.iter_names().map(|s| s.to_string()).collect();
    let new_names: Vec<&str> = legacy_cols.iter().take(old_names.len()).copied().collect();

    // FIX: `.rename` requires a third boolean argument `strict`.
    lf = lf.rename(&old_names, new_names, false);

    let lf = lf
        .filter(
            col("timestamp_naive")
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .gt_eq(lit(start_date))
                .and(col("timestamp_naive").lt_eq(lit(end_date))),
        )
        .select(&[
            col("timestamp_naive").cast(DataType::Datetime(TimeUnit::Milliseconds, None)),
            col("sdi_address").cast(DataType::String),
            col("alpha_out").cast(DataType::Float64),
            col("alpha_in").cast(DataType::Float64),
            col("beta_out").cast(DataType::Float64),
            col("beta_in").cast(DataType::Float64),
            col("tmax_tout").cast(DataType::Float64),
            col("tmax_tinn").cast(DataType::Float64),
            col("batt_volt").cast(DataType::Float64),
            lit(NULL).cast(DataType::Float64).alias("ptemp_c"),
        ])
        .with_columns(
            ["alpha_out", "alpha_in", "beta_out", "beta_in", "tmax_tout", "tmax_tinn"]
            .map(|name| {
                when(col(name).eq(lit(-99.0)))
                    .then(lit(NULL))
                    .otherwise(col(name))
                    .alias(name)
            })
        )
        .filter(col("sdi_address").str().contains(lit("^[a-zA-Z0-9]$"), false));
        
    Ok(lf)
}

/// Processor for the new, wide, multi-sensor format (new CR300).
fn process_multi_sensor_format(
    content: &[u8],
    start_date: NaiveDateTime,
    end_date: NaiveDateTime,
) -> Result<LazyFrame> {
    let mut reader = csv::ReaderBuilder::new().has_headers(false).from_reader(content);
    let headers: Vec<String> = reader.records().nth(1).unwrap()?.iter().map(|s| s.to_string()).collect();
    
    let cursor = Cursor::new(content);
    let mut df = CsvReadOptions::default()
        .with_has_header(false)
        .with_skip_rows(4)
        .with_ignore_errors(true)
        .into_reader_with_file_handle(cursor)
        .finish()?;
    
    df.set_column_names(headers.iter().map(|s| s.as_str()))?;
    
    let id_vars = &["TIMESTAMP", "RECORD", "Batt_volt", "PTemp_C"];
    let value_vars: Vec<String> = headers.iter().filter(|h| !id_vars.contains(&h.as_str())).cloned().collect();

    let melted_df = df.lazy()
        .filter(
            col("TIMESTAMP")
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .gt_eq(lit(start_date))
                .and(col("TIMESTAMP").lt_eq(lit(end_date))),
        )
        // Perform the unpivot/melt operation
        .melt(id_vars, value_vars)?
        .with_columns(&[
            col("variable")
                .str()
                .extract(lit(r"^S(\d+)_"), 1)
                .alias("sdi_address"),
            col("variable")
                .str()
                .extract(lit(r"^S\d+_(.*)$"), 1)
                .alias("measurement_type"),
        ])
        .collect()?;

    // FIX: The pivot operation must be done on a GroupBy object created from an eager DataFrame.
    let pivoted_df = melted_df
        .group_by(["TIMESTAMP", "RECORD", "Batt_volt", "PTemp_C", "sdi_address"])?
        .pivot(col("measurement_type"), col("value"))
        .first()?;

    let lf = pivoted_df.lazy()
        .select(&[
            col("TIMESTAMP").alias("timestamp_naive").cast(DataType::Datetime(TimeUnit::Milliseconds, None)),
            col("sdi_address"),
            col("AlpOut").alias("alpha_out").cast(DataType::Float64),
            col("AlpInn").alias("alpha_in").cast(DataType::Float64),
            col("BetOut").alias("beta_out").cast(DataType::Float64),
            col("BetInn").alias("beta_in").cast(DataType::Float64),
            col("tMxTout").alias("tmax_tout").cast(DataType::Float64),
            col("tMxTinn").alias("tmax_tinn").cast(DataType::Float64),
            col("Batt_volt").alias("batt_volt").cast(DataType::Float64),
            col("PTemp_C").alias("ptemp_c").cast(DataType::Float64),
        ])
        .filter(col("sdi_address").str().contains(lit("^[a-zA-Z0-9]$"), false));

    Ok(lf)
}