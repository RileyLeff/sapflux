// crates/sapflux-core/processing/mod.rs

use crate::error::{PipelineError, Result};
use crate::types::FileSchema;
use chrono::{NaiveDate, Utc};
use polars::prelude::*;
use sqlx::PgPool;

// Declare the modules within this `processing` module.
mod schema;
mod legacy_format;
mod multi_sensor_format;

// Bring the functions from our sub-modules into the current scope.
use legacy_format::process_legacy_format;
use multi_sensor_format::process_multi_sensor_format;

/// Internal struct to hold data fetched from the database for processing.
struct RawFileRecord {
    file_hash: String,
    file_content: Vec<u8>,
    detected_schema_name: FileSchema,
}

#[derive(sqlx::FromRow)]
struct ManualFix {
    file_hash: String,
    action: String,
    value: serde_json::Value,
}

/// **The Orchestrator**: The main public function for the processing pipeline.
/// It fetches all raw files, dispatches them to the appropriate parser,
/// and concatenates the results into a single, unified LazyFrame.
pub async fn get_unified_lazyframe(pool: &PgPool) -> Result<LazyFrame> {
    println!("   -> Fetching all raw files from the database...");
    let raw_files = fetch_all_raw_files(pool).await?;
    println!("      -> Found {} files to process.", raw_files.len());

    let mut lazyframes: Vec<LazyFrame> = Vec::with_capacity(raw_files.len());

    println!("   -> Parsing, cleaning, and unifying files...");
    for (i, record) in raw_files.iter().enumerate() {
        // Use print! with a carriage return to show progress on a single line.
        print!("\r      -> Processing file {}/{}...", i + 1, raw_files.len());
        std::io::Write::flush(&mut std::io::stdout()).unwrap();

        match parse_and_clean_file(record) {
            Ok(lf) => lazyframes.push(lf),
            Err(e) => {
                // Print warnings on a new line so they don't get overwritten.
                eprintln!(
                    "\n      -> WARNING: Could not parse file with hash {}. Reason: {}",
                    &record.file_hash[..8],
                    e
                );
            }
        }
    }

    if lazyframes.is_empty() {
        return Err(PipelineError::Processing(
            "No valid raw files could be parsed into DataFrames.".to_string(),
        ));
    }

    println!("\n   -> Concatenating all files into a single dataset...");
    // The UnionArgs::default() will stack the LazyFrames.
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
    // Define the valid time window for the entire project.
    let start_date = NaiveDate::from_ymd_opt(2021, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
    let end_date = Utc::now().naive_utc();

    // Dispatch based on the schema detected during ingestion.
    let lf = match record.detected_schema_name {
        FileSchema::CRLegacySingleSensor => {
            process_legacy_format(&record.file_content, start_date, end_date)?
        }
        FileSchema::CR300MultiSensor => {
            process_multi_sensor_format(&record.file_content, start_date, end_date)?
        }
    };

    // Add the file hash as a column for traceability.
    Ok(lf.with_column(lit(record.file_hash.clone()).alias("file_hash")))
}