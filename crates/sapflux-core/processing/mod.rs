use crate::error::{PipelineError, Result};
use crate::types::FileSchema;
use chrono::{NaiveDate, Utc};
use polars::prelude::*;
use sqlx::PgPool;

mod schema;
mod legacy_format;
mod multi_sensor_format;

use schema::get_full_schema_columns;
use legacy_format::process_legacy_format;
use multi_sensor_format::process_multi_sensor_format;

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

    let lf = match record.detected_schema_name {
        FileSchema::CRLegacySingleSensor => {
            process_legacy_format(&record.file_content, start_date, end_date)?
        }
        FileSchema::CR300MultiSensor => {
            process_multi_sensor_format(&record.file_content, start_date, end_date)?
        }
    };

    Ok(lf.with_column(lit(record.file_hash.clone()).alias("file_hash")))
}

// Re-export the inspect function if you add it
// pub use multi_sensor_format::inspect_file_by_hash;