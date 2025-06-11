// crates/sapflux-core/src/processing/mod.rs

use crate::error::{PipelineError, Result};
use crate::types::FileSchema;
use chrono::{NaiveDate, Utc};
use polars::prelude::*;
use sqlx::PgPool;
use std::collections::HashMap;

// --- Module Declarations ---
mod schema;
mod legacy_format;
mod multi_sensor_format;

// --- Imports from our modules ---
use legacy_format::process_legacy_format;
use multi_sensor_format::process_multi_sensor_format;

// --- Struct Definitions (Needed in this file's scope) ---

// This struct holds the raw data fetched from the DB for processing.
struct RawFileRecord {
    file_hash: String,
    file_content: Vec<u8>,
    detected_schema_name: FileSchema,
}

// This struct holds the manual correction rules fetched from the DB.
#[derive(sqlx::FromRow, Debug)]
struct ManualFix {
    file_hash: String,
    action: String,
    value: serde_json::Value,
}

// --- Main Pipeline Orchestrator ---

pub async fn get_unified_lazyframe(pool: &PgPool) -> Result<LazyFrame> {
    println!("   -> Fetching manual corrections from the database...");
    let fixes_vec = sqlx::query_as!(ManualFix, "SELECT file_hash, action, value, description FROM manual_fixes")
        .fetch_all(pool)
        .await?;
    
    let fixes_map: HashMap<String, ManualFix> = fixes_vec.into_iter().map(|fix| (fix.file_hash.clone(), fix)).collect();
    println!("      -> Found {} manual fix rules to apply.", fixes_map.len());

    println!("   -> Fetching all raw files from the database...");
    let raw_files = fetch_all_raw_files(pool).await?; // This will now resolve.
    println!("      -> Found {} files to process.", raw_files.len());

    let mut lazyframes: Vec<LazyFrame> = Vec::with_capacity(raw_files.len());

    println!("   -> Parsing, cleaning, and unifying files...");
    for (i, record) in raw_files.iter().enumerate() {
        print!("\r      -> Processing file {}/{}...", i + 1, raw_files.len());
        std::io::Write::flush(&mut std::io::stdout()).unwrap();

        match parse_and_clean_file(record, &fixes_map) {
            Ok(lf) => lazyframes.push(lf),
            Err(e) => {
                // Accessing record.file_hash will now work.
                eprintln!("\n      -> WARNING: Could not parse file with hash {}. Reason: {}", &record.file_hash[..8], e);
            }
        }
    }

    if lazyframes.is_empty() { return Err(PipelineError::Processing("No valid files could be parsed.".to_string())); }

    println!("\n   -> Concatenating all files into a single dataset...");
    let unified_lf = concat(&lazyframes, UnionArgs::default())?;

    println!("✅ Unified LazyFrame created successfully.");
    Ok(unified_lf)
}


// --- Helper Functions (Needed in this file's scope) ---

/// Fetches all raw file records from the database.
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

/// Dispatches a file to the correct parser and applies any one-off fixes.
fn parse_and_clean_file(record: &RawFileRecord, fixes: &HashMap<String, ManualFix>) -> Result<LazyFrame> {
    let start_date = NaiveDate::from_ymd_opt(2021, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
    let end_date = Utc::now().naive_utc();

    // The initial parsing is the same.
    let mut lf = match record.detected_schema_name {
        FileSchema::CRLegacySingleSensor => {
            process_legacy_format(&record.file_content, start_date, end_date)?
        }
        FileSchema::CR300MultiSensor => {
            process_multi_sensor_format(&record.file_content, start_date, end_date)?
        }
    };

    // Apply the fix if one exists for this hash.
    if let Some(fix) = fixes.get(&record.file_hash) {
        println!("\n      -> Applying one-off fix '{}' to hash {}...", fix.action, &record.file_hash[..8]);
        
        lf = match fix.action.as_str() {
            "SET_LOGGER_ID" => {
                let new_id = fix.value.as_i64().ok_or_else(|| PipelineError::Processing(
                    format!("Invalid 'value' for SET_LOGGER_ID on hash {}: not an integer.", record.file_hash)
                ))?;
                
                // This fix is only relevant for legacy files which have a `logger_id` column.
                // We overwrite the entire column with the correct ID.
                lf.with_column(lit(new_id).alias("logger_id"))
            },
            _ => {
                eprintln!("\n      -> WARNING: Unknown fix action '{}' for hash {}. Skipping.", fix.action, record.file_hash);
                lf
            }
        };
    }

    Ok(lf.with_column(lit(record.file_hash.clone()).alias("file_hash")))
}