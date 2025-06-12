// crates/sapflux-core/src/processing/unification.rs

use super::legacy_format::process_legacy_format;
use super::multi_sensor_format::process_multi_sensor_format;
use super::schema::get_full_schema_columns;
use super::types::{ManualFix, RawFileRecord};
use crate::error::{PipelineError, Result};
use chrono::{NaiveDate, Utc};
use polars::prelude::*;
use sqlx::PgPool;
use std::collections::HashMap;

pub async fn get_parsed_and_unified_lazyframe(pool: &PgPool) -> Result<LazyFrame> {
    println!("   -> Step 1: Parsing and unifying raw files...");
    let fixes_map = fetch_manual_fixes(pool).await?;
    let raw_files = fetch_all_raw_files(pool).await?;
    let mut lazyframes: Vec<LazyFrame> = Vec::with_capacity(raw_files.len());

    for record in &raw_files {
        match parse_and_clean_file(record, &fixes_map) {
            Ok(lf) => lazyframes.push(lf),
            Err(e) => eprintln!("\n      -> WARNING: Could not parse file {}. Reason: {}", &record.file_hash[..8], e),
        }
    }

    if lazyframes.is_empty() {
        return Err(PipelineError::Processing("No valid files could be parsed.".to_string()));
    }

    let unified_lf = concat(&lazyframes, UnionArgs::default())?;
    println!("   -> Step 1 Complete: Unified data has {} potential rows.", unified_lf.clone().collect()?.height());
    Ok(unified_lf)
}

async fn fetch_manual_fixes(pool: &PgPool) -> Result<HashMap<String, ManualFix>> {
    let fixes_vec = sqlx::query_as!(ManualFix, "SELECT file_hash, action, value, description FROM manual_fixes").fetch_all(pool).await?;
    Ok(fixes_vec.into_iter().map(|fix| (fix.file_hash.clone(), fix)).collect())
}

async fn fetch_all_raw_files(pool: &PgPool) -> Result<Vec<RawFileRecord>> {
    // FIX: Correct the type hint in the sqlx macro
    sqlx::query_as!(RawFileRecord, r#"SELECT file_hash, file_content, detected_schema_name as "detected_schema_name!: _" FROM raw_files"#)
        .fetch_all(pool).await.map_err(PipelineError::from)
}

fn parse_and_clean_file(record: &RawFileRecord, fixes: &HashMap<String, ManualFix>) -> Result<LazyFrame> {
    let start_date = NaiveDate::from_ymd_opt(2021, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
    let end_date = Utc::now().naive_utc();
    let mut lf = match record.detected_schema_name {
        crate::types::FileSchema::CRLegacySingleSensor => process_legacy_format(&record.file_content, start_date, end_date)?,
        crate::types::FileSchema::CR300MultiSensor => process_multi_sensor_format(&record.file_content, start_date, end_date)?,
    };
    if let Some(fix) = fixes.get(&record.file_hash) {
        lf = match fix.action.as_str() {
            "SET_LOGGER_ID" => {
                let new_id = fix.value.as_i64().ok_or_else(|| PipelineError::Processing("Invalid 'value' for SET_LOGGER_ID".into()))?;
                let mut eager_df = lf.collect()?;
                eager_df.drop_in_place("logger_id")?;
                eager_df.with_column(Series::new("logger_id".into(), vec![new_id; eager_df.height()]))?;
                eager_df.lazy()
            },
            _ => lf,
        };
    }
    Ok(lf.with_column(lit(record.file_hash.clone()).alias("file_hash")).select(get_full_schema_columns()))
}