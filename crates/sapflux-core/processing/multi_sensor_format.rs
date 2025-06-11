use crate::error::{PipelineError, Result};
use chrono::NaiveDateTime;
use polars::prelude::*;
use std::io::Cursor;
use std::sync::Arc;
use super::schema::get_full_schema_columns;

/// Processor for the new, wide, multi-sensor format (new CR300).
pub fn process_multi_sensor_format(
    content: &[u8],
    start_date: NaiveDateTime,
    end_date: NaiveDateTime,
) -> Result<LazyFrame> {
    // First, read just the headers from the second row
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(content);
    
    // Skip first row and get headers from second row
    reader.records().next();
    let headers: Vec<String> = reader.records()
        .next()
        .ok_or_else(|| PipelineError::Processing("No header row found".to_string()))??
        .iter()
        .map(|s| s.to_string())
        .collect();
    
    // Read the actual data
    let cursor = Cursor::new(content);
    let df = CsvReadOptions::default()
        .with_has_header(false)
        .with_skip_rows(4)
        .with_ignore_errors(true)
        .with_n_threads(Some(1))
        .with_infer_schema_length(Some(100))
        .with_columns(Some(Arc::new(
            (0..headers.len()).map(|i| format!("column_{}", i).into()).collect()
        )))
        .into_reader_with_file_handle(cursor)
        .finish()?;
    
    // Rename columns to actual header names
    let temp_names: Vec<String> = (0..headers.len()).map(|i| format!("column_{}", i)).collect();
    let df = df.lazy()
        .rename(temp_names, headers.iter().map(|s| s.as_str()).collect::<Vec<_>>(), false)
        .collect()?;
    
    // Find all sensor prefixes (S0_, S1_, S2_, etc.)
    let sensor_prefixes: Vec<String> = headers.iter()
        .filter_map(|h| {
            if h.starts_with("S") && h.contains("_") {
                let prefix = h.split('_').next()?;
                Some(prefix.to_string())
            } else {
                None
            }
        })
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    
    // Parse timestamp before processing
    let df = df.lazy()
        .with_column(
            col("TIMESTAMP")
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
                .alias("TIMESTAMP")
        )
        .filter(
            col("TIMESTAMP")
                .gt_eq(lit(start_date))
                .and(col("TIMESTAMP").lt_eq(lit(end_date))),
        )
        .collect()?;
    
    // Process each sensor separately and collect results
    let mut sensor_dfs = Vec::new();
    
    for sensor_prefix in sensor_prefixes {
        // Extract the SDI address from the prefix (e.g., "S0" -> "0")
        let sdi_address = sensor_prefix.trim_start_matches('S');
        
        // Create expressions to rename sensor-specific columns
        let mut select_exprs: Vec<Expr> = vec![
            col("TIMESTAMP").alias("timestamp_naive"),
            col("RECORD").alias("record_number"),
            col("Batt_volt").alias("batt_volt"),
            col("PTemp_C").alias("ptemp_c"),
            lit(sdi_address).alias("sdi_address"),
        ];
        
        // Map sensor columns to standard names
        let column_mapping = [
            ("AlpOut", "alpha_out"),
            ("AlpInn", "alpha_in"),
            ("BetOut", "beta_out"),
            ("BetInn", "beta_in"),
            ("tMxTout", "tmax_tout"),
            ("tMxTinn", "tmax_tinn"),
            ("TpDsOut", "tp_ds_out"),
            ("dTDsOut", "dt_ds_out"),
            ("TsDsOut", "ts_ds_out"),
            ("TpUsOut", "tp_us_out"),
            ("dTUsOut", "dt_us_out"),
            ("TsUsOut", "ts_us_out"),
            ("TpDsInn", "tp_ds_inn"),
            ("dTDsInn", "dt_ds_inn"),
            ("TsDsInn", "ts_ds_inn"),
            ("TpUsInn", "tp_us_inn"),
            ("dTUsInn", "dt_us_inn"),
            ("TsUsInn", "ts_us_inn"),
            ("tMxTUsO", "tmax_tus_o"),
            ("tMxTUsI", "tmax_tus_i"),
        ];
        
        for (old_name, new_name) in &column_mapping {
            let full_column_name = format!("{}_{}", sensor_prefix, old_name);
            if headers.contains(&full_column_name) {
                select_exprs.push(col(&full_column_name).alias(new_name));
            } else {
                select_exprs.push(lit(NULL).cast(DataType::Float64).alias(new_name));
            }
        }
        
        let sensor_df = df.clone().lazy()
            .select(&select_exprs)
            .select(&get_full_schema_columns());
            
        sensor_dfs.push(sensor_df);
    }
    
    // Concatenate all sensor dataframes
    if sensor_dfs.is_empty() {
        return Err(PipelineError::Processing("No sensor data found".to_string()));
    }
    
    let unified_lf = concat(&sensor_dfs, UnionArgs::default())?;
    
    Ok(unified_lf)
}