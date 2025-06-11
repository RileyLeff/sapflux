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
    // Parse the CSV manually to handle the irregular structure
    let content_str = std::str::from_utf8(content)
        .map_err(|e| PipelineError::Processing(format!("Invalid UTF-8: {}", e)))?;
    
    let lines: Vec<&str> = content_str.lines().collect();
    if lines.len() < 5 {
        return Err(PipelineError::Processing("File has fewer than 5 lines".to_string()));
    }
    
    // Get headers from the second line
    let headers: Vec<String> = lines[1]
        .split(',')
        .map(|s| s.trim_matches('"').to_string())
        .collect();
    
    println!("      -> Found {} columns in multi-sensor file", headers.len());
    
    // Create a new CSV content starting from line 5 (skip the metadata rows)
    let data_lines = lines[4..].join("\n");
    let data_bytes = data_lines.as_bytes();
    
    // Now read the data with the correct number of columns
    let cursor = Cursor::new(data_bytes);
    let df = CsvReadOptions::default()
        .with_has_header(false)
        .with_ignore_errors(true)
        .with_n_threads(Some(1))
        .with_infer_schema_length(Some(100))
        .with_columns(Some(Arc::new(
            headers.iter().map(|s| PlSmallStr::from(s.as_str())).collect()
        )))
        .into_reader_with_file_handle(cursor)
        .finish()?;
    
    // The data doesn't have headers, so we need to rename columns
    let temp_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
    let df = df.lazy()
        .rename(temp_names, headers.iter().map(|s| s.as_str()).collect::<Vec<_>>(), false)
        .collect()?;
    
    // Find all sensor prefixes (S0_, S1_, S2_, etc.)
    let sensor_prefixes: Vec<String> = headers.iter()
        .filter_map(|h| {
            if h.starts_with("S") && h.contains("_") {
                let prefix = h.split('_').next()?;
                if prefix.chars().skip(1).all(|c| c.is_numeric()) {
                    Some(prefix.to_string())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    
    println!("      -> Found {} sensors: {:?}", sensor_prefixes.len(), sensor_prefixes);
    
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
        
        // Create expressions to select and rename columns
        let mut select_exprs: Vec<Expr> = vec![
            col("TIMESTAMP").alias("timestamp_naive"),
            col("RECORD").cast(DataType::Int64).alias("record_number"),
            col("Batt_volt").cast(DataType::Float64).alias("batt_volt"),
            when(col("PTemp_C").is_not_null())
                .then(col("PTemp_C"))
                .otherwise(lit(NULL))
                .cast(DataType::Float64)
                .alias("ptemp_c"),
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
                select_exprs.push(
                    when(col(&full_column_name).eq(lit(-99.0)))
                        .then(lit(NULL))
                        .otherwise(col(&full_column_name))
                        .cast(DataType::Float64)
                        .alias(new_name)
                );
            } else {
                select_exprs.push(lit(NULL).cast(DataType::Float64).alias(new_name));
            }
        }
        
        let sensor_df = df.clone().lazy()
            .select(&select_exprs)
            .filter(col("sdi_address").str().contains(lit("^[a-zA-Z0-9]$"), false))
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