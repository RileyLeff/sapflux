// crates/sapflux-core/src/processing/multi_sensor_format.rs
use crate::error::{PipelineError, Result};
use chrono::NaiveDateTime;
use polars::prelude::*;
use std::io::Cursor;

pub fn process_multi_sensor_format(
    content: &[u8],
    _start_date: NaiveDateTime,
    _end_date: NaiveDateTime,
) -> Result<LazyFrame> {
    let content_str = std::str::from_utf8(content)
        .map_err(|e| PipelineError::Processing(format!("File content is not valid UTF-8: {}", e)))?;
    let lines: Vec<&str> = content_str.lines().collect();

    if lines.len() < 5 {
        return Err(PipelineError::Processing("Multi-sensor file has fewer than 5 lines.".to_string()));
    }

    let first_line_parts: Vec<&str> = lines[0].split(',').map(|s| s.trim_matches('"')).collect();
    let logger_id_str = first_line_parts.get(1)
        .and_then(|s| s.split('_').nth(1))
        .ok_or_else(|| PipelineError::Processing("Could not parse logger_id from TOA5 header.".to_string()))?;

    let logger_id: i64 = logger_id_str.parse().map_err(|_| PipelineError::Processing(
        format!("Failed to parse '{}' as a valid logger_id integer.", logger_id_str)
    ))?;

    let headers: Vec<String> = lines[1].split(',').map(|s| s.trim_matches('"').to_string()).collect();
    let data_content = lines[4..].join("\n");
    let cursor = Cursor::new(data_content.as_bytes());

    let null_values = NullValues::AllColumns(vec!["NAN".into(), "-99".into(), "-99.0".into()]);
    let parse_options = CsvParseOptions::default().with_null_values(Some(null_values));

    let df_with_temp_names = CsvReadOptions::default()
        .with_has_header(false)
        .with_ignore_errors(true)
        .with_parse_options(parse_options)
        .into_reader_with_file_handle(cursor)
        .finish()?;

    let temp_names: Vec<String> = df_with_temp_names.get_column_names().iter().map(|s| s.to_string()).collect();
    let df = df_with_temp_names.lazy().rename(&temp_names, &headers, true).collect()?;

    let sensor_prefixes: Vec<String> = headers.iter()
        .filter_map(|h| h.split('_').next())
        .filter(|p| p.starts_with('S') && p.chars().nth(1).map_or(false, |c| c.is_ascii_digit()))
        .collect::<std::collections::HashSet<_>>().into_iter().map(|s| s.to_string()).collect();

    if sensor_prefixes.is_empty() { return Err(PipelineError::Processing("No sensor data columns found.".to_string())); }

    let mut sensor_lazyframes = Vec::with_capacity(sensor_prefixes.len());

    for prefix in sensor_prefixes {
        let sdi_address = prefix.trim_start_matches('S');
        let column_mapping = [
            ("AlpOut", "alpha_out"), ("AlpInn", "alpha_in"),
            ("BetOut", "beta_out"), ("BetInn", "beta_in"),
            ("tMxTout", "tmax_tout"), ("tMxTinn", "tmax_tinn"),
            ("TpDsOut", "tp_ds_out"), ("dTDsOut", "dt_ds_out"),
            ("TsDsOut", "ts_ds_out"), ("TpUsOut", "tp_us_out"),
            ("dTUsOut", "dt_us_out"), ("TsUsOut", "ts_us_out"),
            ("TpDsInn", "tp_ds_inn"), ("dTDsInn", "dt_ds_inn"),
            ("TsDsInn", "ts_ds_inn"), ("TpUsInn", "tp_us_inn"),
            ("dTUsInn", "dt_us_inn"), ("TsUsInn", "ts_us_inn"),
            ("tMxTUsO", "tmax_tus_o"), ("tMxTUsI", "tmax_tus_i"),
        ];

        let mut select_exprs: Vec<Expr> = vec![
            col("TIMESTAMP").alias("timestamp_naive").cast(DataType::String),
            col("RECORD").alias("record_number").cast(DataType::Int64),
            // FIX: Explicitly cast the logger_id literal to Int64
            lit(logger_id).alias("logger_id").cast(DataType::Int64),
            col("Batt_volt").alias("batt_volt").cast(DataType::Float64),
            col("PTemp_C").alias("ptemp_c").cast(DataType::Float64),
            lit(sdi_address).alias("sdi_address").cast(DataType::String),
        ];

        for (source_suffix, target_name) in &column_mapping {
            let full_col_name = format!("{}_{}", prefix, source_suffix);
            // FIX: Ensure all data columns are cast to Float64
            select_exprs.push(col(&full_col_name).alias(*target_name).cast(DataType::Float64));
        }

        sensor_lazyframes.push(df.clone().lazy().select(select_exprs));
    }
    
    concat(&sensor_lazyframes, UnionArgs::default()).map_err(PipelineError::from)
}