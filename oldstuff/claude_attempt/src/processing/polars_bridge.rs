use crate::types::{RawDataFile, RawDataPoint, DataChunk};
use polars::prelude::*;
use chrono::{TimeZone, Utc};
use std::collections::HashMap;

#[derive(Debug)]
pub struct DataFrameBuilder;

impl DataFrameBuilder {
    pub fn from_raw_files(raw_files: Vec<RawDataFile>) -> PolarsResult<LazyFrame> {
        let mut all_data_points = Vec::new();
        let mut file_origins = HashMap::new();
        
        for raw_file in raw_files {
            let file_path = raw_file.file_path.clone();
            for point in raw_file.data_points {
                file_origins.insert(all_data_points.len(), file_path.clone());
                all_data_points.push(point);
            }
        }
        
        Self::data_points_to_dataframe(all_data_points)
    }
    
    pub fn from_data_chunks(chunks: Vec<DataChunk>) -> PolarsResult<LazyFrame> {
        let mut all_data_points = Vec::new();
        
        for chunk in chunks {
            all_data_points.extend(chunk.data_points);
        }
        
        Self::data_points_to_dataframe(all_data_points)
    }
    
    fn data_points_to_dataframe(data_points: Vec<RawDataPoint>) -> PolarsResult<LazyFrame> {
        if data_points.is_empty() {
            let empty_df = df! {
                "timestamp" => Vec::<i64>::new(),
                "record_number" => Vec::<u32>::new(),
                "battery_voltage" => Vec::<Option<f64>>::new(),
                "logger_id" => Vec::<Option<u32>>::new(),
                "sdi_address" => Vec::<Option<String>>::new(),
                "alpha_outer" => Vec::<Option<f64>>::new(),
                "alpha_inner" => Vec::<Option<f64>>::new(),
                "beta_outer" => Vec::<Option<f64>>::new(),
                "beta_inner" => Vec::<Option<f64>>::new(),
                "tmax_outer" => Vec::<Option<f64>>::new(),
                "tmax_inner" => Vec::<Option<f64>>::new(),
            }?;
            
            return Ok(empty_df.lazy().with_columns([
                col("timestamp").cast(DataType::Datetime(TimeUnit::Milliseconds, None)).alias("timestamp")
            ]));
        }
        
        let mut timestamps = Vec::<i64>::with_capacity(data_points.len());
        let mut record_numbers = Vec::with_capacity(data_points.len());
        let mut battery_voltages = Vec::with_capacity(data_points.len());
        let mut logger_ids = Vec::with_capacity(data_points.len());
        let mut sdi_addresses = Vec::with_capacity(data_points.len());
        let mut alpha_outer = Vec::with_capacity(data_points.len());
        let mut alpha_inner = Vec::with_capacity(data_points.len());
        let mut beta_outer = Vec::with_capacity(data_points.len());
        let mut beta_inner = Vec::with_capacity(data_points.len());
        let mut tmax_outer = Vec::with_capacity(data_points.len());
        let mut tmax_inner = Vec::with_capacity(data_points.len());
        
        for point in data_points {
            timestamps.push(point.timestamp.timestamp_millis());
            record_numbers.push(point.record_number);
            battery_voltages.push(point.battery_voltage);
            logger_ids.push(point.logger_id);
            sdi_addresses.push(point.sdi_address);
            alpha_outer.push(point.alpha_outer);
            alpha_inner.push(point.alpha_inner);
            beta_outer.push(point.beta_outer);
            beta_inner.push(point.beta_inner);
            tmax_outer.push(point.tmax_outer);
            tmax_inner.push(point.tmax_inner);
        }
        
        let df = df! {
            "timestamp" => timestamps,
            "record_number" => record_numbers,
            "battery_voltage" => battery_voltages,
            "logger_id" => logger_ids,
            "sdi_address" => sdi_addresses,
            "alpha_outer" => alpha_outer,
            "alpha_inner" => alpha_inner,
            "beta_outer" => beta_outer,
            "beta_inner" => beta_inner,
            "tmax_outer" => tmax_outer,
            "tmax_inner" => tmax_inner,
        }?;
        
        // Convert timestamp column to proper datetime type
        let with_datetime = df.lazy()
            .with_columns([
                col("timestamp").cast(DataType::Datetime(TimeUnit::Milliseconds, None)).alias("timestamp")
            ]);
        
        Ok(with_datetime)
    }
    
    pub fn apply_data_cleaning(df: LazyFrame) -> LazyFrame {
        // Define datetime boundaries as milliseconds
        let min_datetime_ms = chrono::Utc.ymd_opt(2021, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap().timestamp_millis();
        let max_datetime_ms = chrono::Utc::now().timestamp_millis();
        
        df
            // Filter out invalid timestamps (before 2021 or after current time)
            .filter(col("timestamp").gt_eq(lit(min_datetime_ms)))
            .filter(col("timestamp").lt_eq(lit(max_datetime_ms)))
            // Filter out null SDI addresses - detailed validation happens in deployment matching
            .filter(col("sdi_address").is_not_null())
            // Replace -99 values with null
            .with_columns([
                when(col("alpha_outer").eq(lit(-99.0)))
                    .then(lit(NULL))
                    .otherwise(col("alpha_outer"))
                    .alias("alpha_outer"),
                when(col("alpha_inner").eq(lit(-99.0)))
                    .then(lit(NULL))
                    .otherwise(col("alpha_inner"))
                    .alias("alpha_inner"),
                when(col("beta_outer").eq(lit(-99.0)))
                    .then(lit(NULL))
                    .otherwise(col("beta_outer"))
                    .alias("beta_outer"),
                when(col("beta_inner").eq(lit(-99.0)))
                    .then(lit(NULL))
                    .otherwise(col("beta_inner"))
                    .alias("beta_inner"),
                when(col("tmax_outer").eq(lit(-99.0)))
                    .then(lit(NULL))
                    .otherwise(col("tmax_outer"))
                    .alias("tmax_outer"),
                when(col("tmax_inner").eq(lit(-99.0)))
                    .then(lit(NULL))
                    .otherwise(col("tmax_inner"))
                    .alias("tmax_inner"),
            ])
            // Sort by timestamp
            .sort(["timestamp"], SortMultipleOptions::default())
    }
    
    pub fn identify_data_chunks(df: LazyFrame) -> PolarsResult<Vec<(String, LazyFrame)>> {
        // Group by unique file combinations to identify chunks
        // This is a simplified version - full implementation would analyze
        // timestamp patterns to detect chunk boundaries
        
        let collected = df.collect()?;
        
        // For now, return the entire dataframe as one chunk
        // In full implementation, this would analyze timestamp gaps and file origins
        Ok(vec![("main_chunk".to_string(), collected.lazy())])
    }
    
    pub fn export_to_parquet(df: LazyFrame, path: &str) -> PolarsResult<()> {
        // Collect DataFrame and write to parquet using correct API
        let mut collected = df.collect()?;
        
        // Use ParquetWriter for writing - correct 0.48.1 API
        use polars::prelude::{SerWriter, ParquetWriter};
        use std::fs::File;
        
        let mut file = File::create(path)?;
        ParquetWriter::new(&mut file).finish(&mut collected)?;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RawDataPoint;
    use chrono::Utc;
    
    #[test]
    fn test_dataframe_creation() {
        let data_points = vec![
            RawDataPoint {
                timestamp: Utc::now(),
                record_number: 1,
                battery_voltage: Some(12.5),
                logger_id: Some(601),
                sdi_address: Some("0".to_string()),
                alpha_outer: Some(0.1),
                alpha_inner: Some(0.2),
                beta_outer: Some(0.3),
                beta_inner: Some(0.4),
                tmax_outer: Some(50.0),
                tmax_inner: Some(40.0),
                temp_pre_downstream_outer: None,
                temp_delta_downstream_outer: None,
                temp_post_downstream_outer: None,
                temp_pre_upstream_outer: None,
                temp_delta_upstream_outer: None,
                temp_post_upstream_outer: None,
                temp_pre_downstream_inner: None,
                temp_delta_downstream_inner: None,
                temp_post_downstream_inner: None,
                temp_pre_upstream_inner: None,
                temp_delta_upstream_inner: None,
                temp_post_upstream_inner: None,
                tmax_upstream_outer: None,
                tmax_upstream_inner: None,
            }
        ];
        
        let result = DataFrameBuilder::data_points_to_dataframe(data_points);
        assert!(result.is_ok());
        
        let df = result.unwrap().collect().unwrap();
        assert_eq!(df.height(), 1);
    }
}