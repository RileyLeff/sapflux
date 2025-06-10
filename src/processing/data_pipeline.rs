use crate::types::{RawDataFile, Deployment};
use crate::parsers::CsvParser;
use crate::processing::{DataFrameBuilder, DstCorrector};
use polars::prelude::*;
use polars::df;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PipelineError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parse error: {0}")]
    Parse(#[from] crate::parsers::ParseError),
    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),
    #[error("Data validation error: {0}")]
    Validation(String),
    #[error("Processing error: {0}")]
    Processing(String),
}

pub struct SapfluxDataPipeline {
    dst_corrector: DstCorrector,
    deployments: Vec<Deployment>,
}

impl SapfluxDataPipeline {
    pub fn new(deployments: Vec<Deployment>) -> Self {
        Self {
            dst_corrector: DstCorrector::new(),
            deployments,
        }
    }
    
    pub fn process_directory<P: AsRef<Path>>(
        &self,
        raw_data_dir: P,
    ) -> Result<LazyFrame, PipelineError> {
        let raw_files = self.discover_and_parse_files(raw_data_dir)?;
        let df = DataFrameBuilder::from_raw_files(raw_files.clone())?;
        let cleaned_df = DataFrameBuilder::apply_data_cleaning(df);
        
        // Apply full DST correction algorithm
        println!("\n🕐 Applying DST correction algorithm...");
        let corrected_df = self.dst_corrector.correct_timestamps_full(cleaned_df, &raw_files)?;
        
        let matched_df = self.apply_deployment_matching(corrected_df)?;
        
        Ok(matched_df)
    }
    
    pub fn process_files(&self, file_paths: Vec<PathBuf>) -> Result<LazyFrame, PipelineError> {
        let mut raw_files = Vec::new();
        
        for path in file_paths {
            if !RawDataFile::should_skip_file(&path) {
                match CsvParser::parse_file(path.clone()) {
                    Ok(raw_file) => {
                        println!("Parsed {}: {} data points", path.display(), raw_file.data_points.len());
                        raw_files.push(raw_file);
                    },
                    Err(e) => {
                        eprintln!("Warning: Failed to parse {}: {}", path.display(), e);
                    }
                }
            }
        }
        
        if raw_files.is_empty() {
            return Err(PipelineError::Validation("No valid data files found".to_string()));
        }
        
        let df = DataFrameBuilder::from_raw_files(raw_files.clone())?;
        let cleaned_df = DataFrameBuilder::apply_data_cleaning(df);
        
        // Apply full DST correction algorithm
        println!("\n🕐 Applying DST correction algorithm...");
        let corrected_df = self.dst_corrector.correct_timestamps_full(cleaned_df, &raw_files)?;
        
        let matched_df = self.apply_deployment_matching(corrected_df)?;
        
        Ok(matched_df)
    }
    
    fn discover_and_parse_files<P: AsRef<Path>>(&self, dir: P) -> Result<Vec<RawDataFile>, PipelineError> {
        let mut raw_files = Vec::new();
        
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                // Recursively process subdirectories
                raw_files.extend(self.discover_and_parse_files(&path)?);
            } else if path.extension().map(|ext| ext == "dat" || ext == "csv").unwrap_or(false) {
                if !RawDataFile::should_skip_file(&path) {
                    match CsvParser::parse_file(path.clone()) {
                        Ok(raw_file) => raw_files.push(raw_file),
                        Err(e) => eprintln!("Warning: Failed to parse {}: {}", path.display(), e),
                    }
                }
            }
        }
        
        Ok(raw_files)
    }
    
    pub fn group_by_logger(&self, df: LazyFrame) -> Result<HashMap<u32, LazyFrame>, PipelineError> {
        let collected = df.collect()?;
        let mut logger_groups = HashMap::new();
        
        // Get unique logger IDs dynamically from the data
        let logger_ids = self.extract_unique_logger_ids(&collected)?;
        
        for logger_id in logger_ids {
            let filtered = collected
                .clone()
                .lazy()
                .filter(col("logger_id").eq(lit(logger_id)));
            
            logger_groups.insert(logger_id, filtered);
        }
        
        Ok(logger_groups)
    }
    
    fn extract_unique_logger_ids(&self, df: &DataFrame) -> Result<Vec<u32>, PipelineError> {
        // Use LazyFrame to get unique logger IDs
        let unique_loggers = df
            .clone()
            .lazy()
            .select([col("logger_id")])
            .filter(col("logger_id").is_not_null())
            .unique(None, UniqueKeepStrategy::First)
            .collect()?;
        
        // Extract the values as Vec<u32>
        let mut logger_ids = Vec::new();
        
        if let Ok(logger_column) = unique_loggers.column("logger_id") {
            for i in 0..logger_column.len() {
                if let Ok(value) = logger_column.get(i) {
                    if let Ok(logger_id) = value.try_extract::<u32>() {
                        logger_ids.push(logger_id);
                    }
                }
            }
        }
        
        if logger_ids.is_empty() {
            return Err(PipelineError::Processing("No valid logger IDs found in data".to_string()));
        }
        
        println!("Found {} unique logger IDs: {:?}", logger_ids.len(), logger_ids);
        Ok(logger_ids)
    }
    
    fn calculate_date_range(&self, df: &DataFrame) -> Result<String, PipelineError> {
        // Calculate min and max timestamps using Polars aggregations
        let date_stats = df
            .clone()
            .lazy()
            .select([
                col("timestamp").min().alias("min_timestamp"),
                col("timestamp").max().alias("max_timestamp"),
            ])
            .collect()?;
        
        // Extract the min and max values
        let min_col = date_stats.column("min_timestamp")?;
        let max_col = date_stats.column("max_timestamp")?;
        
        if min_col.len() > 0 && max_col.len() > 0 {
            if let (Ok(min_val), Ok(max_val)) = (min_col.get(0), max_col.get(0)) {
                // Convert AnyValue back to DateTime for formatting
                if let (Ok(min_dt), Ok(max_dt)) = (
                    min_val.try_extract::<i64>(),
                    max_val.try_extract::<i64>()
                ) {
                    // Convert milliseconds back to DateTime
                    let min_datetime = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(min_dt)
                        .ok_or_else(|| PipelineError::Processing("Invalid min timestamp".to_string()))?;
                    let max_datetime = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(max_dt)
                        .ok_or_else(|| PipelineError::Processing("Invalid max timestamp".to_string()))?;
                    
                    return Ok(format!(
                        "{} to {}",
                        min_datetime.format("%Y-%m-%d"),
                        max_datetime.format("%Y-%m-%d")
                    ));
                }
            }
        }
        
        Ok("Unable to determine date range".to_string())
    }
    
    pub fn export_processed_data(
        &self,
        df: LazyFrame,
        output_path: &str,
    ) -> Result<(), PipelineError> {
        // Add metadata columns before export
        let with_metadata = df.with_columns([
            lit("sap_flux_processed").alias("dataset_type"),
            lit(chrono::Utc::now().to_rfc3339()).alias("processed_timestamp"),
            lit("0.1.0").alias("pipeline_version"),
        ]);
        
        DataFrameBuilder::export_to_parquet(with_metadata, output_path)?;
        println!("Exported processed data to: {}", output_path);
        
        Ok(())
    }
    
    pub fn apply_deployment_matching(&self, df: LazyFrame) -> Result<LazyFrame, PipelineError> {
        let collected = df.collect()?;
        
        // Extract logger IDs and SDI addresses for matching
        let mut logger_sdi_pairs = Vec::new();
        let mut matched_deployments = Vec::new();
        let mut unmatched_count = 0;
        
        // Get all unique combinations of (logger_id, sdi_address) from the data
        let unique_combinations = collected
            .clone()
            .lazy()
            .select([col("logger_id"), col("sdi_address")])
            .filter(col("logger_id").is_not_null())
            .filter(col("sdi_address").is_not_null())
            .unique(None, UniqueKeepStrategy::First)
            .collect()?;
        
        if let (Ok(logger_col), Ok(sdi_col)) = (unique_combinations.column("logger_id"), unique_combinations.column("sdi_address")) {
            for i in 0..logger_col.len() {
                if let (Ok(logger_val), Ok(sdi_val)) = (logger_col.get(i), sdi_col.get(i)) {
                    if let Ok(logger_id) = logger_val.try_extract::<u32>() {
                        if let Some(sdi_addr) = sdi_val.get_str() {
                            // Validate SDI-12 address format: single alphanumeric character
                            if sdi_addr.len() == 1 && sdi_addr.chars().all(|c| c.is_alphanumeric()) {
                                logger_sdi_pairs.push((logger_id, sdi_addr.to_string()));
                            } else {
                                eprintln!("⚠️  Invalid SDI-12 address '{}' for logger {} - skipping", sdi_addr, logger_id);
                            }
                        }
                    }
                }
            }
        }
        
        println!("Found {} unique logger-SDI combinations in data", logger_sdi_pairs.len());
        
        // Try to match each combination with deployment metadata
        let logger_sdi_pairs_len = logger_sdi_pairs.len();
        for (logger_id, sdi_addr) in &logger_sdi_pairs {
            let matching_deployments: Vec<_> = self.deployments.iter()
                .filter(|d| {
                    d.hardware.datalogger_id == *logger_id &&
                    d.hardware.sdi_address.0 == *sdi_addr
                })
                .collect();
            
            match matching_deployments.len() {
                0 => {
                    eprintln!("⚠️  No deployment found for logger {} SDI {}", logger_id, sdi_addr);
                    unmatched_count += 1;
                }
                1 => {
                    println!("✅ Matched logger {} SDI {} to deployment: Tree {} at {}", 
                        logger_id, sdi_addr, 
                        matching_deployments[0].measurement.tree_id,
                        matching_deployments[0].measurement.site_name.as_ref().unwrap_or(&"Unknown".to_string())
                    );
                    matched_deployments.push(matching_deployments[0]);
                }
                n => {
                    println!("🔍 Found {} deployments for logger {} SDI {} - applying temporal matching", 
                        n, logger_id, sdi_addr);
                    println!("    Using DST-corrected timestamps for precise deployment matching");
                    matched_deployments.extend(matching_deployments);
                    // These will be handled by temporal matching below
                }
            }
        }
        
        // Apply temporal deployment matching using DST-corrected timestamps
        let deployments_owned: Vec<_> = matched_deployments.into_iter().cloned().collect();
        let with_deployment_metadata = self.apply_temporal_matching(collected, &deployments_owned)?;
        
        println!("Deployment Matching Summary:");
        println!("- Total deployments available: {}", self.deployments.len());
        println!("- Logger-SDI combinations in data: {}", logger_sdi_pairs_len);
        println!("- Matched combinations: {}", logger_sdi_pairs_len - unmatched_count);
        println!("- Unmatched combinations: {}", unmatched_count);
        
        if unmatched_count > 0 {
            eprintln!("⚠️  {} logger-SDI combinations require attention:", unmatched_count);
            eprintln!("   - Invalid SDI-12 addresses were filtered out");
            eprintln!("   - Multiple deployments require DST correction for temporal matching");
            eprintln!("   - Missing deployment records indicate data quality issues");
            eprintln!("   CRITICAL: Implement DST correction before proceeding with analysis");
        }
        
        Ok(with_deployment_metadata)
    }
    
    /// Apply temporal deployment matching using DST-corrected timestamps
    fn apply_temporal_matching(
        &self,
        df: DataFrame,
        available_deployments: &[crate::types::Deployment],
    ) -> Result<LazyFrame, PipelineError> {
        println!("🕐 Applying temporal deployment matching with DST-corrected timestamps...");
        
        // Convert to vector of rows for processing
        let mut rows = Vec::new();
        let height = df.height();
        
        for i in 0..height {
            let row = df.get_row(i)?;
            rows.push(row);
        }
        
        // Process each row to find temporal deployment match
        let mut deployment_ids = Vec::new();
        let mut tree_ids = Vec::new();
        let mut site_names = Vec::new();
        let mut zone_names = Vec::new();
        let mut plot_names = Vec::new();
        let mut tree_species = Vec::new();
        let mut sensor_types = Vec::new();
        let mut deployment_statuses = Vec::new();
        
        let mut matched_count = 0;
        let mut unmatched_count = 0;
        
        for row in &rows {
            // Extract key fields from row using column names
            let logger_id_idx = df.get_column_names().iter().position(|name| *name == "logger_id");
            let sdi_address_idx = df.get_column_names().iter().position(|name| *name == "sdi_address");
            let timestamp_utc_idx = df.get_column_names().iter().position(|name| *name == "timestamp_utc_corrected");
            
            let logger_id = logger_id_idx.and_then(|idx| row.0[idx].try_extract::<u32>().ok());
            let sdi_address = sdi_address_idx.and_then(|idx| row.0[idx].get_str());
            let timestamp_utc = timestamp_utc_idx.and_then(|idx| row.0[idx].try_extract::<i64>().ok());
            
            if let (Some(logger_id), Some(sdi_addr), Some(timestamp_ms)) = (logger_id, sdi_address, timestamp_utc) {
                // Convert timestamp to chrono DateTime for comparison
                let data_timestamp = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(timestamp_ms)
                    .unwrap_or_else(|| chrono::Utc::now());
                
                // Find matching deployment based on logger, SDI, and temporal overlap
                let matching_deployment = available_deployments.iter().find(|deployment| {
                    // Check logger and SDI match
                    if deployment.hardware.datalogger_id != logger_id {
                        return false;
                    }
                    if deployment.hardware.sdi_address.0 != sdi_addr {
                        return false;
                    }
                    
                    // Check temporal overlap
                    let deployment_start = deployment.start_time_utc;
                    let deployment_end = deployment.end_time_utc;
                    
                    data_timestamp >= deployment_start && 
                    (deployment_end.is_none() || data_timestamp <= deployment_end.unwrap())
                });
                
                if let Some(deployment) = matching_deployment {
                    // Found temporal match
                    deployment_ids.push(Some(deployment.id.to_string()));
                    tree_ids.push(Some(deployment.measurement.tree_id.clone()));
                    site_names.push(deployment.measurement.site_name.clone());
                    zone_names.push(deployment.measurement.zone_name.clone());
                    plot_names.push(deployment.measurement.plot_name.clone());
                    tree_species.push(deployment.measurement.tree_species.clone());
                    sensor_types.push(Some(format!("{:?}", deployment.hardware.sensor_type)));
                    deployment_statuses.push(Some("temporally_matched".to_string()));
                    matched_count += 1;
                } else {
                    // No temporal match found
                    deployment_ids.push(None);
                    tree_ids.push(None);
                    site_names.push(None);
                    zone_names.push(None);
                    plot_names.push(None);
                    tree_species.push(None);
                    sensor_types.push(None);
                    deployment_statuses.push(Some("temporally_unmatched".to_string()));
                    unmatched_count += 1;
                }
            } else {
                // Missing required fields
                deployment_ids.push(None);
                tree_ids.push(None);
                site_names.push(None);
                zone_names.push(None);
                plot_names.push(None);
                tree_species.push(None);
                sensor_types.push(None);
                deployment_statuses.push(Some("missing_data".to_string()));
                unmatched_count += 1;
            }
        }
        
        println!("✅ Temporal matching completed:");
        println!("   - Temporally matched: {}", matched_count);
        println!("   - Temporally unmatched: {}", unmatched_count);
        
        // Create deployment metadata DataFrame using the df! macro
        let deployment_df = df! {
            "deployment_id" => deployment_ids,
            "tree_id" => tree_ids,
            "site_name" => site_names,
            "zone_name" => zone_names,
            "plot_name" => plot_names,
            "tree_species" => tree_species,
            "sensor_type" => sensor_types,
            "deployment_status" => deployment_statuses,
        }?;
        
        // Horizontally concatenate with original data
        let result_df = df.hstack(&deployment_df.get_columns())?;
        
        Ok(result_df.lazy())
    }
    
    pub fn generate_summary_report(&self, df: LazyFrame) -> Result<String, PipelineError> {
        let collected = df.collect()?;
        
        let total_rows = collected.height();
        let date_range = if total_rows > 0 {
            self.calculate_date_range(&collected)?
        } else {
            "No data".to_string()
        };
        
        let unique_logger_ids = self.extract_unique_logger_ids(&collected)?;
        let unique_loggers = unique_logger_ids.len();
        
        // Check deployment matching status
        let deployment_status = if let Ok(status_col) = collected.column("deployment_status") {
            let mut matched = 0;
            let mut unmatched = 0;
            let mut temporal_needed = 0;
            
            for i in 0..status_col.len() {
                if let Ok(status) = status_col.get(i) {
                    if let Some(status_str) = status.get_str() {
                        match status_str {
                            "matched" => matched += 1,
                            "unmatched" => unmatched += 1,
                            "temporal_matching_needed" => temporal_needed += 1,
                            _ => {}
                        }
                    }
                }
            }
            
            format!("Deployment Matching: {} matched, {} unmatched, {} need temporal matching", 
                matched, unmatched, temporal_needed)
        } else {
            "Deployment matching not applied".to_string()
        };
        
        let report = format!(
            "Sap Flux Data Processing Summary\n\
             ================================\n\
             Total data points: {}\n\
             Date range: {}\n\
             Unique loggers: {}\n\
             {}\n\
             Deployments available: {}\n\
             Columns: {:?}\n",
            total_rows,
            date_range,
            unique_loggers,
            deployment_status,
            self.deployments.len(),
            collected.get_column_names()
        );
        
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::fs::write;
    
    #[test]
    fn test_pipeline_creation() {
        let pipeline = SapfluxDataPipeline::new(Vec::new());
        // Basic test to ensure pipeline can be created
        assert_eq!(pipeline.deployments.len(), 0);
    }
}