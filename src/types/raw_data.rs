use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawDataHeader {
    pub model: String,
    pub logger_id: String,
    pub datalogger_type: String,
    pub serial_number: String,
    pub os_version: String,
    pub program_name: String,
    pub program_signature: String,
    pub table_name: String,
}

impl RawDataHeader {
    pub fn parse_toa5_header(first_line: &str) -> Result<Self, String> {
        // Remove surrounding quotes and split by ","
        let clean_line = first_line.trim();
        let parts: Vec<&str> = clean_line
            .split(',')
            .map(|s| s.trim_matches('"'))
            .collect();
        
        if parts.len() < 8 || parts[0] != "TOA5" {
            return Err(format!("Invalid TOA5 header format. Expected 8+ parts, got {}: {:?}", parts.len(), parts));
        }

        Ok(Self {
            model: parts[1].to_string(),
            logger_id: parts[1].split('_').nth(1).unwrap_or("unknown").to_string(),
            datalogger_type: parts[2].to_string(),
            serial_number: parts[3].to_string(),
            os_version: parts[4].to_string(),
            program_name: parts[5].to_string(),
            program_signature: parts[6].to_string(),
            table_name: parts[7].to_string(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawDataPoint {
    pub timestamp: DateTime<Utc>,
    pub record_number: u32,
    pub battery_voltage: Option<f64>,
    pub logger_id: Option<u32>,
    pub sdi_address: Option<String>,
    
    // Core thermal measurements
    pub alpha_outer: Option<f64>,
    pub alpha_inner: Option<f64>,
    pub beta_outer: Option<f64>,
    pub beta_inner: Option<f64>,
    pub tmax_outer: Option<f64>,
    pub tmax_inner: Option<f64>,
    
    // Additional temperature measurements (when available)
    pub temp_pre_downstream_outer: Option<f64>,
    pub temp_delta_downstream_outer: Option<f64>,
    pub temp_post_downstream_outer: Option<f64>,
    pub temp_pre_upstream_outer: Option<f64>,
    pub temp_delta_upstream_outer: Option<f64>,
    pub temp_post_upstream_outer: Option<f64>,
    
    pub temp_pre_downstream_inner: Option<f64>,
    pub temp_delta_downstream_inner: Option<f64>,
    pub temp_post_downstream_inner: Option<f64>,
    pub temp_pre_upstream_inner: Option<f64>,
    pub temp_delta_upstream_inner: Option<f64>,
    pub temp_post_upstream_inner: Option<f64>,
    
    pub tmax_upstream_outer: Option<f64>,
    pub tmax_upstream_inner: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawDataFile {
    pub file_path: PathBuf,
    pub header: RawDataHeader,
    pub column_names: Vec<String>,
    pub units: Vec<String>,
    pub data_types: Vec<String>,
    pub data_points: Vec<RawDataPoint>,
}

impl RawDataFile {
    pub fn new(file_path: PathBuf, header: RawDataHeader) -> Self {
        Self {
            file_path,
            header,
            column_names: Vec::new(),
            units: Vec::new(),
            data_types: Vec::new(),
            data_points: Vec::new(),
        }
    }

    pub fn get_logger_id(&self) -> Result<u32, String> {
        self.header.logger_id.parse::<u32>()
            .map_err(|_| format!("Unable to parse logger ID: {}", self.header.logger_id))
    }

    pub fn should_skip_file(file_path: &PathBuf) -> bool {
        let filename = file_path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        
        crate::types::constants::FORBIDDEN_FILENAME_WORDS
            .iter()
            .any(|&word| filename.contains(word))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataChunk {
    pub origin_files: Vec<PathBuf>,
    pub start_timestamp: DateTime<Utc>,
    pub end_timestamp: DateTime<Utc>,
    pub logger_timezone_offset: i32, // Hours from UTC
    pub data_points: Vec<RawDataPoint>,
}

impl DataChunk {
    pub fn new(origin_files: Vec<PathBuf>, data_points: Vec<RawDataPoint>) -> Option<Self> {
        if data_points.is_empty() {
            return None;
        }

        let start_timestamp = data_points.iter().map(|p| p.timestamp).min()?;
        let end_timestamp = data_points.iter().map(|p| p.timestamp).max()?;

        Some(Self {
            origin_files,
            start_timestamp,
            end_timestamp,
            logger_timezone_offset: 0, // To be determined by DST correction
            data_points,
        })
    }

    pub fn unique_file_signature(&self) -> String {
        let mut files: Vec<String> = self.origin_files
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();
        files.sort();
        files.join("|")
    }
}