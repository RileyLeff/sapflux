use crate::types::{RawDataFile, RawDataHeader, RawDataPoint, FirmwareVersion};
use chrono::{DateTime, Utc, NaiveDateTime};
use csv::ReaderBuilder;
use std::path::PathBuf;
use std::fs::File;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("CSV parsing error: {0}")]
    Csv(#[from] csv::Error),
    #[error("Invalid header format: {0}")]
    InvalidHeader(String),
    #[error("Header parsing error: {0}")]
    HeaderParsing(String),
    #[error("Missing required column: {0}")]
    MissingColumn(String),
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),
    #[error("Unsupported firmware version")]
    UnsupportedFirmware,
}

pub struct CsvParser;

impl CsvParser {
    pub fn parse_file(file_path: PathBuf) -> Result<RawDataFile, ParseError> {
        if RawDataFile::should_skip_file(&file_path) {
            return Err(ParseError::InvalidHeader("File should be skipped".to_string()));
        }

        let file = File::open(&file_path)?;
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .flexible(true) // Allow variable field counts
            .from_reader(file);

        let mut records = reader.records();
        
        // Parse TOA5 header (first line)
        let header_record = records.next()
            .ok_or_else(|| ParseError::InvalidHeader("Empty file".to_string()))??;
        let header_line = header_record.iter().collect::<Vec<&str>>().join(",");
        let header = RawDataHeader::parse_toa5_header(&header_line)
            .map_err(|e| ParseError::HeaderParsing(e))?;

        // Parse column names (second line)
        let column_record = records.next()
            .ok_or_else(|| ParseError::InvalidHeader("Missing column headers".to_string()))??;
        let column_names: Vec<String> = column_record.iter().map(|s| s.trim_matches('"').to_string()).collect();

        // Parse units (third line)
        let units_record = records.next()
            .ok_or_else(|| ParseError::InvalidHeader("Missing units row".to_string()))??;
        let units: Vec<String> = units_record.iter().map(|s| s.trim_matches('"').to_string()).collect();

        // Parse data types (fourth line)
        let types_record = records.next()
            .ok_or_else(|| ParseError::InvalidHeader("Missing data types row".to_string()))??;
        let data_types: Vec<String> = types_record.iter().map(|s| s.trim_matches('"').to_string()).collect();

        // Detect firmware version
        let firmware_version = crate::parsers::detect_firmware_version(&header, &column_names)
            .ok_or(ParseError::UnsupportedFirmware)?;

        let mut raw_file = RawDataFile::new(file_path, header);
        raw_file.column_names = column_names.clone();
        raw_file.units = units;
        raw_file.data_types = data_types;

        // Parse data rows
        for record in records {
            let record = record?;
            if let Ok(mut data_points) = Self::parse_data_row(&record, &column_names, &firmware_version) {
                // Infer logger_id from header for all data points
                let logger_id = raw_file.get_logger_id().ok();
                for data_point in &mut data_points {
                    data_point.logger_id = logger_id;
                }
                raw_file.data_points.extend(data_points);
            }
        }

        Ok(raw_file)
    }

    fn parse_data_row(
        record: &csv::StringRecord,
        column_names: &[String],
        firmware_version: &FirmwareVersion,
    ) -> Result<Vec<RawDataPoint>, ParseError> {
        let values: Vec<&str> = record.iter().collect();
        
        // Find required columns
        let timestamp_idx = Self::find_column_index(column_names, "TIMESTAMP")?;
        let record_idx = Self::find_column_index(column_names, "RECORD")?;
        
        // Parse timestamp
        let timestamp_str = values.get(timestamp_idx)
            .ok_or_else(|| ParseError::MissingColumn("TIMESTAMP".to_string()))?
            .trim_matches('"');
        
        let naive_dt = NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S")
            .map_err(|_| ParseError::InvalidTimestamp(timestamp_str.to_string()))?;
        let timestamp = DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc);

        // Parse record number
        let record_number = values.get(record_idx)
            .ok_or_else(|| ParseError::MissingColumn("RECORD".to_string()))?
            .parse::<u32>()
            .map_err(|_| ParseError::InvalidTimestamp("Invalid record number".to_string()))?;

        // Parse based on firmware version
        match firmware_version {
            FirmwareVersion::Firmware200_1 | FirmwareVersion::Firmware300_1 => {
                let single_point = Self::parse_single_sensor_row(values, column_names, timestamp, record_number)?;
                Ok(vec![single_point])
            },
            FirmwareVersion::Firmware200_2 => {
                let single_point = Self::parse_single_sensor_row(values, column_names, timestamp, record_number)?;
                Ok(vec![single_point])
            },
            FirmwareVersion::Firmware300_2 => {
                Self::parse_multi_sensor_row(values, column_names, timestamp, record_number)
            },
        }
    }

    fn parse_single_sensor_row(
        values: Vec<&str>,
        column_names: &[String],
        timestamp: DateTime<Utc>,
        record_number: u32,
    ) -> Result<RawDataPoint, ParseError> {
        let mut data_point = RawDataPoint {
            timestamp,
            record_number,
            battery_voltage: None,
            logger_id: None,
            sdi_address: None,
            alpha_outer: None,
            alpha_inner: None,
            beta_outer: None,
            beta_inner: None,
            tmax_outer: None,
            tmax_inner: None,
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
        };

        // Parse common fields
        data_point.battery_voltage = Self::parse_optional_float(&values, column_names, "BattV_Min");
        data_point.logger_id = Self::parse_optional_u32(&values, column_names, "id");
        data_point.sdi_address = Self::parse_optional_string(&values, column_names, "SDI0")
            .or_else(|| Self::parse_optional_string(&values, column_names, "SDI1"));

        // Parse sensor data - handle different column naming patterns
        data_point.alpha_outer = Self::parse_optional_float(&values, column_names, "AlphaOut0")
            .or_else(|| Self::parse_optional_float(&values, column_names, "AlphaOut1"));
        data_point.alpha_inner = Self::parse_optional_float(&values, column_names, "AlphaIn0")
            .or_else(|| Self::parse_optional_float(&values, column_names, "AlphaIn1"));
        data_point.beta_outer = Self::parse_optional_float(&values, column_names, "BetaOut0")
            .or_else(|| Self::parse_optional_float(&values, column_names, "BetaOut1"));
        data_point.beta_inner = Self::parse_optional_float(&values, column_names, "BetaIn0")
            .or_else(|| Self::parse_optional_float(&values, column_names, "BetaIn1"));
        data_point.tmax_outer = Self::parse_optional_float(&values, column_names, "tMaxTout0")
            .or_else(|| Self::parse_optional_float(&values, column_names, "tMaxTout1"));
        data_point.tmax_inner = Self::parse_optional_float(&values, column_names, "tMaxTin0")
            .or_else(|| Self::parse_optional_float(&values, column_names, "tMaxTin1"));

        Ok(data_point)
    }

    fn parse_multi_sensor_row(
        values: Vec<&str>,
        column_names: &[String],
        timestamp: DateTime<Utc>,
        record_number: u32,
    ) -> Result<Vec<RawDataPoint>, ParseError> {
        let mut data_points = Vec::new();
        
        // Detect how many sensors are present by looking for S0_, S1_, S2_, etc.
        let sensor_count = Self::detect_sensor_count(column_names);
        
        // Parse common fields once
        let battery_voltage = Self::parse_optional_float(&values, column_names, "Batt_volt");
        let panel_temp = Self::parse_optional_float(&values, column_names, "PTemp_C");
        
        // Create a data point for each sensor
        for sensor_idx in 0..sensor_count {
            let mut data_point = RawDataPoint {
                timestamp,
                record_number,
                battery_voltage,
                logger_id: None, // Will be set below
                sdi_address: Some(sensor_idx.to_string()), // SDI address corresponds to sensor index
                alpha_outer: None,
                alpha_inner: None,
                beta_outer: None,
                beta_inner: None,
                tmax_outer: None,
                tmax_inner: None,
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
            };

            // Parse sensor-specific data using S{sensor_idx}_ prefix
            let prefix = format!("S{}_", sensor_idx);
            
            // Core thermal measurements
            data_point.alpha_outer = Self::parse_optional_float(&values, column_names, &format!("{}AlpOut", prefix));
            data_point.alpha_inner = Self::parse_optional_float(&values, column_names, &format!("{}AlpInn", prefix));
            data_point.beta_outer = Self::parse_optional_float(&values, column_names, &format!("{}BetOut", prefix));
            data_point.beta_inner = Self::parse_optional_float(&values, column_names, &format!("{}BetInn", prefix));
            data_point.tmax_outer = Self::parse_optional_float(&values, column_names, &format!("{}tMxTout", prefix));
            data_point.tmax_inner = Self::parse_optional_float(&values, column_names, &format!("{}tMxTinn", prefix));

            // Extended temperature measurements (when available)
            data_point.temp_pre_downstream_outer = Self::parse_optional_float(&values, column_names, &format!("{}TpDsOut", prefix));
            data_point.temp_delta_downstream_outer = Self::parse_optional_float(&values, column_names, &format!("{}dTDsOut", prefix));
            data_point.temp_post_downstream_outer = Self::parse_optional_float(&values, column_names, &format!("{}TsDsOut", prefix));
            data_point.temp_pre_upstream_outer = Self::parse_optional_float(&values, column_names, &format!("{}TpUsOut", prefix));
            data_point.temp_delta_upstream_outer = Self::parse_optional_float(&values, column_names, &format!("{}dTUsOut", prefix));
            data_point.temp_post_upstream_outer = Self::parse_optional_float(&values, column_names, &format!("{}TsUsOut", prefix));
            
            data_point.temp_pre_downstream_inner = Self::parse_optional_float(&values, column_names, &format!("{}TpDsInn", prefix));
            data_point.temp_delta_downstream_inner = Self::parse_optional_float(&values, column_names, &format!("{}dTDsInn", prefix));
            data_point.temp_post_downstream_inner = Self::parse_optional_float(&values, column_names, &format!("{}TsDsInn", prefix));
            data_point.temp_pre_upstream_inner = Self::parse_optional_float(&values, column_names, &format!("{}TpUsInn", prefix));
            data_point.temp_delta_upstream_inner = Self::parse_optional_float(&values, column_names, &format!("{}dTUsInn", prefix));
            data_point.temp_post_upstream_inner = Self::parse_optional_float(&values, column_names, &format!("{}TsUsInn", prefix));
            
            data_point.tmax_upstream_outer = Self::parse_optional_float(&values, column_names, &format!("{}tMxTUsO", prefix));
            data_point.tmax_upstream_inner = Self::parse_optional_float(&values, column_names, &format!("{}tMxTUsI", prefix));

            // Only add the data point if it has at least some sensor data
            if Self::has_sensor_data(&data_point) {
                data_points.push(data_point);
            }
        }
        
        Ok(data_points)
    }
    
    fn detect_sensor_count(column_names: &[String]) -> usize {
        let mut max_sensor_idx = 0;
        
        for col_name in column_names {
            if let Some(captures) = regex::Regex::new(r"^S(\d+)_").unwrap().captures(col_name) {
                if let Some(idx_str) = captures.get(1) {
                    if let Ok(idx) = idx_str.as_str().parse::<usize>() {
                        max_sensor_idx = max_sensor_idx.max(idx);
                    }
                }
            }
        }
        
        // Return count (max index + 1), with minimum of 1
        (max_sensor_idx + 1).max(1)
    }
    
    fn has_sensor_data(data_point: &RawDataPoint) -> bool {
        // Check if the data point has any non-None sensor measurements
        data_point.alpha_outer.is_some() || 
        data_point.alpha_inner.is_some() || 
        data_point.beta_outer.is_some() || 
        data_point.beta_inner.is_some() || 
        data_point.tmax_outer.is_some() || 
        data_point.tmax_inner.is_some() ||
        data_point.temp_pre_downstream_outer.is_some() ||
        data_point.temp_delta_downstream_outer.is_some() ||
        data_point.temp_post_downstream_outer.is_some()
    }

    fn find_column_index(column_names: &[String], target: &str) -> Result<usize, ParseError> {
        column_names.iter()
            .position(|col| col == target)
            .ok_or_else(|| ParseError::MissingColumn(target.to_string()))
    }

    fn parse_optional_float(values: &[&str], column_names: &[String], column_name: &str) -> Option<f64> {
        let idx = column_names.iter().position(|col| col == column_name)?;
        let value_str = values.get(idx)?.trim_matches('"');
        
        if value_str == "NAN" || value_str == "-99" || value_str.is_empty() {
            return None;
        }
        
        value_str.parse().ok()
    }

    fn parse_optional_u32(values: &[&str], column_names: &[String], column_name: &str) -> Option<u32> {
        let idx = column_names.iter().position(|col| col == column_name)?;
        let value_str = values.get(idx)?.trim_matches('"');
        
        if value_str == "NAN" || value_str == "-99" || value_str.is_empty() {
            return None;
        }
        
        value_str.parse().ok()
    }

    fn parse_optional_string(values: &[&str], column_names: &[String], column_name: &str) -> Option<String> {
        let idx = column_names.iter().position(|col| col == column_name)?;
        let value_str = values.get(idx)?.trim_matches('"');
        
        if value_str == "NAN" || value_str == "-99" || value_str.is_empty() {
            return None;
        }
        
        Some(value_str.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn test_parse_cr200_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, r#""TOA5","CR200Series_601","CR200X","No_SN","CR200X.Std.02","Sapflux2sensors1.CR2","63887","Table1""#).unwrap();
        writeln!(temp_file, r#""TIMESTAMP","RECORD","BattV_Min","id","SDI0","SapFlwTot0","VhOuter0","VhInner0","AlphaOut0","AlphaIn0","BetaOut0","BetaIn0","tMaxTout0","tMaxTin0""#).unwrap();
        writeln!(temp_file, r#""TS","RN","Volts","","","literPerHo","heatVeloci","heatVeloci","logTRatio","logTRatio","logTRatio","logTRatio","second","second""#).unwrap();
        writeln!(temp_file, r#""","","Min","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp""#).unwrap();
        writeln!(temp_file, r#""2024-04-02 11:30:00",0,12.9005,601,0,0.084,0.86,1.52,0.07594,0.1341,0.0577,0.08306,50.896,36.452"#).unwrap();
        
        let result = CsvParser::parse_file(temp_file.path().to_path_buf());
        assert!(result.is_ok());
        
        let raw_file = result.unwrap();
        assert_eq!(raw_file.data_points.len(), 1);
        assert_eq!(raw_file.header.table_name, "Table1");
        assert_eq!(raw_file.data_points[0].logger_id, Some(601));
    }
    
    #[test]
    fn test_parse_multi_sensor_file() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, r#""TOA5","CR300Series_421","CR300","27030","CR300-RF407.Std.10.03","CPU:sapflux_2sensor_CR300_30min.cr300","60975","SapFlowAll""#).unwrap();
        writeln!(temp_file, r#""TIMESTAMP","RECORD","Batt_volt","PTemp_C","S0_AlpOut","S0_AlpInn","S0_BetOut","S0_BetInn","S1_AlpOut","S1_AlpInn","S1_BetOut","S1_BetInn""#).unwrap();
        writeln!(temp_file, r#""TS","RN","","","ratio","ratio","ratio","ratio","ratio","ratio","ratio","ratio""#).unwrap();
        writeln!(temp_file, r#""","","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp""#).unwrap();
        writeln!(temp_file, r#""2025-05-19 18:30:00",0,10.19,27.12,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8"#).unwrap();
        
        let result = CsvParser::parse_file(temp_file.path().to_path_buf());
        assert!(result.is_ok());
        
        let raw_file = result.unwrap();
        assert_eq!(raw_file.data_points.len(), 2); // Should have 2 sensors
        assert_eq!(raw_file.header.table_name, "SapFlowAll");
        assert_eq!(raw_file.data_points[0].logger_id, Some(421));
        assert_eq!(raw_file.data_points[0].sdi_address, Some("0".to_string()));
        assert_eq!(raw_file.data_points[1].sdi_address, Some("1".to_string()));
        assert_eq!(raw_file.data_points[0].alpha_outer, Some(0.1));
        assert_eq!(raw_file.data_points[1].alpha_outer, Some(0.5));
    }
}