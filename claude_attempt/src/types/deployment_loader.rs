use crate::types::{Deployment, HardwareContext, MeasurementContext, SensorType, DataloggerModel, FirmwareVersion, SdiAddress};
use chrono::{DateTime, Utc, NaiveDateTime};
use csv::ReaderBuilder;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum DeploymentLoadError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("CSV parsing error: {0}")]
    Csv(#[from] csv::Error),
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),
    #[error("Invalid sensor type: {0}")]
    InvalidSensorType(String),
    #[error("Invalid SDI address: {0}")]
    InvalidSdiAddress(String),
    #[error("Missing required field: {0}")]
    MissingField(String),
}

pub struct DeploymentLoader;

impl DeploymentLoader {
    pub fn load_monitoring_deployments<P: AsRef<Path>>(
        path: P,
    ) -> Result<Vec<Deployment>, DeploymentLoadError> {
        let file = File::open(path)?;
        let mut reader = ReaderBuilder::new().from_reader(file);
        let mut deployments = Vec::new();

        for result in reader.deserialize() {
            let record: MonitoringDeploymentRecord = result?;
            let deployment = Self::monitoring_record_to_deployment(record)?;
            deployments.push(deployment);
        }

        // Compute end times based on superseding deployments
        Self::compute_superseding_end_times(&mut deployments);

        Ok(deployments)
    }

    pub fn load_stemflow_deployments<P: AsRef<Path>>(
        path: P,
    ) -> Result<Vec<Deployment>, DeploymentLoadError> {
        let file = File::open(path)?;
        let mut reader = ReaderBuilder::new().from_reader(file);
        let mut deployments = Vec::new();

        for result in reader.deserialize() {
            let record: StemflowDeploymentRecord = result?;
            let deployment = Self::stemflow_record_to_deployment(record)?;
            deployments.push(deployment);
        }

        // Compute end times based on superseding deployments
        Self::compute_superseding_end_times(&mut deployments);

        Ok(deployments)
    }

    fn monitoring_record_to_deployment(
        record: MonitoringDeploymentRecord,
    ) -> Result<Deployment, DeploymentLoadError> {
        let start_time_utc = Self::parse_timestamp(&record.start_ts_utc)?;
        let sensor_type = Self::parse_sensor_type(&record.sensor_type)?;
        let sdi_address = SdiAddress::new(record.sdi.to_string())
            .map_err(|e| DeploymentLoadError::InvalidSdiAddress(e))?;

        // Infer datalogger model and firmware from logger_id ranges
        let (datalogger_model, firmware_version) = Self::infer_hardware_from_logger_id(record.logger_id);

        let hardware = HardwareContext {
            datalogger_model,
            datalogger_id: record.logger_id,
            firmware_version,
            sensor_type,
            sdi_address,
        };

        let measurement = MeasurementContext {
            tree_id: record.tree_id,
            site_name: Some(record.site),
            zone_name: Some(record.zone),
            plot_name: Some(record.plot),
            tree_species: Some(record.spp),
            health_status: None,
            collar_present: None,
            notes: None,
        };

        Ok(Deployment {
            id: Uuid::new_v4(),
            start_time_utc,
            end_time_utc: None, // Will be computed later
            hardware,
            measurement,
        })
    }

    fn stemflow_record_to_deployment(
        record: StemflowDeploymentRecord,
    ) -> Result<Deployment, DeploymentLoadError> {
        let start_time_utc = Self::parse_timestamp(&record.start_ts_utc)?;
        let sensor_type = Self::parse_sensor_type(&record.sensor_type)?;
        let sdi_address = SdiAddress::new(record.sdi.to_string())
            .map_err(|e| DeploymentLoadError::InvalidSdiAddress(e))?;

        // Infer datalogger model and firmware from logger_id ranges
        let (datalogger_model, firmware_version) = Self::infer_hardware_from_logger_id(record.logger_id);

        let hardware = HardwareContext {
            datalogger_model,
            datalogger_id: record.logger_id,
            firmware_version,
            sensor_type,
            sdi_address,
        };

        let measurement = MeasurementContext {
            tree_id: record.tree_id,
            site_name: Some(record.site),
            zone_name: None,
            plot_name: None,
            tree_species: Some(record.spp),
            health_status: Some(record.health),
            collar_present: Some(record.collar == 1),
            notes: record.robyn_label,
        };

        Ok(Deployment {
            id: Uuid::new_v4(),
            start_time_utc,
            end_time_utc: None, // Will be computed later
            hardware,
            measurement,
        })
    }

    fn compute_superseding_end_times(deployments: &mut [Deployment]) {
        // Group deployments by logger_id and sdi_address
        let mut grouped: HashMap<(u32, String), Vec<&mut Deployment>> = HashMap::new();
        
        for deployment in deployments.iter_mut() {
            let key = deployment.logger_sdi_key();
            grouped.entry(key).or_default().push(deployment);
        }

        // For each group, sort by start time and set end times
        for (_, group) in grouped.iter_mut() {
            group.sort_by_key(|d| d.start_time_utc);
            
            for i in 0..group.len() - 1 {
                // Set end time to start time of next deployment
                group[i].end_time_utc = Some(group[i + 1].start_time_utc);
            }
            // Last deployment remains active (end_time_utc = None)
        }
    }

    fn parse_timestamp(ts_str: &str) -> Result<DateTime<Utc>, DeploymentLoadError> {
        let naive_dt = NaiveDateTime::parse_from_str(ts_str, "%Y-%m-%d %H:%M:%S")
            .map_err(|_| DeploymentLoadError::InvalidTimestamp(ts_str.to_string()))?;
        Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc))
    }

    fn parse_sensor_type(sensor_str: &str) -> Result<SensorType, DeploymentLoadError> {
        match sensor_str.to_lowercase().as_str() {
            "implexx_old" => Ok(SensorType::ImplexxOld),
            "implexx_new" | "implex_new" => Ok(SensorType::ImplexxNew), // Handle typo in stemflow data
            _ => Err(DeploymentLoadError::InvalidSensorType(sensor_str.to_string())),
        }
    }

    fn infer_hardware_from_logger_id(logger_id: u32) -> (DataloggerModel, FirmwareVersion) {
        match logger_id {
            // CR200 range
            300..=399 => (DataloggerModel::CR200, FirmwareVersion::Firmware200_1),
            500..=599 => (DataloggerModel::CR200, FirmwareVersion::Firmware200_1),
            600..=699 => (DataloggerModel::CR200, FirmwareVersion::Firmware200_1),
            
            // CR300 range
            400..=499 => (DataloggerModel::CR300, FirmwareVersion::Firmware300_1),
            700..=799 => (DataloggerModel::CR300, FirmwareVersion::Firmware300_1),
            
            // Default to CR300 new firmware for unknown ranges
            _ => (DataloggerModel::CR300, FirmwareVersion::Firmware300_2),
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct MonitoringDeploymentRecord {
    logger_id: u32,
    sdi: u32,
    start_ts_utc: String,
    sensor_type: String,
    site: String,
    zone: String,
    plot: String,
    tree_id: String,
    spp: String,
}

#[derive(Debug, serde::Deserialize)]
struct StemflowDeploymentRecord {
    logger_id: u32,
    sdi: u32,
    start_ts_utc: String,
    sensor_type: String,
    site: String,
    tree_id: String,
    spp: String,
    robyn_label: Option<String>,
    collar: u32,
    health: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn test_load_monitoring_deployments() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "logger_id,sdi,start_ts_utc,sensor_type,site,zone,plot,tree_id,spp").unwrap();
        writeln!(temp_file, "301,1,2021-01-30 0:00:00,implexx_old,brnv,H,5,560,pintae").unwrap();
        
        let result = DeploymentLoader::load_monitoring_deployments(temp_file.path());
        assert!(result.is_ok());
        
        let deployments = result.unwrap();
        assert_eq!(deployments.len(), 1);
        assert_eq!(deployments[0].hardware.datalogger_id, 301);
        assert_eq!(deployments[0].measurement.tree_id, "560");
    }
}