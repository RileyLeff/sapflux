use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::types::sensor::SensorType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataloggerModel {
    CR200,
    CR300,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FirmwareVersion {
    Firmware200_1,
    Firmware200_2,
    Firmware300_1,
    Firmware300_2,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SdiAddress(pub String);

impl SdiAddress {
    pub fn new(addr: String) -> Result<Self, String> {
        if addr.len() == 1 && addr.chars().all(|c| c.is_alphanumeric()) {
            Ok(Self(addr))
        } else {
            Err("SDI-12 address must be single alphanumeric character".to_string())
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareContext {
    pub datalogger_model: DataloggerModel,
    pub datalogger_id: u32,
    pub firmware_version: FirmwareVersion,
    pub sensor_type: SensorType,
    pub sdi_address: SdiAddress,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeasurementContext {
    pub tree_id: String,
    pub site_name: Option<String>,
    pub zone_name: Option<String>,
    pub plot_name: Option<String>,
    pub tree_species: Option<String>,
    pub health_status: Option<String>,
    pub collar_present: Option<bool>,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Deployment {
    pub id: Uuid,
    pub start_time_utc: DateTime<Utc>,
    pub end_time_utc: Option<DateTime<Utc>>, // Computed based on superseding deployments
    pub hardware: HardwareContext,
    pub measurement: MeasurementContext,
}

impl Deployment {
    pub fn new(
        start_time_utc: DateTime<Utc>,
        hardware: HardwareContext,
        measurement: MeasurementContext,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            start_time_utc,
            end_time_utc: None,
            hardware,
            measurement,
        }
    }

    pub fn is_active(&self) -> bool {
        self.end_time_utc.is_none()
    }

    pub fn logger_sdi_key(&self) -> (u32, String) {
        (self.hardware.datalogger_id, self.hardware.sdi_address.0.clone())
    }
}

#[derive(Debug)]
pub struct DeploymentValidationError {
    pub message: String,
}

impl std::fmt::Display for DeploymentValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Deployment validation error: {}", self.message)
    }
}

impl std::error::Error for DeploymentValidationError {}

pub fn validate_deployments(deployments: &[Deployment]) -> Result<(), DeploymentValidationError> {
    for deployment in deployments {
        if let Some(end_time) = deployment.end_time_utc {
            if end_time <= deployment.start_time_utc {
                return Err(DeploymentValidationError {
                    message: format!(
                        "Deployment {} has end time before or equal to start time",
                        deployment.id
                    ),
                });
            }
        }
    }

    let mut concurrent_check: std::collections::HashMap<(u32, String), Vec<&Deployment>> = 
        std::collections::HashMap::new();
    
    for deployment in deployments {
        let key = deployment.logger_sdi_key();
        concurrent_check.entry(key).or_default().push(deployment);
    }

    for ((logger_id, sdi_addr), deployments_for_key) in concurrent_check {
        for i in 0..deployments_for_key.len() {
            for j in (i + 1)..deployments_for_key.len() {
                let dep1 = deployments_for_key[i];
                let dep2 = deployments_for_key[j];
                
                let overlap = match (dep1.end_time_utc, dep2.end_time_utc) {
                    (Some(end1), Some(end2)) => {
                        (dep1.start_time_utc < end2) && (dep2.start_time_utc < end1)
                    }
                    (None, _) | (_, None) => {
                        let (active, other) = if dep1.end_time_utc.is_none() { (dep1, dep2) } else { (dep2, dep1) };
                        other.end_time_utc.map_or(true, |end| active.start_time_utc < end)
                    }
                };
                
                if overlap {
                    return Err(DeploymentValidationError {
                        message: format!(
                            "Overlapping deployments for logger {} SDI address {}: {} and {}",
                            logger_id, sdi_addr, dep1.id, dep2.id
                        ),
                    });
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_sdi_address_validation() {
        assert!(SdiAddress::new("0".to_string()).is_ok());
        assert!(SdiAddress::new("A".to_string()).is_ok());
        assert!(SdiAddress::new("z".to_string()).is_ok());
        assert!(SdiAddress::new("01".to_string()).is_err());
        assert!(SdiAddress::new("".to_string()).is_err());
    }

    #[test]
    fn test_deployment_validation() {
        let hardware = HardwareContext {
            datalogger_model: DataloggerModel::CR300,
            datalogger_id: 401,
            firmware_version: FirmwareVersion::Firmware300_1,
            sensor_type: SensorType::ImplexxNew,
            sdi_address: SdiAddress::new("0".to_string()).unwrap(),
        };

        let measurement = MeasurementContext {
            tree_id: "tree1".to_string(),
            site_name: Some("Brownsville".to_string()),
            zone_name: None,
            plot_name: None,
            tree_species: None,
            health_status: None,
            collar_present: None,
            notes: None,
        };

        let deployment1 = Deployment::new(
            Utc::now(),
            hardware.clone(),
            measurement.clone(),
        );

        assert!(validate_deployments(&[deployment1]).is_ok());
    }
}