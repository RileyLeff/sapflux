// crates/sapflux-core/src/types.rs

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{
    decode::Decode,
    encode::{Encode, IsNull},
    postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef},
    types::{Json, Type},
    FromRow, Postgres,
};
use std::error::Error as StdError;
use uuid::Uuid;

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Project {
    pub id: i32,
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Sensor {
    pub id: i32,
    pub sensor_id: String,
    pub downstream_probe_distance_cm: f64,
    pub upstream_probe_distance_cm: f64,
    pub thermistor_depth_1_mm: i32,
    pub thermistor_depth_2_mm: i32,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Parameter {
    pub id: i32,
    pub name: String,
    pub value: f64,
    pub unit: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoastalMonitoringAttributes {
    pub site_name: String,
    pub zone_name: String,
    pub plot_name: String,
    pub species: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StemflowAttributes {
    pub health_status: String,
    pub collar_present: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "project_type")]
pub enum DeploymentAttributes {
    CoastalMonitoring(CoastalMonitoringAttributes),
    Stemflow(StemflowAttributes),
}

#[derive(Debug, Clone, FromRow)]
pub struct Deployment {
    pub id: Uuid,
    pub start_time_utc: DateTime<Utc>,
    pub end_time_utc: Option<DateTime<Utc>>,
    pub datalogger_id: i32,
    pub sdi_address: SdiAddress,
    pub tree_id: String,
    pub project_id: i32,
    pub sensor_id: i32,
    pub attributes: Json<DeploymentAttributes>,
}

#[derive(Debug, Clone)]
pub struct NewDeployment {
    pub start_time_utc: DateTime<Utc>,
    pub datalogger_id: i32,
    pub sdi_address: SdiAddress,
    pub tree_id: String,
    pub project_id: i32,
    pub sensor_id: i32,
    pub attributes: DeploymentAttributes,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum FileSchema {
    CR300MultiSensor,
    CRLegacySingleSensor,
}

impl FileSchema {
    pub fn as_str(&self) -> &'static str {
        match self {
            FileSchema::CR300MultiSensor => "CR300_MULTI_SENSOR",
            FileSchema::CRLegacySingleSensor => "CR_LEGACY_SINGLE_SENSOR",
        }
    }

    pub fn from_str(s: &str) -> Result<Self, Box<dyn StdError + Send + Sync>> {
        match s {
            "CR300_MULTI_SENSOR" => Ok(FileSchema::CR300MultiSensor),
            "CR_LEGACY_SINGLE_SENSOR" => Ok(FileSchema::CRLegacySingleSensor),
            _ => Err(format!("Invalid FileSchema variant: {}", s).into()),
        }
    }
}

impl Type<Postgres> for FileSchema {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("TEXT")
    }
}

impl Encode<'_, Postgres> for FileSchema {
    fn encode_by_ref(
        &self,
        buf: &mut PgArgumentBuffer,
    ) -> Result<IsNull, Box<dyn StdError + Send + Sync + 'static>> {
        let s = self.as_str();
        <String as Encode<Postgres>>::encode(s.to_string(), buf)
    }
}

impl Decode<'_, Postgres> for FileSchema {
    fn decode(value: PgValueRef<'_>) -> Result<Self, Box<dyn StdError + Send + Sync + 'static>> {
        let s = <&str as Decode<Postgres>>::decode(value)?;
        FileSchema::from_str(s)
    }
}

#[derive(Debug, FromRow)]
pub struct DstTransition {
    pub id: i32,
    pub transition_action: String,
    pub ts_local: NaiveDateTime,
}

#[derive(Debug, Clone, sqlx::Type)]
#[sqlx(transparent)]
pub struct SdiAddress(String);

impl SdiAddress {
    pub fn new(addr: &str) -> Result<Self, String> {
        if addr.is_empty() { return Err("SDI-12 address cannot be empty.".to_string()); }
        if addr.chars().count() != 1 { return Err(format!("SDI-12 address must be a single character, but got '{}'", addr)); }
        let c = addr.chars().next().unwrap();
        if !c.is_ascii_alphanumeric() { return Err(format!("SDI-12 address must be alphanumeric (a-z, A-Z, 0-9), but got '{}'", c)); }
        Ok(Self(addr.to_string()))
    }
    
    pub fn as_str(&self) -> &str {
        &self.0
    }
}