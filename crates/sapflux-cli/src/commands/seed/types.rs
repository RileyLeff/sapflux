// crates/sapflux-cli/src/commands/seed/types.rs

use serde::Deserialize;
use chrono::{DateTime, Utc, NaiveDateTime};
use std::collections::HashMap;

// --- Projects ---
#[derive(Debug, Deserialize)]
pub struct ProjectsFile {
    #[serde(rename = "project")]
    pub projects: Vec<ProjectSeed>,
}

#[derive(Debug, Deserialize)]
pub struct ProjectSeed {
    pub name: String,
    pub description: Option<String>,
}

// --- Sensors ---
#[derive(Debug, Deserialize)]
pub struct SensorsFile {
    #[serde(rename = "sensor")]
    pub sensors: Vec<SensorSeed>,
}

#[derive(Debug, Deserialize)]
pub struct SensorSeed {
    pub id: String,
    pub downstream_probe_distance_cm: f64,
    pub upstream_probe_distance_cm: f64,
    pub thermistor_depth_1_mm: i32,
    pub thermistor_depth_2_mm: i32,
}

// --- Parameters ---
#[derive(Debug, Deserialize)]
pub struct ParametersFile {
    pub parameters: HashMap<String, ParameterValueSeed>,
}

#[derive(Debug, Deserialize)]
pub struct ParameterValueSeed {
    pub value: f64,
    pub unit: Option<String>,
    pub description: Option<String>,
}


// --- DST Transitions ---
#[derive(Debug, Deserialize)]
pub struct DstTransitionsFile {
    #[serde(rename = "transitions")]
    pub transitions: Vec<DstTransitionSeed>,
}

#[derive(Debug, Deserialize)]
pub struct DstTransitionSeed {
    pub action: String,
    pub ts_local: NaiveDateTime,
}

// --- Deployments ---
#[derive(Debug, Deserialize)]
pub struct DeploymentsFile {
    #[serde(rename = "deployment")]
    pub deployments: Vec<DeploymentSeed>,
}

#[derive(Debug, Deserialize)]
pub struct DeploymentSeed {
    pub project_name: String,
    pub datalogger_id: i32,
    pub sdi_address: String,
    pub start_time_utc: DateTime<Utc>,
    pub sensor_id: String,
    pub tree_id: String,
    pub attributes: toml::Value,
}