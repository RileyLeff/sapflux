use serde::Deserialize;

// We'll add more fields here as needed (SensorType, etc.)
// This struct will be loaded EAGERLY because the deployment metadata is small.
#[derive(Debug, Deserialize)]
pub struct Deployment {
    pub logger_id: u32,
    pub sdi: u32,
    pub start_ts_utc: String, // Keep it simple for now
    pub sensor_type: String,
    pub tree_id: String,
}