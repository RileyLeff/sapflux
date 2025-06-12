// crates/sapflux-core/src/processing/types.rs

// These structs are used internally by the processing pipeline.
// `pub(crate)` makes them visible to other modules within the `sapflux-core` crate.

use crate::types::FileSchema;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde_json;

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct RawFileRecord {
    pub(crate) file_hash: String,
    pub(crate) file_content: Vec<u8>,
    pub(crate) detected_schema_name: FileSchema,
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct ManualFix {
    pub(crate) file_hash: String,
    pub(crate) action: String,
    pub(crate) value: serde_json::Value,
    pub(crate) description: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct DstTransition {
    pub(crate) transition_action: String,
    pub(crate) ts_local: NaiveDateTime,
}

#[derive(Debug, sqlx::FromRow)]
pub(crate) struct DeploymentInfo {
    pub(crate) datalogger_id: i32,
    pub(crate) sdi_address: String,
    pub(crate) project_name: String,
    pub(crate) tree_id: String,
    pub(crate) sensor_id: String,
    pub(crate) start_time_utc: DateTime<Utc>,
    pub(crate) end_time_utc: Option<DateTime<Utc>>,
}