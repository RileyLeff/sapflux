// crates/sapflux-core/src/types.rs

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::{
    decode::Decode,
    encode::{Encode, IsNull},
    postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef},
    types::Type,
    FromRow, Postgres,
};
use std::error::Error as StdError; // Use an alias to avoid name clashes
use uuid::Uuid;

// --- Deployment Struct (unchanged) ---
#[derive(Debug, Deserialize, FromRow)]
pub struct Deployment {
    pub id: Uuid,
    pub start_time_utc: DateTime<Utc>,
    pub end_time_utc: Option<DateTime<Utc>>,
    pub datalogger_id: i32,
    pub sensor_type: String,
    pub tree_id: String,
    pub site_name: Option<String>,
}


// --- FileSchema Enum and Helpers (unchanged) ---
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


// --- CORRECTED SQLX TRAIT IMPLEMENTATIONS ---

// Implement Type<Postgres> for our enum
impl Type<Postgres> for FileSchema {
    fn type_info() -> PgTypeInfo {
        // We are mapping our enum to a TEXT column in Postgres.
        PgTypeInfo::with_name("TEXT")
    }
}

// Implement encoding (Rust enum -> Postgres TEXT)
impl<'q> Encode<'q, Postgres> for FileSchema {
    // CORRECTED: The signature MUST return a Result.
    fn encode_by_ref(
        &self,
        buf: &mut PgArgumentBuffer,
    ) -> Result<IsNull, Box<dyn StdError + Send + Sync + 'static>> {
        let s = self.as_str();
        // The underlying string encoder returns a Result, which we now
        // correctly propagate up to the caller.
        <String as Encode<Postgres>>::encode(s.to_string(), buf)
    }
}

// Implement decoding (Postgres TEXT -> Rust enum)
impl<'r> Decode<'r, Postgres> for FileSchema {
    // This implementation was already correct.
    fn decode(value: PgValueRef<'r>) -> Result<Self, Box<dyn StdError + Send + Sync + 'static>> {
        let s = <&str as Decode<Postgres>>::decode(value)?;
        FileSchema::from_str(s)
    }
}