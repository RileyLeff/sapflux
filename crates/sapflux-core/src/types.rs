// crates/sapflux-core/src/types.rs

use chrono::{DateTime, NaiveDateTime, Utc};
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

#[derive(Debug, FromRow)]
pub struct DstTransition {
    pub id: i32,
    pub transition_action: String,
    pub ts_local: NaiveDateTime,
}

// A newtype wrapper for SDI-12 addresses that guarantees validity at compile time.
#[derive(Debug, sqlx::Type)] // The derive macro itself
#[sqlx(transparent)]        // The helper attribute for the derive macro
pub struct SdiAddress(String);

impl SdiAddress {
    /// Creates a new `SdiAddress`, returning an error if the input is invalid.
    pub fn new(addr: &str) -> Result<Self, String> {
        // Rule 1: Must not be empty.
        if addr.is_empty() {
            return Err("SDI-12 address cannot be empty.".to_string());
        }

        // Rule 2: Must be a single character.
        if addr.chars().count() != 1 {
            return Err(format!(
                "SDI-12 address must be a single character, but got '{}'",
                addr
            ));
        }

        // Rule 3: The character must be ASCII alphanumeric.
        let c = addr.chars().next().unwrap();
        if !c.is_ascii_alphanumeric() {
            return Err(format!(
                "SDI-12 address must be alphanumeric (a-z, A-Z, 0-9), but got '{}'",
                c
            ));
        }

        // If all rules pass, create the newtype instance.
        Ok(Self(addr.to_string()))
    }
    
    // Provides read-only access to the inner string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
