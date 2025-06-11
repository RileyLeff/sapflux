// crates/sapflux-core/src/error.rs

use thiserror::Error;

#[derive(Error, Debug)]
pub enum PipelineError {
    #[error("Database query failed: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("File I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Environment variable error: {0}")]
    EnvVar(#[from] std::env::VarError),

    #[error("Polars operation failed: {0}")]
    Polars(#[from] polars::error::PolarsError),

    #[error("CSV parsing error: {0}")]
    Csv(#[from] csv::Error),

    #[error("Validation failed: {0}")]
    Validation(String),

    #[error("JSON serialization/deserialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Data processing error: {0}")]
    Processing(String),
}

pub type Result<T> = std::result::Result<T, PipelineError>;