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

    // THIS IS THE NEW VARIANT
    #[error("CSV parsing error: {0}")]
    Csv(#[from] csv::Error),

    #[error("Data processing error: {0}")]
    Processing(String),
}

pub type Result<T> = std::result::Result<T, PipelineError>;