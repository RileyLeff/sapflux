use thiserror::Error;

#[derive(Error, Debug)]
pub enum PipelineError {
    #[error("Polars operation failed: {0}")]
    Polars(#[from] polars::prelude::PolarsError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("File discovery pattern error: {0}")]
    Glob(#[from] glob::PatternError),

    #[error("Error while accessing a file from the glob pattern: {0}")]
    GlobError(#[from] glob::GlobError),

    #[error("Data processing error: {0}")]
    Processing(String),
}

// This allows us to use `Result<T, PipelineError>` throughout our app.
pub type Result<T> = std::result::Result<T, PipelineError>;