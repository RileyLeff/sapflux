use crate::error::{PipelineError, Result};
use polars::prelude::*;
use std::path::Path;

pub fn scan_file_lazy(file_path: &Path) -> Result<LazyFrame> {
    let path_str = file_path.to_str().ok_or_else(|| {
        PipelineError::Processing(format!("Invalid path: {}", file_path.display()))
    })?;

    // Create the LazyFrame by scanning the CSV, skipping the 4-line header
    let lf = LazyCsvReader::new(path_str)
        .with_skip_rows(4)
        .finish()?
        .with_column(lit(path_str).alias("file_origin")); // Add file origin column

    Ok(lf)
}