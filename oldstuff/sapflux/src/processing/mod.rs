// In src/processing/mod.rs

use crate::error::{PipelineError, Result};
use polars::prelude::*;
use std::path::Path;
use glob::glob;

/// Discovers all data files in a directory and builds a single, combined LazyFrame.
/// This function recursively finds all files and then filters out common metadata/status files.
pub fn build_master_lazyframe(dir: &Path) -> Result<LazyFrame> {
    // 1. Create a single, recursive glob pattern to get ALL entries (files and dirs).
    let pattern = dir.join("**/*");
    let pattern_str = pattern.to_str().ok_or_else(|| {
        PipelineError::Processing(format!("Invalid glob pattern from path: {}", dir.display()))
    })?;

    println!("Searching for all files recursively with pattern: {}", pattern_str);

    // 2. Define the words that identify files to be skipped.
    // We will check against a lowercase version of the path for case-insensitivity.
    let forbidden_substrings = ["public", "status", "datatableinfo"];

    let mut lazy_frames: Vec<LazyFrame> = Vec::new();

    for entry in glob(pattern_str)? {
        let path = entry?;

        // 3. Apply filtering logic.
        // First, skip directories themselves.
        if path.is_dir() {
            continue;
        }

        // Now, check the file path against our forbidden words.
        let path_str_lower = path.to_string_lossy().to_lowercase();
        let should_skip = forbidden_substrings
            .iter()
            .any(|word| path_str_lower.contains(word));

        if should_skip {
            println!("Skipping metadata file: {}", path.display());
            continue;
        }

        // 4. If the file has not been skipped, process it.
        println!("Found data file: {}", path.display());
        let lf = crate::parsers::csv_parser::scan_file_lazy(&path)?;
        lazy_frames.push(lf);
    }

    if lazy_frames.is_empty() {
        return Err(PipelineError::Processing(
            "No valid data files found after filtering.".to_string(),
        ));
    }

    println!("\nFound a total of {} valid data files.", lazy_frames.len());

    // 5. Concatenate all resulting LazyFrames into one.
    let master_lf = concat(
        lazy_frames,
        UnionArgs {
            rechunk: false,
            parallel: true,
            ..Default::default()
        },
    )?;

    Ok(master_lf)
}


// The rest of the file (clean_data, apply_dst_correction) remains the same.
pub fn clean_data(lf: LazyFrame) -> Result<LazyFrame> {
    println!("Applying data cleaning (stubbed)...");
    Ok(lf)
}

pub fn apply_dst_correction(lf: LazyFrame) -> Result<LazyFrame> {
    println!("Applying DST correction (stubbed)...");
    Ok(lf)
}