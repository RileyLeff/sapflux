// src/main.rs

use sapflux::error::Result;
use sapflux::processing;
use polars::prelude::{ParquetWriteOptions, SinkTarget, SinkOptions};
use std::path::Path;

#[tokio::main]
async fn main() -> Result<()> {
    println!("--- Sap Flux Pipeline Rewrite ---");

    let data_dir = Path::new("/Users/rileyleff/Documents/dev/sapflux/oldstuff/data/raw");

    // 1. Build the master LazyFrame from all data files.
    let master_lf = processing::build_master_lazyframe(data_dir)?;

    // 2. Apply cleaning steps.
    let cleaned_lf = processing::clean_data(master_lf)?;

    // 3. Apply DST correction logic.
    let corrected_lf = processing::apply_dst_correction(cleaned_lf)?;

    // 6. Execute the plan and sink to a Parquet file.
    println!("\nExecuting the lazy plan and writing to output.parquet...");
    let output_path = Path::new("output");
    std::fs::create_dir_all(output_path)?;

    // Correct the call to sink_parquet for Polars 0.48.1
    let sink_result = corrected_lf.sink_parquet(
        // THIS IS THE CORRECTED LINE: .into() is added at the end
        SinkTarget::Path(output_path.join("output.parquet").into()),
        ParquetWriteOptions::default(),
        None, // No cloud options needed for local file
        SinkOptions::default(),
    );

    // sink_parquet is synchronous, so we don't .await it.
    let _ = sink_result?;

    println!("✅ Pipeline finished successfully!");
    Ok(())
}