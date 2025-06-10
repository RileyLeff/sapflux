// crates/sapflux-cli/src/main.rs

use anyhow::Result;
use clap::Parser;
use sapflux_core::ingestion;
use std::path::PathBuf;

/// A CLI for the Sapflow Data Pipeline
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand, Debug)]
enum Commands {
    /// Ingests raw data files from a directory into the database.
    Ingest {
        /// The path to the directory containing raw sapflow data.
        #[arg(short, long)]
        dir: PathBuf,
    },
    // We will add a `Process` subcommand later.
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().expect(".env file not found");
    let cli = Cli::parse();

    let pool = sapflux_core::db::connect().await?;

    match cli.command {
        Commands::Ingest { dir } => {
            println!("Starting ingestion from directory: {}", dir.display());

            let pattern = dir.join("**/*");
            let pattern_str = pattern.to_str().expect("Invalid path pattern");

            // --- THIS IS THE UPDATED LOGIC ---
            let mut success_count = 0;
            let mut failure_count = 0;

            for entry in glob::glob(pattern_str)? {
                let path = match entry {
                    Ok(path) => path,
                    Err(e) => {
                        eprintln!("WARNING: Could not read path from glob pattern: {}", e);
                        failure_count += 1;
                        continue; // Skip to the next entry
                    }
                };
                
                if path.is_file() {
                    println!("Processing file: {}", path.display());
                    let content = match std::fs::read(&path) {
                        Ok(content) => content,
                        Err(e) => {
                            eprintln!("  -> ERROR: Failed to read file content: {}", e);
                            failure_count += 1;
                            continue;
                        }
                    };

                    // Try to ingest the file, but handle the result gracefully.
                    match ingestion::ingest_file(&pool, &content).await {
                        Ok(_) => {
                            success_count += 1;
                        }
                        Err(e) => {
                            // This is where we catch `.DS_Store` and other invalid files!
                            eprintln!("  -> WARNING: Skipping file. Reason: {}", e);
                            failure_count += 1;
                        }
                    }
                }
            }

            println!("\n--- Ingestion Summary ---");
            println!("  ✅ Successfully processed: {}", success_count);
            println!("  ⚠️  Skipped / Failed: {}", failure_count);
        }
    }

    println!("\n✅ CLI command finished successfully.");
    Ok(())
}