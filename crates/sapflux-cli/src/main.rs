// crates/sapflux-cli/src/main.rs

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

// Bring our new command module into scope
mod commands;
use commands::deployment::{handle_deployment_command, DeploymentCommands};
use commands::seed::handle_seed_command;

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
        #[arg(short, long)]
        dir: PathBuf,
    },
    /// Manage deployment metadata.
    Deployment {
        #[command(subcommand)]
        command: DeploymentCommands,
    },
    Seed,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().expect(".env file not found");
    let cli = Cli::parse();
    let pool = sapflux_core::db::connect().await?;

    match cli.command {
        Commands::Ingest { dir } => {
            // This could also be moved to a handler in `commands/ingest.rs` later
            println!("Starting ingestion from directory: {}", dir.display());
            let pattern = dir.join("**/*");
            let pattern_str = pattern.to_str().expect("Invalid path pattern");

            let mut success_count = 0;
            let mut failure_count = 0;

            for entry in glob::glob(pattern_str)? {
                let path = match entry {
                    Ok(path) => path,
                    Err(e) => {
                        eprintln!("WARNING: Could not read path from glob pattern: {}", e);
                        failure_count += 1;
                        continue;
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

                    match sapflux_core::ingestion::ingest_file(&pool, &content).await {
                        Ok(_) => success_count += 1,
                        Err(e) => {
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
        Commands::Deployment { command } => {
            // Delegate all deployment logic to the handler function
            handle_deployment_command(command, &pool).await?;
        }
        Commands::Seed => {
            handle_seed_command(&pool).await?;
        }
    }

    println!("\n✅ CLI command finished successfully.");
    Ok(())
}