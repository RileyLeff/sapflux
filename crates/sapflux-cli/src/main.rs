// crates/sapflux-cli/src/main.rs

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

// Bring our command modules into scope
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
    /// Seeds the database with initial metadata from TOML files.
    /// This is a destructive operation and will truncate existing metadata tables.
    Seed {
        #[arg(long, default_value = "initial_metadata/projects.toml")]
        projects_file: PathBuf,

        #[arg(long, default_value = "initial_metadata/sensors.toml")]
        sensors_file: PathBuf,

        #[arg(long, default_value = "initial_metadata/parameters.toml")]
        parameters_file: PathBuf,
        
        #[arg(long, default_value = "initial_metadata/dst_transitions.toml")]
        dst_file: PathBuf,

        #[arg(long, default_value = "initial_metadata/deployments.toml")]
        deployments_file: PathBuf,
    },
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
            handle_deployment_command(command, &pool).await?;
        }
        Commands::Seed { 
            projects_file, 
            sensors_file, 
            parameters_file, 
            dst_file, 
            deployments_file 
        } => {
            // Delegate all seeding logic to the new handler
            handle_seed_command(
                &pool, 
                &projects_file, 
                &sensors_file,
                &parameters_file,
                &dst_file,
                &deployments_file
            ).await?;
        }
    }

    println!("\n✅ CLI command finished successfully.");
    Ok(())
}