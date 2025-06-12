// crates/sapflux-cli/src/main.rs

use anyhow::Result;
use clap::Parser;
use polars::prelude::{ParquetWriter, SerWriter};
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

        /// Suppress success messages and only show errors
        #[arg(short, long)]
        quiet: bool,
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

        #[arg(long, default_value = "initial_metadata/fixes.toml")]
        fixes_file: PathBuf,
    },
    /// Fully processes the raw data into final, analysis-ready Parquet files.
    Process {
        /// The base name for the output parquet file(s). Project names will be appended.
        #[arg(short, long, default_value = "output.parquet")]
        output: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().expect(".env file not found");
    let cli = Cli::parse();
    let pool = sapflux_core::db::connect().await?;

    match cli.command {
        Commands::Ingest { dir, quiet } => {
            if !quiet {
                println!("Starting ingestion from directory: {}", dir.display());
            }
            let pattern = dir.join("**/*");
            let pattern_str = pattern.to_str().expect("Invalid path pattern");

            let forbidden_words = ["public", "status", "datatableinfo", "ds_store"];
            if !quiet {
                println!("   -> Applying filename filters, ignoring files containing: {:?}", forbidden_words);
            }

            let mut success_count = 0;
            let mut failure_count = 0;
            let mut skipped_count = 0;

            for entry in glob::glob(pattern_str)? {
                let path = match entry {
                    Ok(path) => path,
                    Err(e) => {
                        eprintln!("WARNING: Could not read path from glob pattern: {}", e);
                        continue;
                    }
                };

                if path.is_file() {
                    let path_str_lower = path.to_string_lossy().to_lowercase();
                    if forbidden_words.iter().any(|word| path_str_lower.contains(word)) {
                        skipped_count += 1;
                        continue;
                    }

                    if !quiet {
                        println!("Processing file: {}", path.display());
                    }
                    let content = match std::fs::read(&path) {
                        Ok(content) => content,
                        Err(e) => {
                            eprintln!("  -> ERROR: Failed to read file '{}': {}", path.display(), e);
                            failure_count += 1;
                            continue;
                        }
                    };

                    match sapflux_core::ingestion::ingest_file(&pool, &content, quiet).await {
                        Ok(_) => success_count += 1,
                        Err(e) => {
                            eprintln!("  -> WARNING: Skipping file '{}'. Reason: {}", path.display(), e);
                            failure_count += 1;
                        }
                    }
                }
            }
            println!("\n--- Ingestion Summary ---");
            println!("  ✅ Successfully processed / found existing: {}", success_count);
            println!("  ⚠️  Skipped / Failed: {}", failure_count);
            println!("  🔎 Filtered by name: {}", skipped_count);
        }
        Commands::Deployment { command } => {
            handle_deployment_command(command, &pool).await?;
        }
        Commands::Seed {
            projects_file,
            sensors_file,
            parameters_file,
            dst_file,
            deployments_file,
            fixes_file,
        } => {
            handle_seed_command(
                &pool,
                &projects_file,
                &sensors_file,
                &parameters_file,
                &dst_file,
                &deployments_file,
                &fixes_file,
            ).await?;
        }
        Commands::Process { output } => {
            println!("🚀 Starting processing pipeline orchestration...");

            // Step 1: Call the parsing and unification function
            let unified_lf = sapflux_core::processing::get_parsed_and_unified_lazyframe(&pool).await?;

            // Step 2: Pass the result to the DST and mapping function
            let final_lf = sapflux_core::processing::apply_dst_correction_and_map_deployments(unified_lf, &pool).await?;

            // Step 3: Collect the final result into an in-memory DataFrame
            let mut final_df = final_lf.collect()?;

            println!("\n✅ Full processing and mapping complete.");
            println!("   -> Final dataset shape: {:?}", final_df.shape());

            // Step 4: Split the DataFrame by project and save each to a separate Parquet file
            let projects = final_df.column("project_name")?.unique()?.utf8()?.into_no_null_iter().collect::<Vec<_>>();
            println!("   -> Found projects: {:?}", projects);

            for project_name in projects {
                println!("      -> Filtering for project: '{}'", project_name);
                let mut project_df = final_df.filter(&final_df.column("project_name")?.equal(project_name)?)?;

                let file_name = format!(
                    "{}_{}.parquet",
                    output.file_stem().unwrap_or_default().to_str().unwrap_or("output"),
                    project_name.replace(' ', "_") // Sanitize project name for filename
                );
                let output_path = output.with_file_name(file_name);

                println!("      -> Writing {} rows to '{}'", project_df.height(), output_path.display());
                let mut file = std::fs::File::create(&output_path)?;
                ParquetWriter::new(&mut file).finish(&mut project_df)?;
            }

            println!("\n✅ All project files saved successfully.");
        }
    }

    println!("\n✅ CLI command finished successfully.");
    Ok(())
}