use std::env;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use sapflux_core::{db, seed};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(author, version, about = "Sapflux administrative tooling", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Seed reference data into the database
    DbSeed(DbSeedArgs),
}

#[derive(Args, Debug, Default)]
struct DbSeedArgs {
    /// Skip running embedded database migrations before seeding
    #[arg(long)]
    skip_migrations: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::DbSeed(args) => handle_db_seed(args).await,
    }
}

async fn handle_db_seed(args: DbSeedArgs) -> Result<()> {
    dotenvy::dotenv().ok();

    let database_url = env::var("DATABASE_URL")
        .or_else(|_| env::var("SAPFLUX_DATABASE_URL"))
        .context("DATABASE_URL (or SAPFLUX_DATABASE_URL) must be set")?;

    let pool = db::connect(&database_url).await?;

    if args.skip_migrations {
        info!("Skipping migrations at user request");
    } else {
        db::run_migrations(&pool).await?;
    }

    seed::run(&pool).await?;

    Ok(())
}
