use std::env;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use sapflux_core::{db, object_gc, object_store::ObjectStore, seed};
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
    /// Plan (and optionally apply) object-store garbage collection
    ObjectStoreGc(ObjectStoreGcArgs),
}

#[derive(Args, Debug, Default)]
struct DbSeedArgs {
    /// Skip running embedded database migrations before seeding
    #[arg(long)]
    skip_migrations: bool,
}

#[derive(Args, Debug, Default)]
struct ObjectStoreGcArgs {
    /// Apply deletions instead of running in dry-run mode
    #[arg(long)]
    apply: bool,
    /// Skip running migrations before inspecting the database
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
        Command::ObjectStoreGc(args) => handle_object_store_gc(args).await,
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

async fn handle_object_store_gc(args: ObjectStoreGcArgs) -> Result<()> {
    dotenvy::dotenv().ok();

    let database_url = env::var("DATABASE_URL")
        .or_else(|_| env::var("SAPFLUX_DATABASE_URL"))
        .context("DATABASE_URL (or SAPFLUX_DATABASE_URL) must be set")?;

    let pool = db::connect(&database_url).await?;

    if !args.skip_migrations {
        db::run_migrations(&pool).await?;
    }

    let store = ObjectStore::from_env_async()
        .await
        .context("failed to configure object store")?;
    let report = object_gc::plan_gc(&pool, &store).await?;

    if report.total_orphaned() == 0 {
        println!("No orphaned objects found. Object store is in sync with the database.");
    } else {
        println!("Found {} orphaned objects:", report.total_orphaned());
        for entry in &report.entries {
            if entry.orphaned.is_empty() {
                continue;
            }
            println!("  Prefix '{}': {} keys", entry.prefix, entry.orphaned.len());
            for key in &entry.orphaned {
                println!("    {}", key);
            }
        }
    }

    if args.apply {
        object_gc::apply_gc(&store, &report).await?;
        println!("Applied object-store garbage collection successfully.");
    } else if report.total_orphaned() > 0 {
        println!("Run again with --apply to delete the orphaned objects.");
    }

    Ok(())
}
