use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use sapflux_core::{db, seed};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(author, version, about = "Sapflux client CLI and API server", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start the Sapflux API server (stub)
    Serve,
    /// Run database migrations
    Migrate,
    /// Seed reference data (optionally running migrations)
    DbSeed(DbSeedArgs),
}

#[derive(Args, Debug, Default)]
struct DbSeedArgs {
    /// Skip running migrations before seeding
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
        Command::Serve => {
            info!("Starting Sapflux API server (stub)");
            Ok(())
        }
        Command::Migrate => {
            let pool = connect_pool().await?;
            db::run_migrations(&pool).await?;
            info!("Database migrations applied");
            Ok(())
        }
        Command::DbSeed(args) => {
            let pool = connect_pool().await?;
            if args.skip_migrations {
                warn!("Skipping migrations before seeding");
            } else {
                db::run_migrations(&pool).await?;
            }
            seed::run(&pool).await?;
            info!("Reference data seeded");
            Ok(())
        }
    }
}

async fn connect_pool() -> Result<db::DbPool> {
    dotenvy::dotenv().ok();
    let database_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("SAPFLUX_DATABASE_URL"))
        .context("DATABASE_URL (or SAPFLUX_DATABASE_URL) must be set")?;
    db::connect(&database_url).await
}
