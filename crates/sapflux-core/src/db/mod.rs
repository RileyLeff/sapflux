use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

pub type DbPool = Pool<Postgres>;

/// Establish a new Postgres connection pool using sensible defaults for the
/// pipeline services.
pub async fn connect(database_url: &str) -> Result<DbPool> {
    PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(10))
        .connect(database_url)
        .await
        .with_context(|| "failed to connect to Postgres")
}

/// Run database migrations embedded at compile-time.
pub async fn run_migrations(pool: &DbPool) -> Result<()> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .with_context(|| "failed to run database migrations")
}
