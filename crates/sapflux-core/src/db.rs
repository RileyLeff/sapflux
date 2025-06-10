// crates/sapflux-core/src/db.rs

use crate::error::Result;
use sqlx::PgPool;

/// Establishes a connection pool to the PostgreSQL database.
///
/// Reads the `DATABASE_URL` environment variable to configure the connection.
///
/// # Panics
/// Panics if the `DATABASE_URL` is not set.
pub async fn connect() -> Result<PgPool> {
    // Read the database connection URL from the environment.
    let db_url = std::env::var("DATABASE_URL")?;

    // Create a connection pool.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5) // Set a reasonable maximum number of connections.
        .connect(&db_url)
        .await?; // The `?` operator here will convert any sqlx::Error into our PipelineError.

    println!("✅ Database connection pool established.");
    Ok(pool)
}