use anyhow::Result;
use sapflux_repository::PostgresRepository;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let database_url = env::var("DATABASE_URL")?;
    let repo = PostgresRepository::connect(&database_url, 5).await?;
    repo.run_migrations().await?;
    Ok(())
}
