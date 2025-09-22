use std::env;

use anyhow::Result;
use sapflux_core::{db, seed};

#[tokio::test]
async fn db_seed_is_idempotent_when_database_available() -> Result<()> {
    let database_url = match env::var("SAPFLUX_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("Skipping db_seed test because SAPFLUX_TEST_DATABASE_URL is not set");
            return Ok(());
        }
    };

    let pool = db::connect(&database_url).await?;
    db::run_migrations(&pool).await?;

    seed::run(&pool).await?;
    seed::run(&pool).await?; // second run should be a no-op

    let format_count: i64 = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM data_formats WHERE code_identifier = $1",
    )
    .bind("sapflow_toa5_hierarchical_v1")
    .fetch_one(&pool)
    .await?;

    assert_eq!(format_count, 1, "expected exactly one canonical data format record");

    Ok(())
}
