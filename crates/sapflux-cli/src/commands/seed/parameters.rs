// crates/sapflux-cli/src/commands/seed/parameters.rs

use crate::commands::seed::types::ParametersFile;
use anyhow::{Context, Result};
use sqlx::{Postgres, Transaction};
use std::path::Path;

pub async fn seed(tx: &mut Transaction<'_, Postgres>, path: &Path) -> Result<()> {
    println!("   -> Seeding parameters from '{}'...", path.display());
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read parameters file at '{}'", path.display()))?;
    let data: ParametersFile = toml::from_str(&content)
        .with_context(|| format!("Failed to parse parameters TOML from '{}'", path.display()))?;
    
    sqlx::query("TRUNCATE TABLE parameters RESTART IDENTITY CASCADE")
        .execute(&mut **tx)
        .await?;

    let mut count = 0;
    for (name, params) in data.parameters {
        sqlx::query(
            "INSERT INTO parameters (name, value, unit, description) VALUES ($1, $2, $3, $4)",
        )
        .bind(name)
        .bind(params.value)
        .bind(params.unit)
        .bind(params.description)
        .execute(&mut **tx)
        .await?;
        count += 1;
    }

    println!("      -> Seeded {} parameters.", count);
    Ok(())
}