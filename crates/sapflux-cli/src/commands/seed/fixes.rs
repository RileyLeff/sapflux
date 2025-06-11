// crates/sapflux-cli/src/commands/seed/fixes.rs
use serde::Deserialize;
use anyhow::{Context, Result};
use sqlx::{Postgres, Transaction};
use std::path::Path;

#[derive(Debug, Deserialize)]
struct FixesFile {
    fix: Vec<FixEntry>,
}

#[derive(Debug, Deserialize)]
struct FixEntry {
    hash: String,
    action: String,
    value: toml::Value,
    description: Option<String>,
}

pub async fn seed(tx: &mut Transaction<'_, Postgres>, path: &Path) -> Result<()> {
    if !path.exists() {
        println!("   -> Skipping optional fixes seeding: '{}' not found.", path.display());
        return Ok(());
    }

    println!("   -> Seeding manual fixes from '{}'...", path.display());
    let content = std::fs::read_to_string(path)?;
    let data: FixesFile = toml::from_str(&content)
        .with_context(|| format!("Failed to parse fixes TOML from '{}'", path.display()))?;

    sqlx::query("TRUNCATE TABLE manual_fixes RESTART IDENTITY CASCADE").execute(&mut **tx).await?;

    for fix in data.fix {
        let json_value = serde_json::to_value(&fix.value)?;
        sqlx::query(
            "INSERT INTO manual_fixes (file_hash, action, value, description) VALUES ($1, $2, $3, $4)",
        )
        .bind(fix.hash)
        .bind(fix.action)
        .bind(json_value)
        .bind(fix.description)
        .execute(&mut **tx)
        .await?;
    }

    println!("      -> Seeded {} manual fixes.", sqlx::query_scalar("SELECT COUNT(*) FROM manual_fixes").fetch_one(&mut **tx).await.unwrap_or(0i64));
    Ok(())
}