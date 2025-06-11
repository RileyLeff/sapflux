// crates/sapflux-cli/src/commands/seed/dst_transitions.rs

use crate::commands::seed::types::DstTransitionsFile;
use anyhow::{Context, Result};
use sqlx::{Postgres, Transaction};
use std::path::Path;

pub async fn seed(tx: &mut Transaction<'_, Postgres>, path: &Path) -> Result<()> {
    println!("   -> Seeding DST transitions from '{}'...", path.display());
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read DST transitions file at '{}'", path.display()))?;
    let data: DstTransitionsFile = toml::from_str(&content)
        .with_context(|| format!("Failed to parse DST transitions TOML from '{}'", path.display()))?;

    sqlx::query("TRUNCATE TABLE dst_transitions RESTART IDENTITY CASCADE")
        .execute(&mut **tx)
        .await?;
    
    let mut count = 0;
    for transition in data.transitions {
        sqlx::query(
            "INSERT INTO dst_transitions (transition_action, ts_local) VALUES ($1, $2)",
        )
        .bind(transition.action)
        .bind(transition.ts_local)
        .execute(&mut **tx)
        .await?;
        count += 1;
    }
    
    println!("      -> Seeded {} DST transitions.", count);
    Ok(())
}