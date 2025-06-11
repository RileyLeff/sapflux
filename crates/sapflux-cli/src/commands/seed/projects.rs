// crates/sapflux-cli/src/commands/seed/projects.rs

use crate::commands::seed::types::ProjectsFile;
use anyhow::{Context, Result};
use sqlx::{Postgres, Transaction};
use std::collections::HashMap;
use std::path::Path;

pub async fn seed(
    tx: &mut Transaction<'_, Postgres>,
    path: &Path,
) -> Result<HashMap<String, i32>> {
    println!("   -> Seeding projects from '{}'...", path.display());
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read projects file at '{}'", path.display()))?;
    let data: ProjectsFile = toml::from_str(&content)
        .with_context(|| format!("Failed to parse projects TOML from '{}'", path.display()))?;
    
    let mut project_map = HashMap::new();

    sqlx::query("TRUNCATE TABLE projects RESTART IDENTITY CASCADE")
        .execute(&mut **tx)
        .await?;

    for project in data.projects {
        let id: i32 = sqlx::query_scalar(
            "INSERT INTO projects (name, description) VALUES ($1, $2) RETURNING id",
        )
        .bind(&project.name)
        .bind(project.description)
        .fetch_one(&mut **tx)
        .await?;
        project_map.insert(project.name, id);
    }

    println!("      -> Seeded {} projects.", project_map.len());
    Ok(project_map)
}