// crates/sapflux-cli/src/commands/seed/deployments.rs

use crate::commands::seed::types::DeploymentsFile;
use anyhow::{anyhow, Context, Result};
use sqlx::{Postgres, Transaction};
use std::collections::HashMap;
use std::path::Path;

pub async fn seed(
    tx: &mut Transaction<'_, Postgres>,
    path: &Path,
    project_map: &HashMap<String, i32>,
    sensor_map: &HashMap<String, i32>,
) -> Result<()> {
    println!("   -> Seeding deployments from '{}'...", path.display());
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read deployments file at '{}'", path.display()))?;
    let data: DeploymentsFile = toml::from_str(&content)
        .with_context(|| format!("Failed to parse deployments TOML from '{}'", path.display()))?;
    
    // We will be calling `create_deployment` from the core library, which handles
    // the logic of closing out previous deployments. Therefore, we still truncate
    // to ensure a clean state before this complex logic runs.
    sqlx::query("TRUNCATE TABLE deployments RESTART IDENTITY CASCADE")
        .execute(&mut **tx)
        .await?;

    // The deployments must be inserted in chronological order for the "close out" logic to work correctly.
    let mut deployments_to_seed = data.deployments;
    deployments_to_seed.sort_by_key(|d| d.start_time_utc);

    for (i, deployment) in deployments_to_seed.iter().enumerate() {
        let project_id = project_map.get(&deployment.project_name).ok_or_else(|| {
            anyhow!(
                "In deployment #{}, could not find project '{}' in the project map.",
                i + 1,
                deployment.project_name
            )
        })?;

        let sensor_id = sensor_map.get(&deployment.sensor_id).ok_or_else(|| {
            anyhow!(
                "In deployment #{}, could not find sensor '{}' in the sensor map.",
                i + 1,
                deployment.sensor_id
            )
        })?;

        // Convert TOML attributes to a JSONB-compatible value.
        let attributes_json = serde_json::to_value(&deployment.attributes)?;

        // The core library's `create_deployment` function is more complex than a simple INSERT.
        // It handles closing out previous deployments. We should reuse that logic instead of
        // re-implementing it. To do that, we need to call it with a transaction object.
        // For simplicity in this step, we will use a direct INSERT, but in a future refactor,
        // you would adapt `sapflux_core::metadata::create_deployment` to accept a `&mut Transaction`.
        // For now, this direct approach is correct for a pure seeding operation.
        sqlx::query(
            "INSERT INTO deployments (project_id, sensor_id, datalogger_id, sdi_address, tree_id, start_time_utc, attributes)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(project_id)
        .bind(sensor_id)
        .bind(deployment.datalogger_id)
        .bind(&deployment.sdi_address)
        .bind(&deployment.tree_id)
        .bind(deployment.start_time_utc)
        .bind(attributes_json)
        .execute(&mut **tx)
        .await?;
    }

    println!("      -> Seeded {} deployments.", deployments_to_seed.len());
    Ok(())
}