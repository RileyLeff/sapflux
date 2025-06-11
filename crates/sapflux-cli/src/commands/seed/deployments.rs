// crates/sapflux-cli/src/commands/seed/deployments.rs

use crate::commands::seed::types::DeploymentsFile;
use anyhow::{anyhow, Context, Result};
use sqlx::{Postgres, Transaction};
use std::collections::HashMap;
use std::path::Path;
// Import the necessary types and functions from the core library
use sapflux_core::{
    metadata,
    types::{NewDeployment, DeploymentAttributes, SdiAddress},
};

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
    
    // We still truncate to ensure a completely clean state before seeding.
    sqlx::query("TRUNCATE TABLE deployments RESTART IDENTITY CASCADE")
        .execute(&mut **tx)
        .await?;

    // The deployments MUST be inserted in chronological order for the "close out" logic to work correctly.
    let mut deployments_to_seed = data.deployments;
    deployments_to_seed.sort_by_key(|d| d.start_time_utc);

    println!("      -> Inserting {} deployments chronologically...", deployments_to_seed.len());
    for (i, deployment) in deployments_to_seed.iter().enumerate() {
        let project_id = *project_map.get(&deployment.project_name).ok_or_else(|| {
            anyhow!(
                "In deployment #{}, could not find project '{}' in the project map.",
                i + 1,
                deployment.project_name
            )
        })?;

        let sensor_id = *sensor_map.get(&deployment.sensor_id).ok_or_else(|| {
            anyhow!(
                "In deployment #{}, could not find sensor '{}' in the sensor map.",
                i + 1,
                deployment.sensor_id
            )
        })?;
        
        // The attributes in the TOML file can be directly deserialized into our core DeploymentAttributes enum
        let attributes: DeploymentAttributes = deployment.attributes.clone().try_into()
            .with_context(|| format!("Failed to parse attributes for deployment #{}", i + 1))?;

        // Create the core data type that our function expects
        let new_deployment_data = NewDeployment {
            start_time_utc: deployment.start_time_utc,
            datalogger_id: deployment.datalogger_id,
            sdi_address: SdiAddress::new(&deployment.sdi_address)?,
            tree_id: deployment.tree_id.clone(),
            project_id,
            sensor_id,
            attributes,
        };

        // NOW we call the core library function, which contains the correct UPDATE/INSERT logic.
        // We pass our existing transaction to it.
        metadata::deployments::create_deployment_in_transaction(tx, &new_deployment_data)
            .await
            .with_context(|| format!("Failed to insert deployment #{} (Logger: {}, SDI: {})", i + 1, new_deployment_data.datalogger_id, new_deployment_data.sdi_address.as_str()))?;
    }

    println!("      -> Seeded {} deployments.", deployments_to_seed.len());
    Ok(())
}