// crates/sapflux-cli/src/commands/seed/mod.rs

// Declare the submodules within the 'seed' module
mod types;
mod projects;
mod sensors;
mod parameters;
mod dst_transitions;
mod deployments;

use sqlx::PgPool;
use std::path::Path;
use anyhow::Result;

/// The main handler for the `seed` command.
/// Orchestrates the entire seeding process within a single transaction.
pub async fn handle_seed_command(
    pool: &PgPool,
    projects_path: &Path,
    sensors_path: &Path,
    parameters_path: &Path,
    dst_path: &Path,
    deployments_path: &Path,
) -> Result<()> {
    println!("🌱 Starting database seeding...");
    let mut tx = pool.begin().await.map_err(|e| anyhow::anyhow!("Failed to begin transaction: {}", e))?;
    println!("   -> Transaction started.");

    // The order of execution is critical to respect foreign key constraints.
    // Each function takes the transaction and the path to its data file.
    // The functions for projects and sensors return lookup maps for use in deployments.
    match (async {
        dst_transitions::seed(&mut tx, dst_path).await?;
        let project_map = projects::seed(&mut tx, projects_path).await?;
        let sensor_map = sensors::seed(&mut tx, sensors_path).await?;
        parameters::seed(&mut tx, parameters_path).await?;
        deployments::seed(&mut tx, deployments_path, &project_map, &sensor_map).await?;
        
        // This weird Ok(()) is needed to satisfy the type checker for the closure.
        Ok::<(), anyhow::Error>(()) 
    }).await {
        Ok(_) => {
            // If all seeders succeeded, commit the transaction.
            tx.commit().await.map_err(|e| anyhow::anyhow!("Failed to commit transaction: {}", e))?;
            println!("\n✅ Database seeding completed successfully.");
        },
        Err(e) => {
            // If any seeder failed, roll back the transaction.
            eprintln!("\n❌ Error during seeding: {}. Rolling back all changes.", e);
            tx.rollback().await.map_err(|e| anyhow::anyhow!("Failed to rollback transaction: {}", e))?;
            // Propagate the original error to exit the CLI with a non-zero status.
            return Err(e);
        }
    }

    Ok(())
}