// crates/sapflux-core/src/metadata/deployments.rs

use crate::error::{PipelineError, Result};
use crate::types::{NewDeployment, DeploymentDetails}; 
use sqlx::{PgPool, Transaction, Postgres}; // Import Transaction and Postgres
use uuid::Uuid;
use serde_json;

/// Creates a new deployment within a self-contained transaction.
/// This is a convenience wrapper around `create_deployment_in_transaction`.
pub async fn create_deployment(pool: &PgPool, data: &NewDeployment) -> Result<Uuid> {
    let mut tx = pool.begin().await?;
    let new_id = create_deployment_in_transaction(&mut tx, data).await?;
    tx.commit().await?;
    Ok(new_id)
}

/// Creates a new deployment within an existing transaction.
/// This function contains the core logic: it finds and "closes out" the previous
/// active deployment for the same sensor before inserting the new one.
pub async fn create_deployment_in_transaction(
    tx: &mut Transaction<'_, Postgres>, 
    data: &NewDeployment
) -> Result<Uuid> {
    let previous_active_deployment_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM deployments
         WHERE datalogger_id = $1 AND sdi_address = $2 AND end_time_utc IS NULL"
    )
    .bind(data.datalogger_id)
    .bind(data.sdi_address.as_str())
    .fetch_optional(&mut **tx) // Use the transaction here
    .await?;

    if let Some(id) = previous_active_deployment_id {
        // Important: Check that we are not trying to create a deployment before the one we are closing.
        // This can happen if the seed file is not perfectly ordered.
        let previous_start_time: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("SELECT start_time_utc FROM deployments WHERE id = $1")
            .bind(id)
            .fetch_one(&mut **tx)
            .await?;
        
        if data.start_time_utc <= previous_start_time {
            return Err(PipelineError::Validation(format!(
                "Validation failed for logger {}-{}: New deployment start time ({}) must be after the previous deployment's start time ({}).",
                data.datalogger_id, data.sdi_address.as_str(), data.start_time_utc, previous_start_time
            )));
        }

        let updated_rows = sqlx::query(
            "UPDATE deployments SET end_time_utc = $1 WHERE id = $2"
        )
        .bind(data.start_time_utc)
        .bind(id)
        .execute(&mut **tx) // Use the transaction here
        .await?
        .rows_affected();

        if updated_rows != 1 {
            return Err(PipelineError::Validation(
                "Failed to update previous deployment. Transaction rolled back.".to_string()
            ));
        }
    }
    
    let new_id: Uuid = sqlx::query_scalar(
        "INSERT INTO deployments (
            start_time_utc,
            datalogger_id,
            sdi_address,
            tree_id,
            project_id,
            sensor_id,
            attributes
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id"
    )
    .bind(data.start_time_utc)
    .bind(data.datalogger_id)
    .bind(data.sdi_address.as_str())
    .bind(&data.tree_id)
    .bind(data.project_id)
    .bind(data.sensor_id)
    .bind(serde_json::to_value(&data.attributes)?) 
    .fetch_one(&mut **tx) // Use the transaction here
    .await?;

    Ok(new_id)
}


/// Fetches a detailed list of all deployments, joined with project and sensor info.
pub async fn get_all_deployments(pool: &PgPool) -> Result<Vec<DeploymentDetails>> {
    let deployments = sqlx::query_as!(
        DeploymentDetails,
        r#"
        SELECT
            d.id,
            p.name as "project_name!",
            d.datalogger_id,
            d.sdi_address,
            d.tree_id,
            s.sensor_id,
            d.start_time_utc,
            d.end_time_utc
        FROM
            deployments d
        JOIN
            projects p ON d.project_id = p.id
        JOIN
            sensors s ON d.sensor_id = s.id
        ORDER BY
            d.datalogger_id, d.sdi_address, d.start_time_utc DESC
        "#
    )
    .fetch_all(pool)
    .await?;

    Ok(deployments)
}