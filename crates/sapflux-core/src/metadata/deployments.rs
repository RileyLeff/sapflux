// crates/sapflux-core/src/metadata/deployments.rs

use crate::error::{PipelineError, Result};
use crate::types::{NewDeployment};
use sqlx::PgPool;
use uuid::Uuid;
use serde_json;

/// Creates a new deployment and automatically "closes out" the previous active
/// deployment for the same sensor, all within a single database transaction.
pub async fn create_deployment(pool: &PgPool, data: &NewDeployment) -> Result<Uuid> {
    let mut tx = pool.begin().await?;

    let previous_active_deployment_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM deployments
         WHERE datalogger_id = $1 AND sdi_address = $2 AND end_time_utc IS NULL"
    )
    .bind(data.datalogger_id)
    .bind(data.sdi_address.as_str())
    .fetch_optional(&mut *tx)
    .await?;

    if let Some(id) = previous_active_deployment_id {
        let updated_rows = sqlx::query(
            "UPDATE deployments SET end_time_utc = $1 WHERE id = $2"
        )
        .bind(data.start_time_utc)
        .bind(id)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        if updated_rows != 1 {
            return Err(PipelineError::Validation(
                "Failed to update previous deployment. Transaction rolled back.".to_string()
            ));
        }
        println!("  -> Closed out previous deployment (ID: {}).", id);
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
    .fetch_one(&mut *tx)
    .await?;

    tx.commit().await?;

    println!("  -> Successfully created new deployment with ID: {}", new_id);

    Ok(new_id)
}