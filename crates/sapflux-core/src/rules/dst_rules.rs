// crates/sapflux-core/src/dst_rules.rs
use crate::error::{PipelineError, Result};
use crate::types::DstTransition;
use chrono::NaiveDateTime;
use sqlx::PgPool;

pub async fn create_dst_rule(
    pool: &PgPool,
    action: &str,
    ts_local: NaiveDateTime,
) -> Result<DstTransition> {
    if action != "start" && action != "end" {
        return Err(PipelineError::Processing("Validation failed: action must be 'start' or 'end'".to_string()));
    }

    let new_rule = sqlx::query_as(
        "INSERT INTO dst_transitions (transition_action, ts_local) VALUES ($1, $2) RETURNING *",
    )
    .bind(action)
    .bind(ts_local)
    .fetch_one(pool)
    .await?;

    Ok(new_rule)
}