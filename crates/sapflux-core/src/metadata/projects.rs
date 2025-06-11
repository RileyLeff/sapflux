// crates/sapflux-core/src/metadata/projects.rs

use crate::error::{PipelineError, Result};
use crate::types::Project;
use sqlx::PgPool;

pub async fn get_project_by_name(pool: &PgPool, name: &str) -> Result<Project> {
    sqlx::query_as!(
        Project,
        "SELECT id, name, description FROM projects WHERE name = $1",
        name
    )
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| PipelineError::Validation(format!("Project with name '{}' not found.", name)))
}