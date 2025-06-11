// crates/sapflux-core/src/metadata/parameters.rs

use crate::error::{PipelineError, Result};
use crate::types::Parameter;
use sqlx::PgPool;

pub async fn get_parameter_by_name(pool: &PgPool, name: &str) -> Result<Parameter> {
    sqlx::query_as!(
        Parameter,
        // We apply the same `!` fix here for the NUMERIC `value` column.
        r#"
        SELECT
            id,
            name,
            value::double precision AS "value!",
            unit,
            description
        FROM parameters WHERE name = $1
        "#,
        name
    )
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| PipelineError::Validation(format!("Parameter with name '{}' not found.", name)))
}