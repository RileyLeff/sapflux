// crates/sapflux-core/src/metadata/sensors.rs

use crate::error::{PipelineError, Result};
use crate::types::Sensor;
use sqlx::PgPool;

pub async fn get_sensor_by_id_string(pool: &PgPool, id_string: &str) -> Result<Sensor> {
    sqlx::query_as!(
        Sensor,
        // The `!` tells sqlx to treat the result as NOT NULL, matching our struct.
        r#"
        SELECT
            id,
            sensor_id,
            downstream_probe_distance_cm::double precision AS "downstream_probe_distance_cm!",
            upstream_probe_distance_cm::double precision AS "upstream_probe_distance_cm!",
            thermistor_depth_1_mm,
            thermistor_depth_2_mm
         FROM sensors WHERE sensor_id = $1
        "#,
        id_string
    )
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| PipelineError::Validation(format!("Sensor with id '{}' not found.", id_string)))
}