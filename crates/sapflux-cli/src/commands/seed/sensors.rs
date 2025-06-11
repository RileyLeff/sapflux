// crates/sapflux-cli/src/commands/seed/sensors.rs

use crate::commands::seed::types::SensorsFile;
use anyhow::{Context, Result};
use sqlx::{Postgres, Transaction};
use std::collections::HashMap;
use std::path::Path;

pub async fn seed(
    tx: &mut Transaction<'_, Postgres>,
    path: &Path,
) -> Result<HashMap<String, i32>> {
    println!("   -> Seeding sensors from '{}'...", path.display());
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read sensors file at '{}'", path.display()))?;
    let data: SensorsFile = toml::from_str(&content)
        .with_context(|| format!("Failed to parse sensors TOML from '{}'", path.display()))?;

    let mut sensor_map = HashMap::new();

    sqlx::query("TRUNCATE TABLE sensors RESTART IDENTITY CASCADE")
        .execute(&mut **tx)
        .await?;

    for sensor in data.sensors {
        let id: i32 = sqlx::query_scalar(
            "INSERT INTO sensors (sensor_id, downstream_probe_distance_cm, upstream_probe_distance_cm, thermistor_depth_1_mm, thermistor_depth_2_mm)
             VALUES ($1, $2, $3, $4, $5) RETURNING id",
        )
        .bind(&sensor.id)
        .bind(sensor.downstream_probe_distance_cm)
        .bind(sensor.upstream_probe_distance_cm)
        .bind(sensor.thermistor_depth_1_mm)
        .bind(sensor.thermistor_depth_2_mm)
        .fetch_one(&mut **tx)
        .await?;
        sensor_map.insert(sensor.id, id);
    }

    println!("      -> Seeded {} sensors.", sensor_map.len());
    Ok(sensor_map)
}