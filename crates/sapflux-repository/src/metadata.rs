use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Row};
use uuid::Uuid;
use chrono::Utc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("record not found")]
    NotFound,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Site {
    pub site_id: Uuid,
    pub code: String,
    pub name: Option<String>,
    pub timezone: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Species {
    pub species_id: Uuid,
    pub code: String,
    pub common_name: Option<String>,
    pub latin_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SensorType {
    pub sensor_type_id: Uuid,
    pub code: String,
    pub description: Option<String>,
    pub downstream_probe_distance_cm: Option<f64>,
    pub upstream_probe_distance_cm: Option<f64>,
    pub thermistor_pairs: Vec<ThermistorPair>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ThermistorPair {
    pub thermistor_pair_id: Uuid,
    pub name: String,
    pub depth_mm: f64,
    pub position_label: Option<String>,
    pub aliases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Deployment {
    pub deployment_id: Uuid,
    pub logger_id: String,
    pub sdi_address: String,
    pub sensor_type_id: Uuid,
    pub site_id: Uuid,
    pub tree_id: String,
    pub species_id: Uuid,
    pub zone: Option<String>,
    pub plot: Option<String>,
    pub start_timestamp_utc: chrono::DateTime<Utc>,
    pub end_timestamp_utc: Option<chrono::DateTime<Utc>>,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ParserConfigRecord {
    pub config_id: Uuid,
    pub version: String,
    pub settings: Value,
    pub updated_at: chrono::DateTime<Utc>,
}

#[derive(Clone)]
pub struct MetadataRepository {
    pool: PgPool,
}

impl MetadataRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn upsert_site(&self, site: &Site) -> Result<(), MetadataError> {
        sqlx::query(
            r#"
            INSERT INTO sites (site_id, code, name, timezone)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (site_id) DO UPDATE SET
                code = EXCLUDED.code,
                name = EXCLUDED.name,
                timezone = EXCLUDED.timezone
            "#,
        )
        .bind(site.site_id)
        .bind(&site.code)
        .bind(&site.name)
        .bind(&site.timezone)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fetch_site_by_code(&self, code: &str) -> Result<Site, MetadataError> {
        let row = sqlx::query(
            "SELECT site_id, code, name, timezone FROM sites WHERE code = $1",
        )
        .bind(code)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|r| Site {
            site_id: r.try_get("site_id").unwrap(),
            code: r.try_get("code").unwrap(),
            name: r.try_get("name").unwrap(),
            timezone: r.try_get("timezone").unwrap(),
        })
        .ok_or(MetadataError::NotFound)
    }

    pub async fn upsert_species(&self, species: &Species) -> Result<(), MetadataError> {
        sqlx::query(
            r#"
            INSERT INTO species (species_id, code, common_name, latin_name)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (species_id) DO UPDATE SET
                code = EXCLUDED.code,
                common_name = EXCLUDED.common_name,
                latin_name = EXCLUDED.latin_name
            "#,
        )
        .bind(species.species_id)
        .bind(&species.code)
        .bind(&species.common_name)
        .bind(&species.latin_name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fetch_species_by_code(&self, code: &str) -> Result<Species, MetadataError> {
        let row = sqlx::query(
            "SELECT species_id, code, common_name, latin_name FROM species WHERE code = $1",
        )
        .bind(code)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|r| Species {
            species_id: r.try_get("species_id").unwrap(),
            code: r.try_get("code").unwrap(),
            common_name: r.try_get("common_name").unwrap(),
            latin_name: r.try_get("latin_name").unwrap(),
        })
        .ok_or(MetadataError::NotFound)
    }

    pub async fn upsert_sensor_type(&self, sensor: &SensorType) -> Result<(), MetadataError> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            r#"
            INSERT INTO sensor_types (
                sensor_type_id,
                code,
                description,
                downstream_probe_distance_cm,
                upstream_probe_distance_cm
            ) VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (sensor_type_id) DO UPDATE SET
                code = EXCLUDED.code,
                description = EXCLUDED.description,
                downstream_probe_distance_cm = EXCLUDED.downstream_probe_distance_cm,
                upstream_probe_distance_cm = EXCLUDED.upstream_probe_distance_cm
            "#,
        )
        .bind(sensor.sensor_type_id)
        .bind(&sensor.code)
        .bind(&sensor.description)
        .bind(sensor.downstream_probe_distance_cm)
        .bind(sensor.upstream_probe_distance_cm)
        .execute(&mut *tx)
        .await?;

        sqlx::query("DELETE FROM sensor_thermistor_pairs WHERE sensor_type_id = $1")
            .bind(sensor.sensor_type_id)
            .execute(&mut *tx)
            .await?;

        for pair in &sensor.thermistor_pairs {
            sqlx::query(
                r#"
                INSERT INTO sensor_thermistor_pairs (
                    thermistor_pair_id,
                    sensor_type_id,
                    name,
                    depth_mm,
                    position_label,
                    aliases
                ) VALUES ($1, $2, $3, $4, $5, $6)
                "#,
            )
            .bind(pair.thermistor_pair_id)
            .bind(sensor.sensor_type_id)
            .bind(&pair.name)
            .bind(pair.depth_mm)
            .bind(&pair.position_label)
            .bind(serde_json::to_value(&pair.aliases).unwrap_or(Value::Null))
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn fetch_sensor_type_by_code( &self, code: &str) -> Result<SensorType, MetadataError> {
        let sensor_row = sqlx::query(
            "SELECT sensor_type_id, code, description, downstream_probe_distance_cm, upstream_probe_distance_cm FROM sensor_types WHERE code = $1",
        )
        .bind(code)
        .fetch_optional(&self.pool)
        .await?;

        let row = sqlx::query(
            "SELECT sensor_type_id, code, description, downstream_probe_distance_cm, upstream_probe_distance_cm FROM sensor_types WHERE code = $1",
        )
        .bind(code)
        .fetch_optional(&self.pool)
        .await?;

        let row = row.ok_or(MetadataError::NotFound)?;
        let sensor_type_id: Uuid = row.try_get("sensor_type_id")?;

        let thermistor_rows = sqlx::query(
            "SELECT thermistor_pair_id, name, depth_mm, position_label, aliases FROM sensor_thermistor_pairs WHERE sensor_type_id = $1",
        )
        .bind(sensor_type_id)
        .fetch_all(&self.pool)
        .await?;

        let thermistor_pairs = thermistor_rows
            .into_iter()
            .map(|r| {
                                let aliases: Option<Value> = r.try_get("aliases").unwrap_or(None);
                let aliases: Vec<String> = aliases
                    .and_then(|v| serde_json::from_value(v).ok())
                    .unwrap_or_default();

                ThermistorPair {
                    thermistor_pair_id: r.try_get("thermistor_pair_id").unwrap(),
                    name: r.try_get("name").unwrap(),
                    depth_mm: r.try_get("depth_mm").unwrap(),
                    position_label: r.try_get("position_label").unwrap(),
                    aliases,
                }
            })
            .collect();

        Ok(SensorType {
            sensor_type_id,
            code: row.try_get("code")?,
            description: row.try_get("description")?,
            downstream_probe_distance_cm: row.try_get("downstream_probe_distance_cm")?,
            upstream_probe_distance_cm: row.try_get("upstream_probe_distance_cm")?,
            thermistor_pairs,
        })
    }

    pub async fn upsert_parser_config(
        &self,
        config_id: Uuid,
        version: &str,
        settings: &Value,
    ) -> Result<(), MetadataError> {
        sqlx::query(
            r#"
            INSERT INTO parser_config (config_id, version, settings, updated_at)
            VALUES ($1, $2, $3, now())
            ON CONFLICT (config_id) DO UPDATE SET
                version = EXCLUDED.version,
                settings = EXCLUDED.settings,
                updated_at = now()
            "#,
        )
        .bind(config_id)
        .bind(version)
        .bind(settings)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fetch_parser_config(&self, config_id: Uuid) -> Result<ParserConfigRecord, MetadataError> {
        let row = sqlx::query(
            "SELECT config_id, version, settings, updated_at FROM parser_config WHERE config_id = $1",
        )
        .bind(config_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|r| ParserConfigRecord {
            config_id: r.try_get("config_id").unwrap(),
            version: r.try_get("version").unwrap(),
            settings: r.try_get("settings").unwrap(),
            updated_at: r.try_get("updated_at").unwrap(),
        })
        .ok_or(MetadataError::NotFound)
    }
}
