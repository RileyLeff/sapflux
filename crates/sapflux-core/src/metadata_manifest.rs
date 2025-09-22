#![cfg(feature = "runtime")]

use std::collections::HashSet;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::db::DbPool;

#[derive(Debug, Deserialize)]
pub struct MetadataManifest {
    #[serde(default)]
    pub deployments: Vec<DeploymentEntry>,
    #[serde(default, rename = "parameter_overrides")]
    pub parameter_overrides: Vec<ParameterOverrideEntry>,
}

impl MetadataManifest {
    pub fn is_empty(&self) -> bool {
        self.deployments.is_empty() && self.parameter_overrides.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct MetadataSummary {
    pub deployments_added: usize,
    pub parameter_overrides_upserted: usize,
}

#[derive(Debug)]
pub struct ResolvedManifest {
    pub deployments: Vec<ResolvedDeployment>,
    pub parameter_overrides: Vec<ResolvedParameterOverride>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DeploymentEntry {
    pub project_code: String,
    pub plant_code: String,
    pub stem_code: String,
    pub datalogger_code: String,
    pub sensor_type_code: String,
    pub sdi_address: String,
    pub start_timestamp_utc: DateTime<Utc>,
    pub end_timestamp_utc: Option<DateTime<Utc>>,
    #[serde(default = "default_object")]
    pub installation_metadata: Value,
    #[serde(default = "default_true")]
    pub include_in_pipeline: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ParameterOverrideEntry {
    pub parameter_code: String,
    pub value: Value,
    pub site_code: Option<String>,
    pub species_code: Option<String>,
    pub zone_name: Option<String>,
    pub plot_name: Option<String>,
    pub plant_code: Option<String>,
    pub stem_code: Option<String>,
}

#[derive(Debug)]
pub struct ResolvedDeployment {
    pub entry: DeploymentEntry,
    pub project_id: Uuid,
    pub stem_id: Uuid,
    pub datalogger_id: Uuid,
    pub sensor_type_id: Uuid,
}

#[derive(Debug)]
pub struct ResolvedParameterOverride {
    pub entry: ParameterOverrideEntry,
    pub parameter_id: Uuid,
    pub site_id: Option<Uuid>,
    pub species_id: Option<Uuid>,
    pub zone_id: Option<Uuid>,
    pub plot_id: Option<Uuid>,
    pub plant_id: Option<Uuid>,
    pub stem_id: Option<Uuid>,
}

pub fn parse_manifest(toml_str: &str) -> Result<MetadataManifest> {
    toml::from_str::<MetadataManifest>(toml_str).context("failed to parse metadata manifest TOML")
}

pub async fn preflight_manifest(
    pool: &DbPool,
    manifest: &MetadataManifest,
) -> Result<(ResolvedManifest, MetadataSummary)> {
    let mut summary = MetadataSummary::default();
    let mut deployments = Vec::with_capacity(manifest.deployments.len());
    let mut seen_deployments = HashSet::new();

    for deployment in &manifest.deployments {
        let project_id = fetch_uuid(
            pool,
            "SELECT project_id FROM projects WHERE code = $1",
            &deployment.project_code,
        )
        .await
        .with_context(|| format!("project '{}' not found", deployment.project_code))?;
        let stem_id = fetch_uuid(
            pool,
            "SELECT stem_id FROM stems WHERE code = $1",
            &deployment.stem_code,
        )
        .await
        .with_context(|| format!("stem '{}' not found", deployment.stem_code))?;
        let datalogger_id = fetch_uuid(
            pool,
            "SELECT datalogger_id FROM dataloggers WHERE code = $1",
            &deployment.datalogger_code,
        )
        .await
        .with_context(|| format!("datalogger '{}' not found", deployment.datalogger_code))?;
        let sensor_type_id = fetch_uuid(
            pool,
            "SELECT sensor_type_id FROM sensor_types WHERE code = $1",
            &deployment.sensor_type_code,
        )
        .await
        .with_context(|| format!("sensor type '{}' not found", deployment.sensor_type_code))?;

        let key = (
            stem_id,
            datalogger_id,
            deployment.sdi_address.clone(),
            deployment.start_timestamp_utc,
        );
        if !seen_deployments.insert(key.clone()) {
            return Err(anyhow!(
                "duplicate deployment entry for stem '{}' datalogger '{}' and start {}",
                deployment.stem_code,
                deployment.datalogger_code,
                deployment.start_timestamp_utc
            ));
        }

        let exists = sqlx::query_scalar::<_, i64>(
            r#"
                SELECT 1
                FROM deployments
                WHERE stem_id = $1
                  AND datalogger_id = $2
                  AND sdi_address = $3
                  AND start_timestamp_utc = $4
            "#,
        )
        .bind(stem_id)
        .bind(datalogger_id)
        .bind(&deployment.sdi_address)
        .bind(deployment.start_timestamp_utc)
        .fetch_optional(pool)
        .await?
        .is_some();

        if exists {
            return Err(anyhow!(
                "deployment already exists for stem '{}' and datalogger '{}' at start {}",
                deployment.stem_code,
                deployment.datalogger_code,
                deployment.start_timestamp_utc
            ));
        }

        deployments.push(ResolvedDeployment {
            entry: deployment.clone(),
            project_id,
            stem_id,
            datalogger_id,
            sensor_type_id,
        });
        summary.deployments_added += 1;
    }

    let mut overrides = Vec::with_capacity(manifest.parameter_overrides.len());
    for override_entry in &manifest.parameter_overrides {
        let parameter_id = fetch_uuid(
            pool,
            "SELECT parameter_id FROM parameters WHERE code = $1",
            &override_entry.parameter_code,
        )
        .await
        .with_context(|| format!("parameter '{}' not found", override_entry.parameter_code))?;

        let site_id = match override_entry.site_code.as_deref() {
            Some(code) => Some(
                fetch_uuid(pool, "SELECT site_id FROM sites WHERE code = $1", code)
                    .await
                    .with_context(|| format!("site '{}' not found", code))?,
            ),
            None => None,
        };
        let species_id = match override_entry.species_code.as_deref() {
            Some(code) => Some(
                fetch_uuid(pool, "SELECT species_id FROM species WHERE code = $1", code)
                    .await
                    .with_context(|| format!("species '{}' not found", code))?,
            ),
            None => None,
        };
        let zone_id = match override_entry.zone_name.as_deref() {
            Some(name) => Some(
                fetch_uuid(pool, "SELECT zone_id FROM zones WHERE name = $1", name)
                    .await
                    .with_context(|| format!("zone '{}' not found", name))?,
            ),
            None => None,
        };
        let plot_id = match override_entry.plot_name.as_deref() {
            Some(name) => Some(
                fetch_uuid(pool, "SELECT plot_id FROM plots WHERE name = $1", name)
                    .await
                    .with_context(|| format!("plot '{}' not found", name))?,
            ),
            None => None,
        };
        let plant_id = match override_entry.plant_code.as_deref() {
            Some(code) => Some(
                fetch_uuid(pool, "SELECT plant_id FROM plants WHERE code = $1", code)
                    .await
                    .with_context(|| format!("plant '{}' not found", code))?,
            ),
            None => None,
        };
        let stem_id = match override_entry.stem_code.as_deref() {
            Some(code) => Some(
                fetch_uuid(pool, "SELECT stem_id FROM stems WHERE code = $1", code)
                    .await
                    .with_context(|| format!("stem '{}' not found", code))?,
            ),
            None => None,
        };

        overrides.push(ResolvedParameterOverride {
            entry: override_entry.clone(),
            parameter_id,
            site_id,
            species_id,
            zone_id,
            plot_id,
            plant_id,
            stem_id,
        });
        summary.parameter_overrides_upserted += 1;
    }

    Ok((
        ResolvedManifest {
            deployments,
            parameter_overrides: overrides,
        },
        summary,
    ))
}

pub async fn apply_manifest(
    pool: &DbPool,
    resolved: &ResolvedManifest,
    triggering_transaction: Uuid,
) -> Result<()> {
    if resolved.deployments.is_empty() && resolved.parameter_overrides.is_empty() {
        return Ok(());
    }

    let mut tx = pool.begin().await?;

    for deployment in &resolved.deployments {
        let installation_metadata = match deployment.entry.installation_metadata.clone() {
            Value::Null => json!({}),
            other => other,
        };

        sqlx::query(
            r#"
                INSERT INTO deployments (
                    deployment_id,
                    project_id,
                    stem_id,
                    datalogger_id,
                    sensor_type_id,
                    sdi_address,
                    start_timestamp_utc,
                    end_timestamp_utc,
                    installation_metadata,
                    include_in_pipeline
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(deployment.project_id)
        .bind(deployment.stem_id)
        .bind(deployment.datalogger_id)
        .bind(deployment.sensor_type_id)
        .bind(&deployment.entry.sdi_address)
        .bind(deployment.entry.start_timestamp_utc)
        .bind(deployment.entry.end_timestamp_utc)
        .bind(&installation_metadata)
        .bind(deployment.entry.include_in_pipeline)
        .execute(&mut *tx)
        .await?;
    }

    for override_entry in &resolved.parameter_overrides {
        sqlx::query(
            r#"
                INSERT INTO parameter_overrides (
                    override_id,
                    parameter_id,
                    value,
                    site_id,
                    species_id,
                    zone_id,
                    plot_id,
                    plant_id,
                    stem_id,
                    deployment_id,
                    effective_transaction_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, $10)
                ON CONFLICT (parameter_id, site_id, species_id, zone_id, plot_id, plant_id, stem_id, deployment_id)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    effective_transaction_id = EXCLUDED.effective_transaction_id
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(override_entry.parameter_id)
        .bind(&override_entry.entry.value)
        .bind(override_entry.site_id)
        .bind(override_entry.species_id)
        .bind(override_entry.zone_id)
        .bind(override_entry.plot_id)
        .bind(override_entry.plant_id)
        .bind(override_entry.stem_id)
        .bind(triggering_transaction)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

async fn fetch_uuid(pool: &DbPool, sql: &str, param: &str) -> Result<Uuid> {
    sqlx::query_scalar::<_, Uuid>(sql)
        .bind(param)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| anyhow!("record not found"))
}

const fn default_true() -> bool {
    true
}

fn default_object() -> Value {
    json!({})
}
