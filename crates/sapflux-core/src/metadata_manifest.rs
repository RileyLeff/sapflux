#![cfg(feature = "runtime")]

use std::collections::HashMap;
use std::convert::TryFrom;

use anyhow::{anyhow, ensure, Context, Result};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use geojson::{GeoJson, Value as GeoJsonValue};
use sapflux_parser::Sdi12Address;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::db::DbPool;

#[derive(Debug, Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
pub struct AddBlock<T> {
    #[serde(default)]
    pub add: Vec<T>,
}

impl<T> Default for AddBlock<T> {
    fn default() -> Self {
        Self { add: Vec::new() }
    }
}

#[derive(Debug, Deserialize)]
pub struct MetadataManifest {
    #[serde(default)]
    pub projects: AddBlock<ProjectAddEntry>,
    #[serde(default)]
    pub sites: AddBlock<SiteAddEntry>,
    #[serde(default)]
    pub zones: AddBlock<ZoneAddEntry>,
    #[serde(default)]
    pub plots: AddBlock<PlotAddEntry>,
    #[serde(default)]
    pub species: AddBlock<SpeciesAddEntry>,
    #[serde(default)]
    pub plants: AddBlock<PlantAddEntry>,
    #[serde(default)]
    pub stems: AddBlock<StemAddEntry>,
    #[serde(default)]
    pub datalogger_types: AddBlock<DataloggerTypeAddEntry>,
    #[serde(default)]
    pub dataloggers: AddBlock<DataloggerAddEntry>,
    #[serde(default)]
    pub datalogger_aliases: AddBlock<DataloggerAliasAddEntry>,
    #[serde(default)]
    pub sensor_types: AddBlock<SensorTypeAddEntry>,
    #[serde(default)]
    pub sensor_thermistor_pairs: AddBlock<SensorThermistorPairAddEntry>,
    #[serde(default)]
    pub deployments: Vec<DeploymentEntry>,
    #[serde(default, rename = "parameter_overrides")]
    pub parameter_overrides: Vec<ParameterOverrideEntry>,
}

impl MetadataManifest {
    pub fn is_empty(&self) -> bool {
        self.projects.add.is_empty()
            && self.sites.add.is_empty()
            && self.zones.add.is_empty()
            && self.plots.add.is_empty()
            && self.species.add.is_empty()
            && self.plants.add.is_empty()
            && self.stems.add.is_empty()
            && self.datalogger_types.add.is_empty()
            && self.dataloggers.add.is_empty()
            && self.datalogger_aliases.add.is_empty()
            && self.sensor_types.add.is_empty()
            && self.sensor_thermistor_pairs.add.is_empty()
            && self.deployments.is_empty()
            && self.parameter_overrides.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct MetadataSummary {
    pub projects_added: usize,
    pub sites_added: usize,
    pub zones_added: usize,
    pub plots_added: usize,
    pub species_added: usize,
    pub plants_added: usize,
    pub stems_added: usize,
    pub datalogger_types_added: usize,
    pub dataloggers_added: usize,
    pub datalogger_aliases_added: usize,
    pub sensor_types_added: usize,
    pub sensor_thermistor_pairs_added: usize,
    pub deployments_added: usize,
    pub parameter_overrides_upserted: usize,
}

#[derive(Debug, Default)]
pub struct ResolvedManifest {
    pub projects: Vec<ResolvedProject>,
    pub sites: Vec<ResolvedSite>,
    pub zones: Vec<ResolvedZone>,
    pub plots: Vec<ResolvedPlot>,
    pub species: Vec<ResolvedSpecies>,
    pub plants: Vec<ResolvedPlant>,
    pub stems: Vec<ResolvedStem>,
    pub datalogger_types: Vec<ResolvedDataloggerType>,
    pub dataloggers: Vec<ResolvedDatalogger>,
    pub datalogger_aliases: Vec<ResolvedDataloggerAlias>,
    pub sensor_types: Vec<ResolvedSensorType>,
    pub sensor_thermistor_pairs: Vec<ResolvedSensorThermistorPair>,
    pub deployments: Vec<ResolvedDeployment>,
    pub parameter_overrides: Vec<ResolvedParameterOverride>,
}

impl ResolvedManifest {
    fn is_empty(&self) -> bool {
        self.projects.is_empty()
            && self.sites.is_empty()
            && self.zones.is_empty()
            && self.plots.is_empty()
            && self.species.is_empty()
            && self.plants.is_empty()
            && self.stems.is_empty()
            && self.datalogger_types.is_empty()
            && self.dataloggers.is_empty()
            && self.datalogger_aliases.is_empty()
            && self.sensor_types.is_empty()
            && self.sensor_thermistor_pairs.is_empty()
            && self.deployments.is_empty()
            && self.parameter_overrides.is_empty()
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProjectAddEntry {
    pub code: String,
    pub name: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SiteAddEntry {
    pub code: String,
    pub name: Option<String>,
    pub timezone: String,
    pub icon_path: Option<String>,
    #[serde(default)]
    pub boundary: Option<Value>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ZoneAddEntry {
    pub site_code: String,
    pub name: String,
    #[serde(default)]
    pub boundary: Option<Value>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PlotAddEntry {
    pub site_code: String,
    pub zone_name: String,
    pub name: String,
    #[serde(default)]
    pub boundary: Option<Value>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SpeciesAddEntry {
    pub code: String,
    #[serde(default)]
    pub common_name: Option<Value>,
    #[serde(default)]
    pub latin_name: Option<Value>,
    pub icon_path: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PlantAddEntry {
    pub site_code: String,
    pub zone_name: String,
    pub plot_name: String,
    pub species_code: String,
    pub code: String,
    #[serde(default)]
    pub location: Option<Value>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StemAddEntry {
    pub plant_code: String,
    pub code: String,
    pub dbh_cm: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DataloggerTypeAddEntry {
    pub code: String,
    pub name: Option<String>,
    pub icon_path: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DataloggerAddEntry {
    pub datalogger_type_code: String,
    pub code: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DataloggerAliasAddEntry {
    pub datalogger_code: String,
    pub alias: String,
    pub start_timestamp_utc: DateTime<Utc>,
    pub end_timestamp_utc: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SensorTypeAddEntry {
    pub code: String,
    pub description: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SensorThermistorPairAddEntry {
    pub sensor_type_code: String,
    pub name: String,
    pub depth_mm: f64,
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
pub struct ResolvedProject {
    pub entry: ProjectAddEntry,
    pub project_id: Uuid,
}

#[derive(Debug)]
pub struct ResolvedSite {
    pub entry: SiteAddEntry,
    pub site_id: Uuid,
    pub boundary_geojson: Option<String>,
}

#[derive(Debug)]
pub struct ResolvedZone {
    pub entry: ZoneAddEntry,
    pub zone_id: Uuid,
    pub site_id: Uuid,
    pub boundary_geojson: Option<String>,
}

#[derive(Debug)]
pub struct ResolvedPlot {
    pub entry: PlotAddEntry,
    pub plot_id: Uuid,
    pub zone_id: Uuid,
    pub boundary_geojson: Option<String>,
}

#[derive(Debug)]
pub struct ResolvedSpecies {
    pub entry: SpeciesAddEntry,
    pub species_id: Uuid,
}

#[derive(Debug)]
pub struct ResolvedPlant {
    pub entry: PlantAddEntry,
    pub plant_id: Uuid,
    pub plot_id: Uuid,
    pub species_id: Uuid,
    pub location_geojson: Option<String>,
}

#[derive(Debug)]
pub struct ResolvedStem {
    pub entry: StemAddEntry,
    pub stem_id: Uuid,
    pub plant_id: Uuid,
}

#[derive(Debug)]
pub struct ResolvedDataloggerType {
    pub entry: DataloggerTypeAddEntry,
    pub datalogger_type_id: Uuid,
}

#[derive(Debug)]
pub struct ResolvedDatalogger {
    pub entry: DataloggerAddEntry,
    pub datalogger_id: Uuid,
    pub datalogger_type_id: Uuid,
}

#[derive(Debug)]
pub struct ResolvedDataloggerAlias {
    pub entry: DataloggerAliasAddEntry,
    pub datalogger_alias_id: Uuid,
    pub datalogger_id: Uuid,
    pub(crate) range: TimeRange,
}

#[derive(Debug)]
pub struct ResolvedSensorType {
    pub entry: SensorTypeAddEntry,
    pub sensor_type_id: Uuid,
}

#[derive(Debug)]
pub struct ResolvedSensorThermistorPair {
    pub entry: SensorThermistorPairAddEntry,
    pub thermistor_pair_id: Uuid,
    pub sensor_type_id: Uuid,
}

#[derive(Debug)]
pub struct ResolvedDeployment {
    pub entry: DeploymentEntry,
    pub project_id: Uuid,
    pub stem_id: Uuid,
    pub plant_id: Uuid,
    pub datalogger_id: Uuid,
    pub sensor_type_id: Uuid,
    pub(crate) range: TimeRange,
    pub installation_metadata: Value,
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
    let mut resolved = ResolvedManifest::default();
    let mut context = PreflightContext::load(pool).await?;

    for entry in &manifest.projects.add {
        ensure!(!entry.code.is_empty(), "project code cannot be empty");
        if context.projects.contains_key(&entry.code) {
            return Err(anyhow!("project '{}' already exists", entry.code));
        }
        let id = Uuid::new_v4();
        context
            .projects
            .insert(entry.code.clone(), ProjectRecord { id });
        resolved.projects.push(ResolvedProject {
            entry: entry.clone(),
            project_id: id,
        });
        summary.projects_added += 1;
    }

    for entry in &manifest.sites.add {
        ensure!(!entry.code.is_empty(), "site code cannot be empty");
        ensure_timezone_valid(&entry.timezone, &entry.code)?;
        if context.sites.contains_key(&entry.code) {
            return Err(anyhow!("site '{}' already exists", entry.code));
        }
        let boundary_geojson = value_to_geojson_string(
            entry.boundary.as_ref(),
            GeoExpectation::Polygon,
            &format!("site '{}' boundary", entry.code),
        )?;
        let id = Uuid::new_v4();
        context.sites.insert(entry.code.clone(), SiteRecord { id });
        resolved.sites.push(ResolvedSite {
            entry: entry.clone(),
            site_id: id,
            boundary_geojson,
        });
        summary.sites_added += 1;
    }

    for entry in &manifest.zones.add {
        let site_id = {
            let site = context.lookup_site(&entry.site_code)?;
            site.id
        };
        if context
            .zones
            .contains_key(&(entry.site_code.clone(), entry.name.clone()))
        {
            return Err(anyhow!(
                "zone '{}' already exists for site '{}'",
                entry.name,
                entry.site_code
            ));
        }
        let boundary_geojson = value_to_geojson_string(
            entry.boundary.as_ref(),
            GeoExpectation::Polygon,
            &format!("zone '{}' boundary", entry.name),
        )?;
        let id = Uuid::new_v4();
        context.insert_zone(
            entry.site_code.clone(),
            entry.name.clone(),
            ZoneRecord { id },
        );
        resolved.zones.push(ResolvedZone {
            entry: entry.clone(),
            zone_id: id,
            site_id,
            boundary_geojson,
        });
        summary.zones_added += 1;
    }

    for entry in &manifest.plots.add {
        context.lookup_site(&entry.site_code)?;
        let zone_id = {
            let zone = context.lookup_zone(&entry.site_code, &entry.zone_name)?;
            zone.id
        };
        if context.plots.contains_key(&(
            entry.site_code.clone(),
            entry.zone_name.clone(),
            entry.name.clone(),
        )) {
            return Err(anyhow!(
                "plot '{}' already exists for zone '{}' in site '{}'",
                entry.name,
                entry.zone_name,
                entry.site_code
            ));
        }
        let boundary_geojson = value_to_geojson_string(
            entry.boundary.as_ref(),
            GeoExpectation::Polygon,
            &format!("plot '{}' boundary", entry.name),
        )?;
        let id = Uuid::new_v4();
        context.insert_plot(
            entry.site_code.clone(),
            entry.zone_name.clone(),
            entry.name.clone(),
            PlotRecord { id },
        );
        resolved.plots.push(ResolvedPlot {
            entry: entry.clone(),
            plot_id: id,
            zone_id,
            boundary_geojson,
        });
        summary.plots_added += 1;
    }

    for entry in &manifest.species.add {
        ensure!(!entry.code.is_empty(), "species code cannot be empty");
        if context.species.contains_key(&entry.code) {
            return Err(anyhow!("species '{}' already exists", entry.code));
        }
        let id = Uuid::new_v4();
        context
            .species
            .insert(entry.code.clone(), SpeciesRecord { id });
        resolved.species.push(ResolvedSpecies {
            entry: entry.clone(),
            species_id: id,
        });
        summary.species_added += 1;
    }

    for entry in &manifest.plants.add {
        let plot_id = {
            let plot = context.lookup_plot(&entry.site_code, &entry.zone_name, &entry.plot_name)?;
            plot.id
        };
        let species_id = {
            let species = context.lookup_species(&entry.species_code)?;
            species.id
        };
        if context.plant_exists_in_plot(plot_id, &entry.code) {
            return Err(anyhow!(
                "plant '{}' already exists in plot '{}'",
                entry.code,
                plot_id
            ));
        }
        let location_geojson = value_to_geojson_string(
            entry.location.as_ref(),
            GeoExpectation::Point,
            &format!("plant '{}' location", entry.code),
        )?;
        let id = Uuid::new_v4();
        context.insert_plant(
            plot_id,
            entry.code.clone(),
            PlantRecord {
                id,
                code: entry.code.clone(),
            },
        );
        resolved.plants.push(ResolvedPlant {
            entry: entry.clone(),
            plant_id: id,
            plot_id,
            species_id,
            location_geojson,
        });
        summary.plants_added += 1;
    }

    for entry in &manifest.stems.add {
        let plant = context.lookup_plant(&entry.plant_code)?;
        let plant_id = plant.id;
        if context.stem_exists_for_plant(plant_id, &entry.code) {
            return Err(anyhow!(
                "stem '{}' already exists for plant '{}'",
                entry.code,
                plant_id
            ));
        }
        let id = Uuid::new_v4();
        context.insert_stem(plant_id, entry.code.clone(), StemRecord { id });
        resolved.stems.push(ResolvedStem {
            entry: entry.clone(),
            stem_id: id,
            plant_id,
        });
        summary.stems_added += 1;
    }

    for entry in &manifest.datalogger_types.add {
        ensure!(
            !entry.code.is_empty(),
            "datalogger type code cannot be empty"
        );
        if context.datalogger_types.contains_key(&entry.code) {
            return Err(anyhow!("datalogger type '{}' already exists", entry.code));
        }
        let id = Uuid::new_v4();
        context
            .datalogger_types
            .insert(entry.code.clone(), DataloggerTypeRecord { id });
        resolved.datalogger_types.push(ResolvedDataloggerType {
            entry: entry.clone(),
            datalogger_type_id: id,
        });
        summary.datalogger_types_added += 1;
    }

    for entry in &manifest.dataloggers.add {
        let datalogger_type_id = {
            let logger_type = context.lookup_datalogger_type(&entry.datalogger_type_code)?;
            logger_type.id
        };
        if context.dataloggers.contains_key(&entry.code) {
            return Err(anyhow!("datalogger '{}' already exists", entry.code));
        }
        let id = Uuid::new_v4();
        context.insert_datalogger(entry.code.clone(), DataloggerRecord { id });
        resolved.dataloggers.push(ResolvedDatalogger {
            entry: entry.clone(),
            datalogger_id: id,
            datalogger_type_id,
        });
        summary.dataloggers_added += 1;
    }

    for entry in &manifest.datalogger_aliases.add {
        ensure!(!entry.alias.is_empty(), "alias cannot be empty");
        let datalogger_id = {
            let datalogger = context.lookup_datalogger(&entry.datalogger_code)?;
            datalogger.id
        };
        let range = TimeRange::new(
            entry.start_timestamp_utc,
            entry.end_timestamp_utc,
            &format!("alias '{}'", entry.alias),
        )?;
        context.ensure_alias_available(&entry.alias, &range)?;
        let id = Uuid::new_v4();
        context
            .datalogger_aliases
            .entry(entry.alias.clone())
            .or_default()
            .push(range.clone());
        resolved.datalogger_aliases.push(ResolvedDataloggerAlias {
            entry: entry.clone(),
            datalogger_alias_id: id,
            datalogger_id,
            range,
        });
        summary.datalogger_aliases_added += 1;
    }

    for entry in &manifest.sensor_types.add {
        ensure!(!entry.code.is_empty(), "sensor type code cannot be empty");
        if context.sensor_types.contains_key(&entry.code) {
            return Err(anyhow!("sensor type '{}' already exists", entry.code));
        }
        let id = Uuid::new_v4();
        context
            .sensor_types
            .insert(entry.code.clone(), SensorTypeRecord { id });
        resolved.sensor_types.push(ResolvedSensorType {
            entry: entry.clone(),
            sensor_type_id: id,
        });
        summary.sensor_types_added += 1;
    }

    for entry in &manifest.sensor_thermistor_pairs.add {
        let sensor_type_id = {
            let sensor_type = context.lookup_sensor_type(&entry.sensor_type_code)?;
            sensor_type.id
        };
        let key = (entry.sensor_type_code.clone(), entry.name.clone());
        if context.sensor_pairs.contains_key(&key) {
            return Err(anyhow!(
                "thermistor pair '{}' already exists for sensor type '{}'",
                entry.name,
                entry.sensor_type_code
            ));
        }
        let id = Uuid::new_v4();
        context
            .sensor_pairs
            .insert(key, SensorThermistorPairRecord { id });
        resolved
            .sensor_thermistor_pairs
            .push(ResolvedSensorThermistorPair {
                entry: entry.clone(),
                thermistor_pair_id: id,
                sensor_type_id,
            });
        summary.sensor_thermistor_pairs_added += 1;
    }

    for entry in &manifest.deployments {
        let project_id = {
            let project = context.lookup_project(&entry.project_code)?;
            project.id
        };
        let plant = context.lookup_plant(&entry.plant_code)?;
        let plant_id = plant.id;
        let stem = context.lookup_stem_for_plant(plant_id, &entry.stem_code)?;
        let stem_id = stem.id;
        let datalogger_id = {
            let datalogger = context.lookup_datalogger(&entry.datalogger_code)?;
            datalogger.id
        };
        let sensor_type_id = {
            let sensor_type = context.lookup_sensor_type(&entry.sensor_type_code)?;
            sensor_type.id
        };
        validate_sdi12_address(&entry.sdi_address)?;

        let range = TimeRange::new(
            entry.start_timestamp_utc,
            entry.end_timestamp_utc,
            &format!(
                "deployment logger '{}' address '{}'",
                entry.datalogger_code, entry.sdi_address
            ),
        )?;

        context.ensure_deployment_available(datalogger_id, &entry.sdi_address, &range)?;

        let metadata = match entry.installation_metadata.clone() {
            Value::Null => json!({}),
            other => other,
        };

        context
            .deployment_ranges
            .entry((datalogger_id, entry.sdi_address.clone()))
            .or_default()
            .push(range.clone());

        resolved.deployments.push(ResolvedDeployment {
            entry: entry.clone(),
            project_id,
            stem_id,
            plant_id,
            datalogger_id,
            sensor_type_id,
            range,
            installation_metadata: metadata,
        });
        summary.deployments_added += 1;
    }

    for entry in &manifest.parameter_overrides {
        let parameter_id = context
            .lookup_parameter(&entry.parameter_code, pool)
            .await?;
        let site_id = match entry.site_code.as_deref() {
            Some(code) => Some(context.lookup_site(code)?.id),
            None => None,
        };
        let species_id = match entry.species_code.as_deref() {
            Some(code) => Some(context.lookup_species(code)?.id),
            None => None,
        };
        let zone_id = match entry.zone_name.as_deref() {
            Some(name) => Some(context.lookup_zone_by_name(name)?),
            None => None,
        };
        let plot_id = match entry.plot_name.as_deref() {
            Some(name) => Some(context.lookup_plot_by_name(name)?),
            None => None,
        };
        let plant = entry
            .plant_code
            .as_deref()
            .map(|code| context.lookup_plant(code))
            .transpose()?;
        let plant_id = plant.map(|record| record.id);
        let stem_id = match entry.stem_code.as_deref() {
            Some(code) => {
                let plant_id = plant_id.ok_or_else(|| {
                    anyhow!("parameter override for stem '{}' requires plant_code", code)
                })?;
                Some(context.lookup_stem_for_plant(plant_id, code)?.id)
            }
            None => None,
        };

        resolved
            .parameter_overrides
            .push(ResolvedParameterOverride {
                entry: entry.clone(),
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

    Ok((resolved, summary))
}

pub async fn apply_manifest(
    pool: &DbPool,
    resolved: &ResolvedManifest,
    triggering_transaction: Uuid,
) -> Result<()> {
    if resolved.is_empty() {
        return Ok(());
    }

    let mut tx = pool.begin().await?;

    for project in &resolved.projects {
        sqlx::query(
            r#"
                INSERT INTO projects (project_id, code, name, description)
                VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(project.project_id)
        .bind(&project.entry.code)
        .bind(project.entry.name.as_deref())
        .bind(project.entry.description.as_deref())
        .execute(&mut *tx)
        .await?;
    }

    for site in &resolved.sites {
        sqlx::query(
            r#"
                INSERT INTO sites (site_id, code, name, timezone, boundary, icon_path)
                VALUES (
                    $1,
                    $2,
                    $3,
                    $4,
                    CASE WHEN $5 IS NULL THEN NULL ELSE ST_SetSRID(ST_GeomFromGeoJSON($5), 4326) END,
                    $6
                )
            "#,
        )
        .bind(site.site_id)
        .bind(&site.entry.code)
        .bind(site.entry.name.as_deref())
        .bind(&site.entry.timezone)
        .bind(site.boundary_geojson.as_deref())
        .bind(site.entry.icon_path.as_deref())
        .execute(&mut *tx)
        .await?;
    }

    for zone in &resolved.zones {
        sqlx::query(
            r#"
                INSERT INTO zones (zone_id, site_id, name, boundary)
                VALUES (
                    $1,
                    $2,
                    $3,
                    CASE WHEN $4 IS NULL THEN NULL ELSE ST_SetSRID(ST_GeomFromGeoJSON($4), 4326) END
                )
            "#,
        )
        .bind(zone.zone_id)
        .bind(zone.site_id)
        .bind(&zone.entry.name)
        .bind(zone.boundary_geojson.as_deref())
        .execute(&mut *tx)
        .await?;
    }

    for plot in &resolved.plots {
        sqlx::query(
            r#"
                INSERT INTO plots (plot_id, zone_id, name, boundary)
                VALUES (
                    $1,
                    $2,
                    $3,
                    CASE WHEN $4 IS NULL THEN NULL ELSE ST_SetSRID(ST_GeomFromGeoJSON($4), 4326) END
                )
            "#,
        )
        .bind(plot.plot_id)
        .bind(plot.zone_id)
        .bind(&plot.entry.name)
        .bind(plot.boundary_geojson.as_deref())
        .execute(&mut *tx)
        .await?;
    }

    for species in &resolved.species {
        sqlx::query(
            r#"
                INSERT INTO species (species_id, code, common_name, latin_name, icon_path)
                VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(species.species_id)
        .bind(&species.entry.code)
        .bind(species.entry.common_name.clone().map(sqlx::types::Json))
        .bind(species.entry.latin_name.clone().map(sqlx::types::Json))
        .bind(species.entry.icon_path.as_deref())
        .execute(&mut *tx)
        .await?;
    }

    for plant in &resolved.plants {
        sqlx::query(
            r#"
                INSERT INTO plants (plant_id, plot_id, species_id, code, location)
                VALUES (
                    $1,
                    $2,
                    $3,
                    $4,
                    CASE WHEN $5 IS NULL THEN NULL ELSE ST_SetSRID(ST_GeomFromGeoJSON($5), 4326) END
                )
            "#,
        )
        .bind(plant.plant_id)
        .bind(plant.plot_id)
        .bind(plant.species_id)
        .bind(&plant.entry.code)
        .bind(plant.location_geojson.as_deref())
        .execute(&mut *tx)
        .await?;
    }

    for stem in &resolved.stems {
        sqlx::query(
            r#"
                INSERT INTO stems (stem_id, plant_id, code, dbh_cm)
                VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(stem.stem_id)
        .bind(stem.plant_id)
        .bind(&stem.entry.code)
        .bind(stem.entry.dbh_cm)
        .execute(&mut *tx)
        .await?;
    }

    for logger_type in &resolved.datalogger_types {
        sqlx::query(
            r#"
                INSERT INTO datalogger_types (datalogger_type_id, code, name, icon_path)
                VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(logger_type.datalogger_type_id)
        .bind(&logger_type.entry.code)
        .bind(logger_type.entry.name.as_deref())
        .bind(logger_type.entry.icon_path.as_deref())
        .execute(&mut *tx)
        .await?;
    }

    for datalogger in &resolved.dataloggers {
        sqlx::query(
            r#"
                INSERT INTO dataloggers (datalogger_id, datalogger_type_id, code)
                VALUES ($1, $2, $3)
            "#,
        )
        .bind(datalogger.datalogger_id)
        .bind(datalogger.datalogger_type_id)
        .bind(&datalogger.entry.code)
        .execute(&mut *tx)
        .await?;
    }

    for alias in &resolved.datalogger_aliases {
        sqlx::query(
            r#"
                INSERT INTO datalogger_aliases (
                    datalogger_alias_id,
                    datalogger_id,
                    alias,
                    active_during
                )
                VALUES ($1, $2, $3, tstzrange($4, $5, '[)'))
            "#,
        )
        .bind(alias.datalogger_alias_id)
        .bind(alias.datalogger_id)
        .bind(&alias.entry.alias)
        .bind(alias.range.start)
        .bind(alias.range.end)
        .execute(&mut *tx)
        .await?;
    }

    for sensor_type in &resolved.sensor_types {
        sqlx::query(
            r#"
                INSERT INTO sensor_types (sensor_type_id, code, description)
                VALUES ($1, $2, $3)
            "#,
        )
        .bind(sensor_type.sensor_type_id)
        .bind(&sensor_type.entry.code)
        .bind(sensor_type.entry.description.as_deref())
        .execute(&mut *tx)
        .await?;
    }

    for pair in &resolved.sensor_thermistor_pairs {
        sqlx::query(
            r#"
                INSERT INTO sensor_thermistor_pairs (thermistor_pair_id, sensor_type_id, name, depth_mm)
                VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(pair.thermistor_pair_id)
        .bind(pair.sensor_type_id)
        .bind(&pair.entry.name)
        .bind(pair.entry.depth_mm)
        .execute(&mut *tx)
        .await?;
    }

    for deployment in &resolved.deployments {
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
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(deployment.project_id)
        .bind(deployment.stem_id)
        .bind(deployment.datalogger_id)
        .bind(deployment.sensor_type_id)
        .bind(&deployment.entry.sdi_address)
        .bind(deployment.range.start)
        .bind(deployment.range.end)
        .bind(sqlx::types::Json(deployment.installation_metadata.clone()))
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
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, $10)
                ON CONFLICT (parameter_id, site_id, species_id, zone_id, plot_id, plant_id, stem_id, deployment_id)
                DO UPDATE SET
                    value = EXCLUDED.value,
                    effective_transaction_id = EXCLUDED.effective_transaction_id
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(override_entry.parameter_id)
        .bind(sqlx::types::Json(override_entry.entry.value.clone()))
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

fn ensure_timezone_valid(value: &str, code: &str) -> Result<()> {
    value
        .parse::<Tz>()
        .map(|_| ())
        .map_err(|_| anyhow!("site '{}' has invalid timezone '{}'", code, value))
}

fn validate_sdi12_address(address: &str) -> Result<()> {
    Sdi12Address::try_from(address)
        .map(|_| ())
        .map_err(|_| anyhow!("invalid SDI-12 address '{}'", address))
}

#[derive(Debug, Clone)]
pub(crate) struct TimeRange {
    start: DateTime<Utc>,
    end: Option<DateTime<Utc>>,
}

impl TimeRange {
    fn new(start: DateTime<Utc>, end: Option<DateTime<Utc>>, label: &str) -> Result<Self> {
        if let Some(end_ts) = end {
            ensure!(end_ts > start, "{} end must be after start", label);
        }
        Ok(TimeRange { start, end })
    }
}

enum GeoExpectation {
    Polygon,
    Point,
}

fn value_to_geojson_string(
    value: Option<&Value>,
    expected: GeoExpectation,
    label: &str,
) -> Result<Option<String>> {
    let Some(raw) = value else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }

    let geojson = if let Value::String(text) = raw {
        text.parse::<GeoJson>()
            .with_context(|| format!("{} is not valid GeoJSON", label))?
    } else {
        GeoJson::from_json_value(raw.clone())
            .with_context(|| format!("{} is not valid GeoJSON", label))?
    };

    let geometry = match &geojson {
        GeoJson::Geometry(geometry) => geometry,
        _ => return Err(anyhow!("{} must be a GeoJSON geometry", label)),
    };

    match expected {
        GeoExpectation::Polygon => {
            if !matches!(geometry.value, GeoJsonValue::Polygon(_)) {
                return Err(anyhow!("{} must be a GeoJSON Polygon", label));
            }
        }
        GeoExpectation::Point => {
            if !matches!(geometry.value, GeoJsonValue::Point(_)) {
                return Err(anyhow!("{} must be a GeoJSON Point", label));
            }
        }
    }

    Ok(Some(geojson.to_string()))
}

fn ranges_overlap_or_touch(a: &TimeRange, b: &TimeRange) -> bool {
    if let Some(end) = a.end {
        if end < b.start {
            return false;
        }
    }
    if let Some(end) = b.end {
        if end < a.start {
            return false;
        }
    }
    if let Some(end) = a.end {
        if end == b.start {
            return true;
        }
    }
    if let Some(end) = b.end {
        if end == a.start {
            return true;
        }
    }
    true
}

struct ProjectRecord {
    id: Uuid,
}

struct SiteRecord {
    id: Uuid,
}

struct ZoneRecord {
    id: Uuid,
}

struct PlotRecord {
    id: Uuid,
}

struct SpeciesRecord {
    id: Uuid,
}

struct PlantRecord {
    id: Uuid,
    code: String,
}

struct StemRecord {
    id: Uuid,
}

struct DataloggerTypeRecord {
    id: Uuid,
}

struct DataloggerRecord {
    id: Uuid,
}

struct SensorTypeRecord {
    id: Uuid,
}

#[allow(dead_code)]
struct SensorThermistorPairRecord {
    id: Uuid,
}

struct PreflightContext {
    projects: HashMap<String, ProjectRecord>,
    sites: HashMap<String, SiteRecord>,
    zones: HashMap<(String, String), ZoneRecord>,
    zones_by_name: HashMap<String, Vec<Uuid>>,
    plots: HashMap<(String, String, String), PlotRecord>,
    plots_by_name: HashMap<String, Vec<Uuid>>,
    species: HashMap<String, SpeciesRecord>,
    plants: HashMap<(Uuid, String), PlantRecord>,
    stems: HashMap<(Uuid, String), StemRecord>,
    datalogger_types: HashMap<String, DataloggerTypeRecord>,
    dataloggers: HashMap<String, DataloggerRecord>,
    sensor_types: HashMap<String, SensorTypeRecord>,
    sensor_pairs: HashMap<(String, String), SensorThermistorPairRecord>,
    datalogger_aliases: HashMap<String, Vec<TimeRange>>,
    deployment_ranges: HashMap<(Uuid, String), Vec<TimeRange>>,
    parameter_cache: HashMap<String, Uuid>,
}

impl PreflightContext {
    async fn load(pool: &DbPool) -> Result<Self> {
        let mut ctx = PreflightContext {
            projects: HashMap::new(),
            sites: HashMap::new(),
            zones: HashMap::new(),
            zones_by_name: HashMap::new(),
            plots: HashMap::new(),
            plots_by_name: HashMap::new(),
            species: HashMap::new(),
            plants: HashMap::new(),
            stems: HashMap::new(),
            datalogger_types: HashMap::new(),
            dataloggers: HashMap::new(),
            sensor_types: HashMap::new(),
            sensor_pairs: HashMap::new(),
            datalogger_aliases: HashMap::new(),
            deployment_ranges: HashMap::new(),
            parameter_cache: HashMap::new(),
        };

        ctx.load_projects(pool).await?;
        ctx.load_sites(pool).await?;
        ctx.load_zones(pool).await?;
        ctx.load_plots(pool).await?;
        ctx.load_species(pool).await?;
        ctx.load_plants(pool).await?;
        ctx.load_stems(pool).await?;
        ctx.load_datalogger_types(pool).await?;
        ctx.load_dataloggers(pool).await?;
        ctx.load_sensor_types(pool).await?;
        ctx.load_sensor_pairs(pool).await?;
        ctx.load_aliases(pool).await?;
        ctx.load_deployments(pool).await?;
        Ok(ctx)
    }

    async fn load_projects(&mut self, pool: &DbPool) -> Result<()> {
        let rows = sqlx::query("SELECT project_id, code FROM projects")
            .fetch_all(pool)
            .await?;
        for row in rows {
            let id: Uuid = row.try_get("project_id")?;
            let code: String = row.try_get("code")?;
            self.projects.insert(code, ProjectRecord { id });
        }
        Ok(())
    }

    async fn load_sites(&mut self, pool: &DbPool) -> Result<()> {
        let rows = sqlx::query("SELECT site_id, code FROM sites")
            .fetch_all(pool)
            .await?;
        for row in rows {
            let id: Uuid = row.try_get("site_id")?;
            let code: String = row.try_get("code")?;
            self.sites.insert(code, SiteRecord { id });
        }
        Ok(())
    }

    async fn load_zones(&mut self, pool: &DbPool) -> Result<()> {
        let rows = sqlx::query(
            r#"
                SELECT z.zone_id, z.name AS zone_name, s.code AS site_code
                FROM zones z
                JOIN sites s ON z.site_id = s.site_id
            "#,
        )
        .fetch_all(pool)
        .await?;
        for row in rows {
            let id: Uuid = row.try_get("zone_id")?;
            let zone_name: String = row.try_get("zone_name")?;
            let site_code: String = row.try_get("site_code")?;
            self.zones
                .insert((site_code.clone(), zone_name.clone()), ZoneRecord { id });
            self.zones_by_name.entry(zone_name).or_default().push(id);
        }
        Ok(())
    }

    async fn load_plots(&mut self, pool: &DbPool) -> Result<()> {
        let rows = sqlx::query(
            r#"
                SELECT p.plot_id, p.name AS plot_name, z.name AS zone_name, s.code AS site_code
                FROM plots p
                JOIN zones z ON p.zone_id = z.zone_id
                JOIN sites s ON z.site_id = s.site_id
            "#,
        )
        .fetch_all(pool)
        .await?;
        for row in rows {
            let id: Uuid = row.try_get("plot_id")?;
            let plot_name: String = row.try_get("plot_name")?;
            let zone_name: String = row.try_get("zone_name")?;
            let site_code: String = row.try_get("site_code")?;
            self.plots.insert(
                (site_code.clone(), zone_name.clone(), plot_name.clone()),
                PlotRecord { id },
            );
            self.plots_by_name.entry(plot_name).or_default().push(id);
        }
        Ok(())
    }

    async fn load_species(&mut self, pool: &DbPool) -> Result<()> {
        let rows = sqlx::query("SELECT species_id, code FROM species")
            .fetch_all(pool)
            .await?;
        for row in rows {
            let id: Uuid = row.try_get("species_id")?;
            let code: String = row.try_get("code")?;
            self.species.insert(code, SpeciesRecord { id });
        }
        Ok(())
    }

    async fn load_plants(&mut self, pool: &DbPool) -> Result<()> {
        let rows = sqlx::query("SELECT plant_id, plot_id, code FROM plants")
            .fetch_all(pool)
            .await?;
        for row in rows {
            let id: Uuid = row.try_get("plant_id")?;
            let plot_id: Uuid = row.try_get("plot_id")?;
            let code: String = row.try_get("code")?;
            self.plants
                .insert((plot_id, code.clone()), PlantRecord { id, code });
        }
        Ok(())
    }

    async fn load_stems(&mut self, pool: &DbPool) -> Result<()> {
        let rows = sqlx::query("SELECT stem_id, plant_id, code FROM stems")
            .fetch_all(pool)
            .await?;
        for row in rows {
            let id: Uuid = row.try_get("stem_id")?;
            let plant_id: Uuid = row.try_get("plant_id")?;
            let code: String = row.try_get("code")?;
            self.stems.insert((plant_id, code), StemRecord { id });
        }
        Ok(())
    }

    async fn load_datalogger_types(&mut self, pool: &DbPool) -> Result<()> {
        let rows = sqlx::query("SELECT datalogger_type_id, code FROM datalogger_types")
            .fetch_all(pool)
            .await?;
        for row in rows {
            let id: Uuid = row.try_get("datalogger_type_id")?;
            let code: String = row.try_get("code")?;
            self.datalogger_types
                .insert(code, DataloggerTypeRecord { id });
        }
        Ok(())
    }

    async fn load_dataloggers(&mut self, pool: &DbPool) -> Result<()> {
        let rows = sqlx::query("SELECT datalogger_id, code FROM dataloggers")
            .fetch_all(pool)
            .await?;
        for row in rows {
            let id: Uuid = row.try_get("datalogger_id")?;
            let code: String = row.try_get("code")?;
            self.dataloggers.insert(code, DataloggerRecord { id });
        }
        Ok(())
    }

    async fn load_sensor_types(&mut self, pool: &DbPool) -> Result<()> {
        let rows = sqlx::query("SELECT sensor_type_id, code FROM sensor_types")
            .fetch_all(pool)
            .await?;
        for row in rows {
            let id: Uuid = row.try_get("sensor_type_id")?;
            let code: String = row.try_get("code")?;
            self.sensor_types.insert(code, SensorTypeRecord { id });
        }
        Ok(())
    }

    async fn load_sensor_pairs(&mut self, pool: &DbPool) -> Result<()> {
        let rows = sqlx::query(
            r#"
                SELECT tp.thermistor_pair_id,
                       tp.name AS pair_name,
                       st.code AS sensor_type_code
                FROM sensor_thermistor_pairs tp
                JOIN sensor_types st ON tp.sensor_type_id = st.sensor_type_id
            "#,
        )
        .fetch_all(pool)
        .await?;
        for row in rows {
            let id: Uuid = row.try_get("thermistor_pair_id")?;
            let name: String = row.try_get("pair_name")?;
            let sensor_type_code: String = row.try_get("sensor_type_code")?;
            self.sensor_pairs
                .insert((sensor_type_code, name), SensorThermistorPairRecord { id });
        }
        Ok(())
    }

    async fn load_aliases(&mut self, pool: &DbPool) -> Result<()> {
        let rows = sqlx::query(
            r#"
                SELECT alias,
                       lower(active_during) AS start_ts,
                       upper(active_during) AS end_ts
                FROM datalogger_aliases
            "#,
        )
        .fetch_all(pool)
        .await?;
        for row in rows {
            let alias: String = row.try_get("alias")?;
            let start: DateTime<Utc> = row.try_get("start_ts")?;
            let end: Option<DateTime<Utc>> = row.try_get("end_ts")?;
            self.datalogger_aliases
                .entry(alias)
                .or_default()
                .push(TimeRange { start, end });
        }
        Ok(())
    }

    async fn load_deployments(&mut self, pool: &DbPool) -> Result<()> {
        let rows = sqlx::query(
            r#"
                SELECT datalogger_id,
                       sdi_address,
                       start_timestamp_utc,
                       end_timestamp_utc
                FROM deployments
            "#,
        )
        .fetch_all(pool)
        .await?;
        for row in rows {
            let datalogger_id: Uuid = row.try_get("datalogger_id")?;
            let address: String = row.try_get("sdi_address")?;
            let start: DateTime<Utc> = row.try_get("start_timestamp_utc")?;
            let end: Option<DateTime<Utc>> = row.try_get("end_timestamp_utc")?;
            self.deployment_ranges
                .entry((datalogger_id, address))
                .or_default()
                .push(TimeRange { start, end });
        }
        Ok(())
    }

    fn lookup_project(&self, code: &str) -> Result<&ProjectRecord> {
        self.projects
            .get(code)
            .ok_or_else(|| anyhow!("project '{}' not found", code))
    }

    fn lookup_site(&self, code: &str) -> Result<&SiteRecord> {
        self.sites
            .get(code)
            .ok_or_else(|| anyhow!("site '{}' not found", code))
    }

    fn lookup_zone(&self, site_code: &str, zone_name: &str) -> Result<&ZoneRecord> {
        self.zones
            .get(&(site_code.to_string(), zone_name.to_string()))
            .ok_or_else(|| anyhow!("zone '{}' not found for site '{}'", zone_name, site_code))
    }

    fn lookup_plot(
        &self,
        site_code: &str,
        zone_name: &str,
        plot_name: &str,
    ) -> Result<&PlotRecord> {
        self.plots
            .get(&(
                site_code.to_string(),
                zone_name.to_string(),
                plot_name.to_string(),
            ))
            .ok_or_else(|| {
                anyhow!(
                    "plot '{}' not found for zone '{}' in site '{}'",
                    plot_name,
                    zone_name,
                    site_code
                )
            })
    }

    fn lookup_species(&self, code: &str) -> Result<&SpeciesRecord> {
        self.species
            .get(code)
            .ok_or_else(|| anyhow!("species '{}' not found", code))
    }

    fn plant_exists_in_plot(&self, plot_id: Uuid, code: &str) -> bool {
        self.plants.contains_key(&(plot_id, code.to_string()))
    }

    fn lookup_plant(&self, code: &str) -> Result<&PlantRecord> {
        let mut matches = self.plants.values().filter(|record| record.code == code);
        let first = matches
            .next()
            .ok_or_else(|| anyhow!("plant '{}' not found", code))?;
        if matches.next().is_some() {
            return Err(anyhow!(
                "plant '{}' exists in multiple plots; ensure plant codes are unique per plot",
                code
            ));
        }
        Ok(first)
    }

    fn stem_exists_for_plant(&self, plant_id: Uuid, code: &str) -> bool {
        self.stems.contains_key(&(plant_id, code.to_string()))
    }

    fn lookup_stem_for_plant(&self, plant_id: Uuid, code: &str) -> Result<&StemRecord> {
        self.stems
            .get(&(plant_id, code.to_string()))
            .ok_or_else(|| anyhow!("stem '{}' not found for plant '{}'", code, plant_id))
    }

    // lookup_stem_for_plant provides the canonical stem resolution; callers that
    // only have a stem code must first resolve the owning plant.

    fn lookup_datalogger_type(&self, code: &str) -> Result<&DataloggerTypeRecord> {
        self.datalogger_types
            .get(code)
            .ok_or_else(|| anyhow!("datalogger type '{}' not found", code))
    }

    fn lookup_datalogger(&self, code: &str) -> Result<&DataloggerRecord> {
        self.dataloggers
            .get(code)
            .ok_or_else(|| anyhow!("datalogger '{}' not found", code))
    }

    fn lookup_sensor_type(&self, code: &str) -> Result<&SensorTypeRecord> {
        self.sensor_types
            .get(code)
            .ok_or_else(|| anyhow!("sensor type '{}' not found", code))
    }

    fn lookup_zone_by_name(&self, name: &str) -> Result<Uuid> {
        match self.zones_by_name.get(name) {
            Some(ids) if ids.len() == 1 => Ok(ids[0]),
            Some(_) => Err(anyhow!("zone name '{}' is ambiguous across sites", name)),
            None => Err(anyhow!("zone '{}' not found", name)),
        }
    }

    fn lookup_plot_by_name(&self, name: &str) -> Result<Uuid> {
        match self.plots_by_name.get(name) {
            Some(ids) if ids.len() == 1 => Ok(ids[0]),
            Some(_) => Err(anyhow!(
                "plot name '{}' is ambiguous across zones/sites",
                name
            )),
            None => Err(anyhow!("plot '{}' not found", name)),
        }
    }

    fn insert_zone(&mut self, site_code: String, zone_name: String, record: ZoneRecord) {
        let id = record.id;
        self.zones
            .insert((site_code.clone(), zone_name.clone()), record);
        self.zones_by_name.entry(zone_name).or_default().push(id);
    }

    fn insert_plot(
        &mut self,
        site_code: String,
        zone_name: String,
        plot_name: String,
        record: PlotRecord,
    ) {
        let id = record.id;
        self.plots
            .insert((site_code, zone_name, plot_name.clone()), record);
        self.plots_by_name.entry(plot_name).or_default().push(id);
    }

    fn insert_plant(&mut self, plot_id: Uuid, code: String, record: PlantRecord) {
        self.plants.insert((plot_id, code), record);
    }

    fn insert_stem(&mut self, plant_id: Uuid, code: String, record: StemRecord) {
        self.stems.insert((plant_id, code), record);
    }

    fn insert_datalogger(&mut self, code: String, record: DataloggerRecord) {
        self.dataloggers.insert(code, record);
    }

    fn ensure_alias_available(&self, alias: &str, range: &TimeRange) -> Result<()> {
        if let Some(existing) = self.datalogger_aliases.get(alias) {
            for existing_range in existing {
                if ranges_overlap_or_touch(existing_range, range) {
                    return Err(anyhow!(
                        "alias '{}' overlaps or touches an existing interval",
                        alias
                    ));
                }
            }
        }
        Ok(())
    }

    fn ensure_deployment_available(
        &self,
        datalogger_id: Uuid,
        address: &str,
        range: &TimeRange,
    ) -> Result<()> {
        if let Some(existing) = self
            .deployment_ranges
            .get(&(datalogger_id, address.to_string()))
        {
            for existing_range in existing {
                if ranges_overlap_or_touch(existing_range, range) {
                    return Err(anyhow!(
                        "deployment for logger '{}' address '{}' overlaps or touches an existing interval",
                        datalogger_id,
                        address
                    ));
                }
            }
        }
        Ok(())
    }

    async fn lookup_parameter(&mut self, code: &str, pool: &DbPool) -> Result<Uuid> {
        if let Some(id) = self.parameter_cache.get(code) {
            return Ok(*id);
        }
        let id =
            sqlx::query_scalar::<_, Uuid>("SELECT parameter_id FROM parameters WHERE code = $1")
                .bind(code)
                .fetch_optional(pool)
                .await?
                .ok_or_else(|| anyhow!("parameter '{}' not found", code))?;
        self.parameter_cache.insert(code.to_string(), id);
        Ok(id)
    }
}

fn default_true() -> bool {
    true
}

fn default_object() -> Value {
    json!({})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_manifest_defaults() {
        let toml = r#"
[metadata]
# empty
"#;

        let manifest = parse_manifest(toml).expect("parse manifest");
        assert!(manifest.projects.add.is_empty());
        assert!(manifest.deployments.is_empty());
        assert!(manifest.parameter_overrides.is_empty());
    }
}
