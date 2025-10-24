use std::collections::{HashMap, HashSet};

use polars::prelude::*;
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum MetadataEnrichmentError {
    #[error("polars operation failed: {0}")]
    Polars(#[from] PolarsError),
    #[error(
        "multiple deployments matched logger {logger_id} / address {sdi_address} at timestamp {timestamp}"
    )]
    AmbiguousDeployment {
        logger_id: String,
        sdi_address: String,
        timestamp: i64,
    },
    #[error("multiple aliases matched alias {alias} at timestamp {timestamp}")]
    AmbiguousAlias { alias: String, timestamp: i64 },
}

#[derive(Debug, Clone)]
pub struct DeploymentRow {
    pub deployment_id: Uuid,
    pub datalogger_id: String,
    pub sdi_address: String,
    pub project_id: Uuid,
    pub project_code: Option<String>,
    pub project_name: Option<String>,
    pub site_id: Uuid,
    pub site_code: Option<String>,
    pub site_name: Option<String>,
    pub zone_id: Option<Uuid>,
    pub zone_name: Option<String>,
    pub plot_id: Option<Uuid>,
    pub plot_name: Option<String>,
    pub plant_id: Option<Uuid>,
    pub plant_code: Option<String>,
    pub species_id: Option<Uuid>,
    pub species_code: Option<String>,
    pub species_scientific_name: Option<String>,
    pub stem_id: Uuid,
    pub stem_code: Option<String>,
    pub start_timestamp_utc: i64,
    pub end_timestamp_utc: Option<i64>,
    pub installation_metadata: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct DataloggerAliasRow {
    pub alias: String,
    pub datalogger_id: String,
    pub start_timestamp_utc: i64,
    pub end_timestamp_utc: Option<i64>,
}

pub fn enrich_with_metadata(
    observations: &DataFrame,
    deployments: &[DeploymentRow],
    aliases: &[DataloggerAliasRow],
) -> Result<DataFrame, MetadataEnrichmentError> {
    if observations.is_empty() {
        return Ok(observations.clone());
    }

    let logger_ids = observations
        .column("logger_id")?
        .as_materialized_series()
        .str()?;
    let sdi_addresses = observations
        .column("sdi12_address")?
        .as_materialized_series()
        .str()?;
    let timestamps = observations
        .column("timestamp_utc")?
        .as_materialized_series()
        .datetime()?;

    let mut key_set: HashSet<String> = HashSet::new();
    let mut deployment_map: HashMap<(String, String), Vec<&DeploymentRow>> = HashMap::new();
    let mut alias_map: HashMap<&str, Vec<&DataloggerAliasRow>> = HashMap::new();

    for deployment in deployments {
        for key in deployment.installation_metadata.keys() {
            key_set.insert(key.clone());
        }
        deployment_map
            .entry((
                deployment.datalogger_id.clone(),
                deployment.sdi_address.clone(),
            ))
            .or_default()
            .push(deployment);
    }

    for entries in deployment_map.values_mut() {
        entries.sort_by_key(|dep| dep.start_timestamp_utc);
    }

    for alias in aliases {
        alias_map
            .entry(alias.alias.as_str())
            .or_default()
            .push(alias);
    }

    for entries in alias_map.values_mut() {
        entries.sort_by_key(|alias| alias.start_timestamp_utc);
    }

    let mut metadata_columns: HashMap<String, Vec<Option<String>>> = key_set
        .into_iter()
        .map(|key| (key, Vec::with_capacity(observations.height())))
        .collect();

    let mut deployment_ids = Vec::with_capacity(observations.height());
    let mut datalogger_ids = Vec::with_capacity(observations.height());
    let mut project_ids = Vec::with_capacity(observations.height());
    let mut site_ids = Vec::with_capacity(observations.height());
    let mut zone_ids = Vec::with_capacity(observations.height());
    let mut plot_ids = Vec::with_capacity(observations.height());
    let mut plant_ids = Vec::with_capacity(observations.height());
    let mut species_ids = Vec::with_capacity(observations.height());
    let mut stem_ids = Vec::with_capacity(observations.height());
    let mut project_codes = Vec::with_capacity(observations.height());
    let mut project_names = Vec::with_capacity(observations.height());
    let mut site_codes = Vec::with_capacity(observations.height());
    let mut site_names = Vec::with_capacity(observations.height());
    let mut zone_names = Vec::with_capacity(observations.height());
    let mut plot_names = Vec::with_capacity(observations.height());
    let mut plant_codes = Vec::with_capacity(observations.height());
    let mut stem_codes = Vec::with_capacity(observations.height());
    let mut species_codes = Vec::with_capacity(observations.height());
    let mut species_scientific_names = Vec::with_capacity(observations.height());
    let mut deployment_start_us = Vec::with_capacity(observations.height());
    let mut deployment_end_us = Vec::with_capacity(observations.height());

    for idx in 0..observations.height() {
        let logger = match logger_ids.get(idx) {
            Some(value) => value,
            None => {
                push_none(
                    &mut deployment_ids,
                    &mut datalogger_ids,
                    &mut project_ids,
                    &mut site_ids,
                    &mut zone_ids,
                    &mut plot_ids,
                    &mut plant_ids,
                    &mut species_ids,
                    &mut stem_ids,
                    &mut project_codes,
                    &mut project_names,
                    &mut site_codes,
                    &mut site_names,
                    &mut zone_names,
                    &mut plot_names,
                    &mut plant_codes,
                    &mut stem_codes,
                    &mut species_codes,
                    &mut species_scientific_names,
                    &mut deployment_start_us,
                    &mut deployment_end_us,
                    &mut metadata_columns,
                );
                continue;
            }
        };
        let address = match sdi_addresses.get(idx) {
            Some(value) => value,
            None => {
                push_none(
                    &mut deployment_ids,
                    &mut datalogger_ids,
                    &mut project_ids,
                    &mut site_ids,
                    &mut zone_ids,
                    &mut plot_ids,
                    &mut plant_ids,
                    &mut species_ids,
                    &mut stem_ids,
                    &mut project_codes,
                    &mut project_names,
                    &mut site_codes,
                    &mut site_names,
                    &mut zone_names,
                    &mut plot_names,
                    &mut plant_codes,
                    &mut stem_codes,
                    &mut species_codes,
                    &mut species_scientific_names,
                    &mut deployment_start_us,
                    &mut deployment_end_us,
                    &mut metadata_columns,
                );
                continue;
            }
        };
        let ts = match timestamps.get(idx) {
            Some(value) => value,
            None => {
                push_none(
                    &mut deployment_ids,
                    &mut datalogger_ids,
                    &mut project_ids,
                    &mut site_ids,
                    &mut zone_ids,
                    &mut plot_ids,
                    &mut plant_ids,
                    &mut species_ids,
                    &mut stem_ids,
                    &mut project_codes,
                    &mut project_names,
                    &mut site_codes,
                    &mut site_names,
                    &mut zone_names,
                    &mut plot_names,
                    &mut plant_codes,
                    &mut stem_codes,
                    &mut species_codes,
                    &mut species_scientific_names,
                    &mut deployment_start_us,
                    &mut deployment_end_us,
                    &mut metadata_columns,
                );
                continue;
            }
        };

        let mut canonical_logger = logger.to_string();
        let mut deployment = select_deployment(
            deployment_map.get(&(canonical_logger.clone(), address.to_string())),
            &canonical_logger,
            address,
            ts,
        )?;

        if deployment.is_none() {
            if let Some(alias_rows) = alias_map.get(logger) {
                if let Some(alias_row) = select_alias(alias_rows, logger, ts)? {
                    canonical_logger = alias_row.datalogger_id.clone();
                    deployment = select_deployment(
                        deployment_map.get(&(canonical_logger.clone(), address.to_string())),
                        &canonical_logger,
                        address,
                        ts,
                    )?;
                }
            }
        }

        if let Some(dep) = deployment {
            deployment_ids.push(Some(dep.deployment_id.to_string()));
            datalogger_ids.push(Some(dep.datalogger_id.clone()));
            project_ids.push(Some(dep.project_id.to_string()));
            site_ids.push(Some(dep.site_id.to_string()));
            zone_ids.push(dep.zone_id.map(|id| id.to_string()));
            plot_ids.push(dep.plot_id.map(|id| id.to_string()));
            plant_ids.push(dep.plant_id.map(|id| id.to_string()));
            species_ids.push(dep.species_id.map(|id| id.to_string()));
            stem_ids.push(Some(dep.stem_id.to_string()));
            project_codes.push(dep.project_code.clone());
            project_names.push(dep.project_name.clone());
            site_codes.push(dep.site_code.clone());
            site_names.push(dep.site_name.clone());
            zone_names.push(dep.zone_name.clone());
            plot_names.push(dep.plot_name.clone());
            plant_codes.push(dep.plant_code.clone());
            stem_codes.push(dep.stem_code.clone());
            species_codes.push(dep.species_code.clone());
            species_scientific_names.push(dep.species_scientific_name.clone());
            deployment_start_us.push(Some(dep.start_timestamp_utc));
            deployment_end_us.push(dep.end_timestamp_utc);

            for (key, values) in metadata_columns.iter_mut() {
                let value = dep.installation_metadata.get(key).and_then(value_to_string);
                values.push(value);
            }
        } else {
            push_none(
                &mut deployment_ids,
                &mut datalogger_ids,
                &mut project_ids,
                &mut site_ids,
                &mut zone_ids,
                &mut plot_ids,
                &mut plant_ids,
                &mut species_ids,
                &mut stem_ids,
                &mut project_codes,
                &mut project_names,
                &mut site_codes,
                &mut site_names,
                &mut zone_names,
                &mut plot_names,
                &mut plant_codes,
                &mut stem_codes,
                &mut species_codes,
                &mut species_scientific_names,
                &mut deployment_start_us,
                &mut deployment_end_us,
                &mut metadata_columns,
            );
        }
    }

    let mut enriched = observations.clone();
    enriched.with_column(Series::new("deployment_id".into(), deployment_ids))?;
    enriched.with_column(Series::new("datalogger_id".into(), datalogger_ids))?;
    enriched.with_column(Series::new("project_id".into(), project_ids))?;
    enriched.with_column(Series::new("site_id".into(), site_ids))?;
    enriched.with_column(Series::new("zone_id".into(), zone_ids))?;
    enriched.with_column(Series::new("plot_id".into(), plot_ids))?;
    enriched.with_column(Series::new("plant_id".into(), plant_ids))?;
    enriched.with_column(Series::new("species_id".into(), species_ids))?;
    enriched.with_column(Series::new("stem_id".into(), stem_ids))?;
    enriched.with_column(Series::new("project_code".into(), project_codes))?;
    enriched.with_column(Series::new("project_name".into(), project_names))?;
    enriched.with_column(Series::new("site_code".into(), site_codes))?;
    enriched.with_column(Series::new("site_name".into(), site_names))?;
    enriched.with_column(Series::new("zone_name".into(), zone_names))?;
    enriched.with_column(Series::new("plot_name".into(), plot_names))?;
    enriched.with_column(Series::new("plant_code".into(), plant_codes))?;
    enriched.with_column(Series::new("stem_code".into(), stem_codes))?;
    enriched.with_column(Series::new("species_code".into(), species_codes))?;
    enriched.with_column(Series::new(
        "species_scientific_name".into(),
        species_scientific_names,
    ))?;
    enriched.with_column(
        Series::new("deployment_start_timestamp_utc".into(), deployment_start_us).cast(
            &DataType::Datetime(TimeUnit::Microseconds, Some(polars::prelude::TimeZone::UTC)),
        )?,
    )?;
    enriched.with_column(
        Series::new("deployment_end_timestamp_utc".into(), deployment_end_us).cast(
            &DataType::Datetime(TimeUnit::Microseconds, Some(polars::prelude::TimeZone::UTC)),
        )?,
    )?;

    for (key, values) in metadata_columns {
        enriched.with_column(Series::new(key.into(), values))?;
    }

    Ok(enriched)
}

fn timestamp_in_range(ts: i64, start: i64, end: Option<i64>) -> bool {
    ts >= start && ts < end.unwrap_or(i64::MAX)
}

fn select_deployment<'a>(
    entries: Option<&'a Vec<&'a DeploymentRow>>,
    logger_id: &str,
    sdi_address: &str,
    timestamp: i64,
) -> Result<Option<&'a DeploymentRow>, MetadataEnrichmentError> {
    let Some(entries) = entries else {
        return Ok(None);
    };

    let mut matches = entries
        .iter()
        .copied()
        .filter(|dep| timestamp_in_range(timestamp, dep.start_timestamp_utc, dep.end_timestamp_utc))
        .peekable();

    let first = matches.next();
    if matches.peek().is_some() {
        return Err(MetadataEnrichmentError::AmbiguousDeployment {
            logger_id: logger_id.to_string(),
            sdi_address: sdi_address.to_string(),
            timestamp,
        });
    }

    Ok(first)
}

fn select_alias<'a>(
    entries: &'a Vec<&'a DataloggerAliasRow>,
    alias: &str,
    timestamp: i64,
) -> Result<Option<&'a DataloggerAliasRow>, MetadataEnrichmentError> {
    let mut matches = entries
        .iter()
        .copied()
        .filter(|row| timestamp_in_range(timestamp, row.start_timestamp_utc, row.end_timestamp_utc))
        .peekable();

    let first = matches.next();
    if matches.peek().is_some() {
        return Err(MetadataEnrichmentError::AmbiguousAlias {
            alias: alias.to_string(),
            timestamp,
        });
    }

    Ok(first)
}

#[allow(clippy::too_many_arguments)]
fn push_none(
    deployment_ids: &mut Vec<Option<String>>,
    datalogger_ids: &mut Vec<Option<String>>,
    project_ids: &mut Vec<Option<String>>,
    site_ids: &mut Vec<Option<String>>,
    zone_ids: &mut Vec<Option<String>>,
    plot_ids: &mut Vec<Option<String>>,
    plant_ids: &mut Vec<Option<String>>,
    species_ids: &mut Vec<Option<String>>,
    stem_ids: &mut Vec<Option<String>>,
    project_codes: &mut Vec<Option<String>>,
    project_names: &mut Vec<Option<String>>,
    site_codes: &mut Vec<Option<String>>,
    site_names: &mut Vec<Option<String>>,
    zone_names: &mut Vec<Option<String>>,
    plot_names: &mut Vec<Option<String>>,
    plant_codes: &mut Vec<Option<String>>,
    stem_codes: &mut Vec<Option<String>>,
    species_codes: &mut Vec<Option<String>>,
    species_scientific_names: &mut Vec<Option<String>>,
    deployment_start_us: &mut Vec<Option<i64>>,
    deployment_end_us: &mut Vec<Option<i64>>,
    metadata_columns: &mut HashMap<String, Vec<Option<String>>>,
) {
    deployment_ids.push(None);
    datalogger_ids.push(None);
    project_ids.push(None);
    site_ids.push(None);
    zone_ids.push(None);
    plot_ids.push(None);
    plant_ids.push(None);
    species_ids.push(None);
    stem_ids.push(None);
    project_codes.push(None);
    project_names.push(None);
    site_codes.push(None);
    site_names.push(None);
    zone_names.push(None);
    plot_names.push(None);
    plant_codes.push(None);
    stem_codes.push(None);
    species_codes.push(None);
    species_scientific_names.push(None);
    deployment_start_us.push(None);
    deployment_end_us.push(None);

    for values in metadata_columns.values_mut() {
        values.push(None);
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(s.clone()),
        Value::Array(_) | Value::Object(_) => Some(value.to_string()),
    }
}
