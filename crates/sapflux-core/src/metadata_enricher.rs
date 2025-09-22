use std::collections::{HashMap, HashSet};

use polars::prelude::*;
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum MetadataEnrichmentError {
    #[error("polars operation failed: {0}")]
    Polars(#[from] PolarsError),
}

#[derive(Debug, Clone)]
pub struct DeploymentRow {
    pub deployment_id: Uuid,
    pub datalogger_id: String,
    pub sdi_address: String,
    pub project_id: Uuid,
    pub site_id: Uuid,
    pub stem_id: Uuid,
    pub start_timestamp_utc: i64,
    pub end_timestamp_utc: Option<i64>,
    pub installation_metadata: HashMap<String, Value>,
}

pub fn enrich_with_metadata(
    observations: &DataFrame,
    deployments: &[DeploymentRow],
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

    let mut metadata_columns: HashMap<String, Vec<Option<String>>> = key_set
        .into_iter()
        .map(|key| (key, Vec::with_capacity(observations.height())))
        .collect();

    let mut deployment_ids = Vec::with_capacity(observations.height());
    let mut project_ids = Vec::with_capacity(observations.height());
    let mut site_ids = Vec::with_capacity(observations.height());
    let mut stem_ids = Vec::with_capacity(observations.height());

    for idx in 0..observations.height() {
        let logger = match logger_ids.get(idx) {
            Some(value) => value,
            None => {
                push_none(
                    &mut deployment_ids,
                    &mut project_ids,
                    &mut site_ids,
                    &mut stem_ids,
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
                    &mut project_ids,
                    &mut site_ids,
                    &mut stem_ids,
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
                    &mut project_ids,
                    &mut site_ids,
                    &mut stem_ids,
                    &mut metadata_columns,
                );
                continue;
            }
        };

        let key = (logger.to_string(), address.to_string());
        let deployment = deployment_map.get(&key).and_then(|deps| {
            deps.iter().find(|dep| {
                ts >= dep.start_timestamp_utc
                    && ts < dep.end_timestamp_utc.unwrap_or(i64::MAX)
            })
        });

        if let Some(dep) = deployment {
            deployment_ids.push(Some(dep.deployment_id.to_string()));
            project_ids.push(Some(dep.project_id.to_string()));
            site_ids.push(Some(dep.site_id.to_string()));
            stem_ids.push(Some(dep.stem_id.to_string()));

            for (key, values) in metadata_columns.iter_mut() {
                let value = dep.installation_metadata.get(key).and_then(value_to_string);
                values.push(value);
            }
        } else {
            push_none(
                &mut deployment_ids,
                &mut project_ids,
                &mut site_ids,
                &mut stem_ids,
                &mut metadata_columns,
            );
        }
    }

    let mut enriched = observations.clone();
    enriched.with_column(Series::new("deployment_id".into(), deployment_ids))?;
    enriched.with_column(Series::new("project_id".into(), project_ids))?;
    enriched.with_column(Series::new("site_id".into(), site_ids))?;
    enriched.with_column(Series::new("stem_id".into(), stem_ids))?;

    for (key, values) in metadata_columns {
        enriched.with_column(Series::new(key.into(), values))?;
    }

    Ok(enriched)
}

fn push_none(
    deployment_ids: &mut Vec<Option<String>>,
    project_ids: &mut Vec<Option<String>>,
    site_ids: &mut Vec<Option<String>>,
    stem_ids: &mut Vec<Option<String>>,
    metadata_columns: &mut HashMap<String, Vec<Option<String>>>,
) {
    deployment_ids.push(None);
    project_ids.push(None);
    site_ids.push(None);
    stem_ids.push(None);

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
