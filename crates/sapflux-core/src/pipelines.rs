use anyhow::{anyhow, Result};
use chrono::Utc;
use once_cell::sync::Lazy;
use polars::prelude::DataFrame;

#[cfg(feature = "runtime")]
use anyhow::Context;
#[cfg(feature = "runtime")]
use chrono::DateTime;
#[cfg(feature = "runtime")]
use serde_json::Value;
#[cfg(feature = "runtime")]
use std::collections::HashMap;
#[cfg(feature = "runtime")]
use uuid::Uuid;

use crate::{
    calculator,
    flatten::flatten_parsed_files,
    metadata_enricher::{
        self, DataloggerAliasRow as EnrichmentAliasRow, DeploymentRow as EnrichmentDeploymentRow,
    },
    parameter_resolver::{self, ParameterDefinition, ParameterOverride},
    parsers::ParsedData,
    quality_filters,
    timestamp_fixer::{
        self, DeploymentMetadata as TsDeploymentMetadata, SiteMetadata as TsSiteMetadata,
        SkippedChunk,
    },
};
use sapflux_parser::ParsedFileData;

#[cfg(feature = "runtime")]
use crate::db::DbPool;
#[cfg(feature = "runtime")]
use sqlx::Row;

#[derive(Debug)]
pub struct ExecutionContext {
    pub timestamp_sites: Vec<TsSiteMetadata>,
    pub timestamp_deployments: Vec<TsDeploymentMetadata>,
    pub enrichment_deployments: Vec<EnrichmentDeploymentRow>,
    pub datalogger_aliases: Vec<EnrichmentAliasRow>,
    pub parameter_definitions: Vec<ParameterDefinition>,
    pub parameter_overrides: Vec<ParameterOverride>,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self {
            timestamp_sites: Vec::new(),
            timestamp_deployments: Vec::new(),
            enrichment_deployments: Vec::new(),
            datalogger_aliases: Vec::new(),
            parameter_definitions: parameter_resolver::canonical_parameter_definitions(),
            parameter_overrides: Vec::new(),
        }
    }
}

impl ExecutionContext {
    #[cfg(feature = "runtime")]
    pub async fn load_from_db(pool: &DbPool) -> Result<Self> {
        let site_rows = sqlx::query(r#"SELECT site_id, timezone FROM sites"#)
            .fetch_all(pool)
            .await?;

        let mut site_tz_map: HashMap<Uuid, chrono_tz::Tz> = HashMap::new();
        let mut timestamp_sites = Vec::with_capacity(site_rows.len());
        for row in site_rows {
            let site_id: Uuid = row.try_get("site_id")?;
            let timezone: String = row.try_get("timezone")?;
            let tz: chrono_tz::Tz = timezone
                .parse()
                .with_context(|| format!("unknown timezone {}", timezone))?;
            site_tz_map.insert(site_id, tz);
            timestamp_sites.push(TsSiteMetadata {
                site_id,
                timezone: tz,
            });
        }

        let deployment_rows = sqlx::query(
            r#"
            SELECT
                d.deployment_id,
                d.project_id,
                projects.code AS project_code,
                projects.name AS project_name,
                d.stem_id,
                d.sdi_address,
                d.start_timestamp_utc,
                d.end_timestamp_utc,
                d.installation_metadata,
                dl.code AS datalogger_code,
                s.site_id,
                s.code AS site_code,
                s.name AS site_name,
                zones.zone_id,
                zones.name AS zone_name,
                plots.plot_id,
                plots.name AS plot_name,
                plants.plant_id,
                plants.code AS plant_code,
                species.species_id,
                species.code AS species_code,
                species.latin_name AS species_latin_name,
                st.code AS stem_code
            FROM deployments d
            JOIN dataloggers dl ON d.datalogger_id = dl.datalogger_id
            JOIN stems st ON d.stem_id = st.stem_id
            JOIN plants ON st.plant_id = plants.plant_id
            JOIN plots ON plants.plot_id = plots.plot_id
            JOIN zones ON plots.zone_id = zones.zone_id
            JOIN sites s ON zones.site_id = s.site_id
            JOIN species ON plants.species_id = species.species_id
            JOIN projects ON d.project_id = projects.project_id
            WHERE d.include_in_pipeline = TRUE
            "#,
        )
        .fetch_all(pool)
        .await?;

        let mut timestamp_deployments = Vec::with_capacity(deployment_rows.len());
        let mut enrichment_deployments = Vec::with_capacity(deployment_rows.len());

        for row in deployment_rows {
            let deployment_id: Uuid = row.try_get("deployment_id")?;
            let project_id: Uuid = row.try_get("project_id")?;
            let project_code: String = row.try_get("project_code")?;
            let project_name: Option<String> = row.try_get("project_name")?;
            let stem_id: Uuid = row.try_get("stem_id")?;
            let sdi_address: String = row.try_get("sdi_address")?;
            let start_timestamp_utc: DateTime<Utc> = row.try_get("start_timestamp_utc")?;
            let end_timestamp_utc: Option<DateTime<Utc>> = row.try_get("end_timestamp_utc")?;
            let installation_metadata: Option<Value> = row.try_get("installation_metadata")?;
            let datalogger_code: String = row.try_get("datalogger_code")?;
            let site_id: Uuid = row.try_get("site_id")?;
            let site_code: String = row.try_get("site_code")?;
            let site_name: Option<String> = row.try_get("site_name")?;
            let zone_id: Option<Uuid> = row.try_get("zone_id")?;
            let zone_name: Option<String> = row.try_get("zone_name")?;
            let plot_id: Option<Uuid> = row.try_get("plot_id")?;
            let plot_name: Option<String> = row.try_get("plot_name")?;
            let plant_id: Uuid = row.try_get("plant_id")?;
            let plant_code: String = row.try_get("plant_code")?;
            let species_id: Uuid = row.try_get("species_id")?;
            let species_code: String = row.try_get("species_code")?;
            let species_latin_name: Option<Value> = row.try_get("species_latin_name")?;
            let stem_code: String = row.try_get("stem_code")?;

            let tz = site_tz_map
                .get(&site_id)
                .copied()
                .ok_or_else(|| anyhow!("missing timezone for site {}", site_id))?;

            let start_local = start_timestamp_utc.with_timezone(&tz).naive_local();
            let end_local = end_timestamp_utc.map(|dt| dt.with_timezone(&tz).naive_local());

            timestamp_deployments.push(TsDeploymentMetadata {
                datalogger_id: datalogger_code.clone(),
                site_id,
                start_timestamp_local: start_local,
                end_timestamp_local: end_local,
            });

            let installation_metadata = json_value_to_map(installation_metadata);
            let species_scientific_name = scientific_name_from_latin(species_latin_name.as_ref());

            enrichment_deployments.push(EnrichmentDeploymentRow {
                deployment_id,
                datalogger_id: datalogger_code.clone(),
                sdi_address,
                project_id,
                project_code: Some(project_code),
                project_name,
                site_id,
                site_code: Some(site_code),
                site_name,
                zone_id,
                zone_name,
                plot_id,
                plot_name,
                plant_id: Some(plant_id),
                plant_code: Some(plant_code),
                species_id: Some(species_id),
                species_code: Some(species_code),
                species_scientific_name,
                stem_id,
                stem_code: Some(stem_code),
                start_timestamp_utc: datetime_to_utc_micros(start_timestamp_utc),
                end_timestamp_utc: end_timestamp_utc.map(datetime_to_utc_micros),
                installation_metadata,
            });
        }

        let alias_rows = sqlx::query(
            r#"
                SELECT
                    da.alias,
                    dl.code AS datalogger_code,
                    lower(da.active_during) AS start_utc,
                    upper(da.active_during) AS end_utc
                FROM datalogger_aliases da
                JOIN dataloggers dl ON da.datalogger_id = dl.datalogger_id
            "#,
        )
        .fetch_all(pool)
        .await?;

        let mut datalogger_aliases = Vec::with_capacity(alias_rows.len());
        for row in alias_rows {
            let alias: String = row.try_get("alias")?;
            let datalogger_code: String = row.try_get("datalogger_code")?;
            let start_utc: DateTime<Utc> = row.try_get("start_utc")?;
            let end_utc: Option<DateTime<Utc>> = row.try_get("end_utc")?;

            datalogger_aliases.push(EnrichmentAliasRow {
                alias,
                datalogger_id: datalogger_code,
                start_timestamp_utc: datetime_to_utc_micros(start_utc),
                end_timestamp_utc: end_utc.map(datetime_to_utc_micros),
            });
        }

        let override_rows = sqlx::query(
            r#"
                SELECT
                    p.code,
                    po.value,
                    po.site_id,
                    po.species_id,
                    po.zone_id,
                    po.plot_id,
                    po.plant_id,
                    po.stem_id,
                    po.deployment_id
                FROM parameter_overrides po
                JOIN parameters p ON po.parameter_id = p.parameter_id
            "#,
        )
        .fetch_all(pool)
        .await?;

        let mut parameter_overrides = Vec::with_capacity(override_rows.len());
        for row in override_rows {
            parameter_overrides.push(ParameterOverride {
                code: row.try_get("code")?,
                value: row.try_get("value")?,
                site_id: row.try_get("site_id")?,
                species_id: row.try_get("species_id")?,
                zone_id: row.try_get("zone_id")?,
                plot_id: row.try_get("plot_id")?,
                plant_id: row.try_get("plant_id")?,
                stem_id: row.try_get("stem_id")?,
                deployment_id: row.try_get("deployment_id")?,
            });
        }

        Ok(Self {
            timestamp_sites,
            timestamp_deployments,
            enrichment_deployments,
            datalogger_aliases,
            parameter_definitions: parameter_resolver::canonical_parameter_definitions(),
            parameter_overrides,
        })
    }
}

pub struct PipelineBatchOutput {
    pub dataframe: DataFrame,
    pub skipped_chunks: Vec<SkippedChunk>,
}

pub trait ProcessingPipeline: Send + Sync {
    fn code_identifier(&self) -> &'static str;
    fn version(&self) -> &'static str;
    fn input_data_format(&self) -> &'static str;
    fn run_batch(
        &self,
        _context: &ExecutionContext,
        _parsed_batch: &[&dyn ParsedData],
    ) -> Result<PipelineBatchOutput>;
}

#[derive(Debug, Clone)]
pub struct ProcessingPipelineDescriptor {
    pub code: &'static str,
    pub version: &'static str,
    pub input_data_format: &'static str,
    pub include_in_pipeline: bool,
    pub description: &'static str,
}

static PIPELINES: Lazy<Vec<ProcessingPipelineDescriptor>> = Lazy::new(|| {
    vec![ProcessingPipelineDescriptor {
        code: "standard_v1_dst_fix",
        version: "0.1.0",
        input_data_format: "sapflow_toa5_hierarchical_v1",
        include_in_pipeline: true,
        description: "Timestamp fix + metadata enrichment + DMA Peclet calculation",
    }]
});

pub fn all_pipeline_descriptors() -> &'static [ProcessingPipelineDescriptor] {
    PIPELINES.as_slice()
}

static PIPELINE_IMPLEMENTATIONS: Lazy<Vec<&'static dyn ProcessingPipeline>> =
    Lazy::new(|| vec![&StandardPipelineStub as &dyn ProcessingPipeline]);

pub fn all_pipelines() -> &'static [&'static dyn ProcessingPipeline] {
    PIPELINE_IMPLEMENTATIONS.as_slice()
}

struct StandardPipelineStub;

impl ProcessingPipeline for StandardPipelineStub {
    fn code_identifier(&self) -> &'static str {
        "standard_v1_dst_fix"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn input_data_format(&self) -> &'static str {
        "sapflow_toa5_hierarchical_v1"
    }

    fn run_batch(
        &self,
        context: &ExecutionContext,
        _parsed_batch: &[&dyn ParsedData],
    ) -> Result<PipelineBatchOutput> {
        if context.timestamp_deployments.is_empty() || context.timestamp_sites.is_empty() {
            return Err(anyhow!(
                "standard_v1_dst_fix requires deployment/site metadata"
            ));
        }

        let mut typed_files: Vec<&ParsedFileData> = Vec::with_capacity(_parsed_batch.len());
        for parsed in _parsed_batch {
            let file = parsed.downcast_ref::<ParsedFileData>().ok_or_else(|| {
                anyhow!("standard_v1_dst_fix requires sapflow_toa5_hierarchical_v1")
            })?;
            typed_files.push(file);
        }

        let flattened = flatten_parsed_files(&typed_files)?;
        let timestamp_result = timestamp_fixer::correct_timestamps(
            &flattened,
            &context.timestamp_sites,
            &context.timestamp_deployments,
        )?;
        let corrected = timestamp_result.dataframe;
        let skipped_chunks = timestamp_result.skipped_chunks;

        let enriched = metadata_enricher::enrich_with_metadata(
            &corrected,
            &context.enrichment_deployments,
            &context.datalogger_aliases,
        )?;

        let resolved = if context.parameter_definitions.is_empty() {
            enriched
        } else {
            parameter_resolver::resolve_parameters(
                &enriched,
                &context.parameter_definitions,
                &context.parameter_overrides,
            )?
        };

        let calculated = calculator::apply_dma_peclet(&resolved)?;
        let with_quality = quality_filters::apply_quality_filters(&calculated, Utc::now())?;

        Ok(PipelineBatchOutput {
            dataframe: with_quality,
            skipped_chunks,
        })
    }
}

#[cfg(feature = "runtime")]
fn json_value_to_map(value: Option<Value>) -> HashMap<String, Value> {
    match value {
        Some(Value::Object(map)) => map.into_iter().collect(),
        _ => HashMap::new(),
    }
}

#[cfg(feature = "runtime")]
fn datetime_to_utc_micros(dt: DateTime<Utc>) -> i64 {
    dt.timestamp_micros()
}

#[cfg(feature = "runtime")]
fn scientific_name_from_latin(value: Option<&Value>) -> Option<String> {
    value.and_then(|val| match val {
        Value::String(text) => normalize_scientific_name(text),
        Value::Object(map) => {
            const CANDIDATE_KEYS: [&str; 5] = ["binomial", "scientific", "value", "name", "text"];
            for key in CANDIDATE_KEYS {
                if let Some(Value::String(text)) = map.get(key) {
                    if let Some(normalized) = normalize_scientific_name(text) {
                        return Some(normalized);
                    }
                }
            }

            if let (Some(Value::String(genus)), Some(Value::String(species))) =
                (map.get("genus"), map.get("species"))
            {
                let mut base = format!("{genus} {species}");
                if let Some(Value::String(subspecies)) = map.get("subspecies") {
                    base.push(' ');
                    base.push_str(subspecies);
                }
                if let Some(normalized) = normalize_scientific_name(&base) {
                    return Some(normalized);
                }
            }

            for value in map.values() {
                if let Value::String(text) = value {
                    if let Some(normalized) = normalize_scientific_name(text) {
                        return Some(normalized);
                    }
                }
            }

            None
        }
        Value::Array(items) => {
            for item in items {
                if let Some(normalized) = scientific_name_from_latin(Some(item)) {
                    return Some(normalized);
                }
            }
            None
        }
        _ => None,
    })
}

#[cfg(feature = "runtime")]
fn normalize_scientific_name(raw: &str) -> Option<String> {
    let tokens: Vec<&str> = raw.split_whitespace().filter(|s| !s.is_empty()).collect();
    if tokens.is_empty() {
        return None;
    }

    let genus = tokens[0];
    if genus.is_empty() {
        return None;
    }

    let mut chars = genus.chars();
    let first = chars.next()?;
    let mut normalized = String::new();
    for c in first.to_uppercase() {
        normalized.push(c);
    }
    normalized.push_str(&chars.as_str().to_ascii_lowercase());

    let mut formatted_tail: Vec<String> = Vec::new();
    for token in tokens.iter().skip(1) {
        if token.is_empty() {
            continue;
        }
        formatted_tail.push(token.to_ascii_lowercase());
    }

    if !formatted_tail.is_empty() {
        normalized.push(' ');
        normalized.push_str(&formatted_tail.join(" "));
    }

    Some(normalized)
}
