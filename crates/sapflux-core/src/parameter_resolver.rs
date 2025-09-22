use std::collections::HashMap;

use polars::prelude::*;
use serde_json::{json, Value};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum ParameterResolverError {
    #[error("polars operation failed: {0}")]
    Polars(#[from] PolarsError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParameterKind {
    Float,
    Integer,
    Boolean,
    String,
}

#[derive(Debug, Clone)]
pub struct ParameterDefinition {
    pub code: &'static str,
    pub kind: ParameterKind,
    pub default_value: Value,
}

pub fn canonical_parameter_definitions() -> Vec<ParameterDefinition> {
    use ParameterKind::Float;

    vec![
        ParameterDefinition {
            code: "parameter_thermal_diffusivity_k_cm2_s",
            kind: Float,
            default_value: json!(0.002409611f64),
        },
        ParameterDefinition {
            code: "parameter_probe_distance_downstream_cm",
            kind: Float,
            default_value: json!(0.6f64),
        },
        ParameterDefinition {
            code: "parameter_probe_distance_upstream_cm",
            kind: Float,
            default_value: json!(0.6f64),
        },
        ParameterDefinition {
            code: "parameter_heat_pulse_duration_s",
            kind: Float,
            default_value: json!(3.0f64),
        },
        ParameterDefinition {
            code: "parameter_wound_correction_a",
            kind: Float,
            default_value: json!(1.8905f64),
        },
        ParameterDefinition {
            code: "parameter_wound_correction_b",
            kind: Float,
            default_value: json!(0.0f64),
        },
        ParameterDefinition {
            code: "parameter_wound_correction_c",
            kind: Float,
            default_value: json!(0.0f64),
        },
        ParameterDefinition {
            code: "parameter_wood_density_kg_m3",
            kind: Float,
            default_value: json!(500.0f64),
        },
        ParameterDefinition {
            code: "parameter_wood_specific_heat_j_kg_c",
            kind: Float,
            default_value: json!(1000.0f64),
        },
        ParameterDefinition {
            code: "parameter_water_content_g_g",
            kind: Float,
            default_value: json!(1.0f64),
        },
        ParameterDefinition {
            code: "parameter_water_specific_heat_j_kg_c",
            kind: Float,
            default_value: json!(4182.0f64),
        },
        ParameterDefinition {
            code: "parameter_water_density_kg_m3",
            kind: Float,
            default_value: json!(1000.0f64),
        },
        ParameterDefinition {
            code: "quality_max_flux_cm_hr",
            kind: Float,
            default_value: json!(40.0f64),
        },
        ParameterDefinition {
            code: "quality_min_flux_cm_hr",
            kind: Float,
            default_value: json!(-15.0f64),
        },
        ParameterDefinition {
            code: "quality_gap_years",
            kind: Float,
            default_value: json!(2.0f64),
        },
        ParameterDefinition {
            code: "quality_deployment_start_grace_minutes",
            kind: Float,
            default_value: json!(0.0f64),
        },
        ParameterDefinition {
            code: "quality_deployment_end_grace_minutes",
            kind: Float,
            default_value: json!(0.0f64),
        },
        ParameterDefinition {
            code: "quality_future_lead_minutes",
            kind: Float,
            default_value: json!(5.0f64),
        },
    ]
}

#[derive(Debug, Clone)]
pub struct ParameterOverride {
    pub code: String,
    pub value: Value,
    pub site_id: Option<Uuid>,
    pub species_id: Option<Uuid>,
    pub zone_id: Option<Uuid>,
    pub plot_id: Option<Uuid>,
    pub plant_id: Option<Uuid>,
    pub stem_id: Option<Uuid>,
    pub deployment_id: Option<Uuid>,
}

pub fn resolve_parameters(
    observations: &DataFrame,
    definitions: &[ParameterDefinition],
    overrides: &[ParameterOverride],
) -> Result<DataFrame, ParameterResolverError> {
    if observations.is_empty() {
        return Ok(observations.clone());
    }

    let context = ObservationContext::from_dataframe(observations)?;
    let mut grouped_overrides: HashMap<&str, Vec<&ParameterOverride>> = HashMap::new();
    for override_row in overrides {
        grouped_overrides
            .entry(override_row.code.as_str())
            .or_default()
            .push(override_row);
    }
    for override_list in grouped_overrides.values_mut() {
        override_list.sort_by_key(|o| override_precedence(o));
    }

    let mut enriched = observations.clone();

    for definition in definitions {
        let override_list = grouped_overrides
            .get(definition.code)
            .cloned()
            .unwrap_or_default();
        resolve_single_parameter(definition, override_list, &context, &mut enriched)?;
    }

    Ok(enriched)
}

struct ObservationContext {
    height: usize,
    site_ids: Vec<Option<Uuid>>,
    species_ids: Vec<Option<Uuid>>,
    zone_ids: Vec<Option<Uuid>>,
    plot_ids: Vec<Option<Uuid>>,
    plant_ids: Vec<Option<Uuid>>,
    stem_ids: Vec<Option<Uuid>>,
    deployment_ids: Vec<Option<Uuid>>,
}

impl ObservationContext {
    fn from_dataframe(df: &DataFrame) -> Result<Self, ParameterResolverError> {
        let height = df.height();
        Ok(Self {
            height,
            site_ids: extract_uuid_column(df, "site_id")?,
            species_ids: extract_uuid_column(df, "species_id")?,
            zone_ids: extract_uuid_column(df, "zone_id")?,
            plot_ids: extract_uuid_column(df, "plot_id")?,
            plant_ids: extract_uuid_column(df, "plant_id")?,
            stem_ids: extract_uuid_column(df, "stem_id")?,
            deployment_ids: extract_uuid_column(df, "deployment_id")?,
        })
    }
}

fn extract_uuid_column(
    df: &DataFrame,
    name: &str,
) -> Result<Vec<Option<Uuid>>, ParameterResolverError> {
    match df.column(name) {
        Ok(column) => {
            let str_col = column.as_materialized_series().str()?;
            let mut values = Vec::with_capacity(df.height());
            for idx in 0..df.height() {
                let parsed = str_col.get(idx).and_then(|s| Uuid::parse_str(s).ok());
                values.push(parsed);
            }
            Ok(values)
        }
        Err(_) => Ok(vec![None; df.height()]),
    }
}

fn resolve_single_parameter(
    definition: &ParameterDefinition,
    overrides: Vec<&ParameterOverride>,
    context: &ObservationContext,
    df: &mut DataFrame,
) -> Result<(), ParameterResolverError> {
    let mut provenance = Vec::with_capacity(context.height);

    match definition.kind {
        ParameterKind::Float => {
            let mut values = Vec::with_capacity(context.height);
            for idx in 0..context.height {
                let (value, source) = resolve_value(definition, &overrides, context, idx);
                values.push(value.as_ref().and_then(value_to_f64));
                provenance.push(source);
            }
            df.with_column(Series::new(definition.code.into(), values))?;
        }
        ParameterKind::Integer => {
            let mut values = Vec::with_capacity(context.height);
            for idx in 0..context.height {
                let (value, source) = resolve_value(definition, &overrides, context, idx);
                values.push(value.as_ref().and_then(value_to_i64));
                provenance.push(source);
            }
            df.with_column(Series::new(definition.code.into(), values))?;
        }
        ParameterKind::Boolean => {
            let mut values = Vec::with_capacity(context.height);
            for idx in 0..context.height {
                let (value, source) = resolve_value(definition, &overrides, context, idx);
                values.push(value.as_ref().and_then(value_to_bool));
                provenance.push(source);
            }
            df.with_column(Series::new(definition.code.into(), values))?;
        }
        ParameterKind::String => {
            let mut values = Vec::with_capacity(context.height);
            for idx in 0..context.height {
                let (value, source) = resolve_value(definition, &overrides, context, idx);
                values.push(value.map(value_to_string));
                provenance.push(source);
            }
            df.with_column(Series::new(definition.code.into(), values))?;
        }
    }

    let provenance_col = format!("parameter_source_{}", definition.code);
    df.with_column(Series::new(provenance_col.into(), provenance))?;

    Ok(())
}

fn resolve_value(
    definition: &ParameterDefinition,
    overrides: &[&ParameterOverride],
    context: &ObservationContext,
    idx: usize,
) -> (Option<Value>, String) {
    for override_row in overrides {
        if override_matches(override_row, context, idx) {
            let label = provenance_label(override_row);
            return (Some(override_row.value.clone()), label.to_string());
        }
    }
    (
        Some(definition.default_value.clone()),
        "default".to_string(),
    )
}

fn override_matches(
    override_row: &ParameterOverride,
    context: &ObservationContext,
    idx: usize,
) -> bool {
    let matches_site = match override_row.site_id {
        Some(ref id) => context.site_ids[idx].as_ref() == Some(id),
        None => true,
    };
    let matches_species = match override_row.species_id {
        Some(ref id) => context.species_ids[idx].as_ref() == Some(id),
        None => true,
    };
    let matches_zone = match override_row.zone_id {
        Some(ref id) => context.zone_ids[idx].as_ref() == Some(id),
        None => true,
    };
    let matches_plot = match override_row.plot_id {
        Some(ref id) => context.plot_ids[idx].as_ref() == Some(id),
        None => true,
    };
    let matches_plant = match override_row.plant_id {
        Some(ref id) => context.plant_ids[idx].as_ref() == Some(id),
        None => true,
    };
    let matches_stem = match override_row.stem_id {
        Some(ref id) => context.stem_ids[idx].as_ref() == Some(id),
        None => true,
    };
    let matches_deployment = match override_row.deployment_id {
        Some(ref id) => context.deployment_ids[idx].as_ref() == Some(id),
        None => true,
    };

    matches_site
        && matches_species
        && matches_zone
        && matches_plot
        && matches_plant
        && matches_stem
        && matches_deployment
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn value_to_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(b) => Some(*b),
        Value::String(s) => match s.to_ascii_lowercase().as_str() {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn value_to_string(value: Value) -> String {
    match value {
        Value::String(s) => s,
        other => other.to_string(),
    }
}

fn override_precedence(override_row: &ParameterOverride) -> u8 {
    if override_row.deployment_id.is_some() {
        0
    } else if override_row.stem_id.is_some() {
        1
    } else if override_row.plant_id.is_some() {
        2
    } else if override_row.plot_id.is_some() {
        3
    } else if override_row.zone_id.is_some() {
        4
    } else if override_row.species_id.is_some() {
        5
    } else if override_row.site_id.is_some() {
        6
    } else {
        7
    }
}

fn provenance_label(override_row: &ParameterOverride) -> &'static str {
    if override_row.deployment_id.is_some() {
        "deployment_override"
    } else if override_row.stem_id.is_some() {
        "stem_override"
    } else if override_row.plant_id.is_some() {
        "plant_override"
    } else if override_row.plot_id.is_some() {
        "plot_override"
    } else if override_row.zone_id.is_some() {
        "zone_override"
    } else if override_row.species_id.is_some() {
        "species_override"
    } else if override_row.site_id.is_some() {
        "site_override"
    } else {
        "default"
    }
}
