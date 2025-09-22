use std::collections::HashMap;

use polars::prelude::*;
use sapflux_core::parameter_resolver::{
    self, ParameterDefinition, ParameterKind, ParameterOverride,
};
use serde_json::json;
use uuid::Uuid;

struct ObservationFixtures {
    df: DataFrame,
    deployment_ids: Vec<Uuid>,
    stem_ids: Vec<Uuid>,
    _site_id: Uuid,
}

fn make_observations() -> ObservationFixtures {
    let deployment_a = Uuid::new_v4();
    let deployment_b = Uuid::new_v4();
    let stem_a = Uuid::new_v4();
    let stem_b = Uuid::new_v4();
    let site_id = Uuid::new_v4();

    let df = df![
        "deployment_id" => &[deployment_a.to_string(), deployment_b.to_string()],
        "stem_id" => &[stem_a.to_string(), stem_b.to_string()],
        "site_id" => &[site_id.to_string(), site_id.to_string()],
    ]
    .expect("df");

    ObservationFixtures {
        df,
        deployment_ids: vec![deployment_a, deployment_b],
        stem_ids: vec![stem_a, stem_b],
        _site_id: site_id,
    }
}

#[test]
fn parameter_resolver_prefers_more_specific_override() {
    let fixtures = make_observations();
    let df = fixtures.df.clone();

    let defs = vec![ParameterDefinition {
        code: "parameter_wood_density_kg_m3",
        kind: ParameterKind::Float,
        default_value: json!(500.0),
    }];

    let overrides = vec![
        ParameterOverride {
            code: "parameter_wood_density_kg_m3".to_string(),
            value: json!(600.0),
            site_id: None,
            species_id: None,
            zone_id: None,
            plot_id: None,
            plant_id: None,
            stem_id: Some(fixtures.stem_ids[0]),
            deployment_id: None,
        },
        ParameterOverride {
            code: "parameter_wood_density_kg_m3".to_string(),
            value: json!(650.0),
            site_id: None,
            species_id: None,
            zone_id: None,
            plot_id: None,
            plant_id: None,
            stem_id: None,
            deployment_id: Some(fixtures.deployment_ids[1]),
        },
    ];

    let resolved = parameter_resolver::resolve_parameters(&df, &defs, &overrides).expect("resolve");

    let values = resolved
        .column("parameter_wood_density_kg_m3")
        .expect("values")
        .f64()
        .unwrap();

    let sources = resolved
        .column("parameter_source_parameter_wood_density_kg_m3")
        .expect("sources")
        .str()
        .unwrap();

    assert_eq!(values.get(0), Some(600.0));
    assert_eq!(sources.get(0), Some("stem_override"));
    assert_eq!(values.get(1), Some(650.0));
    assert_eq!(sources.get(1), Some("deployment_override"));
}

#[test]
fn parameter_resolver_falls_back_to_default() {
    let fixtures = make_observations();
    let df = fixtures.df.clone();

    let defs = vec![ParameterDefinition {
        code: "parameter_heat_pulse_duration_s",
        kind: ParameterKind::Float,
        default_value: json!(3.0),
    }];

    let overrides: Vec<ParameterOverride> = Vec::new();

    let resolved = parameter_resolver::resolve_parameters(&df, &defs, &overrides).expect("resolve");

    let values = resolved
        .column("parameter_heat_pulse_duration_s")
        .expect("values")
        .f64()
        .unwrap();
    let sources = resolved
        .column("parameter_source_parameter_heat_pulse_duration_s")
        .expect("sources")
        .str()
        .unwrap();

    assert_eq!(values.get(0), Some(3.0));
    assert_eq!(values.get(1), Some(3.0));
    assert_eq!(sources.get(0), Some("default"));
}

#[test]
fn canonical_definitions_include_quality_thresholds() {
    let defs = parameter_resolver::canonical_parameter_definitions();
    let lookup: HashMap<&str, &ParameterDefinition> =
        defs.iter().map(|def| (def.code, def)).collect();

    let future_lead = lookup
        .get("quality_future_lead_minutes")
        .expect("quality_future_lead_minutes present");
    assert_eq!(future_lead.kind, ParameterKind::Float);
    assert_eq!(future_lead.default_value.as_f64(), Some(5.0));
}
