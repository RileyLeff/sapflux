use std::collections::HashMap;

use polars::lazy::dsl::col;
use polars::prelude::*;
use serde_json::json;
use sapflux_core::metadata_enricher::{self, DeploymentRow};
use uuid::Uuid;

fn make_observations(logger: &str, address: &str, timestamp_micros: i64) -> DataFrame {
    df![
        "logger_id" => &[logger],
        "sdi12_address" => &[address],
        "timestamp_utc" => &[timestamp_micros],
    ]
    .expect("df")
    .lazy()
    .with_column(
        col("timestamp_utc")
            .cast(DataType::Datetime(TimeUnit::Microseconds, None)),
    )
    .collect()
    .expect("collect")
}

#[test]
fn enrichment_populates_deployment_and_metadata() {
    let timestamp = 1_720_137_600_i64 * 1_000_000; // 2024-07-05 00:00:00 UTC
    let df = make_observations("420", "0", timestamp);

    let deployment_id = Uuid::new_v4();
    let project_id = Uuid::new_v4();
    let site_id = Uuid::new_v4();
    let stem_id = Uuid::new_v4();

    let mut installation_metadata = HashMap::new();
    installation_metadata.insert("probe_azimuth".to_string(), json!(180));
    installation_metadata.insert("notes".to_string(), json!("Shaded"));

    let deployments = vec![DeploymentRow {
        deployment_id,
        datalogger_id: "420".to_string(),
        sdi_address: "0".to_string(),
        project_id,
        site_id,
        stem_id,
        start_timestamp_utc: timestamp - 1_000_000,
        end_timestamp_utc: Some(timestamp + 1_000_000),
        installation_metadata,
    }];

    let enriched = metadata_enricher::enrich_with_metadata(&df, &deployments)
        .expect("enrichment");

    let deployment_col = enriched
        .column("deployment_id")
        .expect("deployment_id")
        .str()
        .unwrap();
    let deployment_id_str = deployment_id.to_string();
    assert_eq!(deployment_col.get(0), Some(deployment_id_str.as_str()));

    let azimuth_col = enriched
        .column("probe_azimuth")
        .expect("probe_azimuth")
        .str()
        .unwrap();
    assert_eq!(azimuth_col.get(0), Some("180"));

    let notes_col = enriched
        .column("notes")
        .expect("notes")
        .str()
        .unwrap();
    assert_eq!(notes_col.get(0), Some("Shaded"));
}

#[test]
fn enrichment_handles_missing_deployment() {
    let timestamp = 1_720_137_600_i64 * 1_000_000;
    let df = make_observations("420", "0", timestamp);

    let deployments: Vec<DeploymentRow> = Vec::new();

    let enriched = metadata_enricher::enrich_with_metadata(&df, &deployments)
        .expect("enrichment");

    let deployment_col = enriched
        .column("deployment_id")
        .expect("deployment_id")
        .str()
        .unwrap();
    assert_eq!(deployment_col.get(0), None);
}
