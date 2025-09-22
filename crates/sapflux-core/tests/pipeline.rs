use anyhow::Result;
use chrono::NaiveDateTime;
use chrono_tz::Tz;
use sapflux_core::{
    flatten::flatten_parsed_files,
    timestamp_fixer::{self, DeploymentMetadata, SiteMetadata},
};
use sapflux_parser::parse_sapflow_file;
use uuid::Uuid;

fn fixture(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../sapflux-parser/tests/data")
        .join(name);
    std::fs::read_to_string(path).expect("read fixture")
}

#[test]
fn timestamp_fixer_groups_records_by_file_signature() -> Result<()> {
    let mut parsed_a = parse_sapflow_file(&fixture("CR300Series_420_SapFlowAll.dat"))?;
    parsed_a.file_hash = "hash_a".into();
    let mut parsed_b = parse_sapflow_file(&fixture("CR300Series_420_SapFlowAll.dat"))?;
    parsed_b.file_hash = "hash_b".into();

    let flattened = flatten_parsed_files(&[&parsed_a, &parsed_b])?;
    assert!(!flattened.is_empty());

    let site_id = Uuid::new_v4();
    let deployments = vec![DeploymentMetadata {
        datalogger_id: "420".into(),
        site_id,
        start_timestamp_local: NaiveDateTime::parse_from_str(
            "2025-07-28 00:00:00",
            "%Y-%m-%d %H:%M:%S",
        )?,
        end_timestamp_local: None,
    }];

    let corrected = timestamp_fixer::correct_timestamps(
        &flattened,
        &[SiteMetadata {
            site_id,
            timezone: "America/New_York".parse::<Tz>().unwrap(),
        }],
        &deployments,
    )?;

    let signature_series = corrected.column("file_set_signature")?.str()?;
    let expected = {
        let mut hashes = vec!["hash_a", "hash_b"];
        hashes.sort();
        hashes.join("+")
    };

    let unique_signatures: std::collections::HashSet<_> =
        signature_series.into_iter().filter_map(|opt| opt).collect();

    assert_eq!(unique_signatures.len(), 1);
    assert_eq!(unique_signatures.iter().next().unwrap(), &expected);

    let ts_utc = corrected.column("timestamp_utc")?.datetime()?;
    assert_eq!(ts_utc.null_count(), 0);

    assert!(corrected.column("calculation_method_used").is_ok());
    assert!(corrected.column("sap_flux_density_j_dma_cm_hr").is_ok());
    assert!(corrected.column("quality").is_ok());

    Ok(())
}
