use anyhow::Result;
use chrono::{NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use sapflux_core::{
    flatten::flatten_parsed_files,
    metadata_enricher::DeploymentRow,
    pipelines::{all_pipelines, ExecutionContext},
    timestamp_fixer::{DeploymentMetadata, SiteMetadata},
};
use sapflux_parser::parse_sapflow_file;
use std::collections::{HashMap, HashSet};
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

    let logger_series = flattened.column("logger_id")?.str()?;
    let logger_id = logger_series
        .get(0)
        .expect("flattened data should contain logger_id");

    let site_id = Uuid::new_v4();
    let timezone: Tz = "America/New_York".parse().unwrap();
    let start_local = NaiveDateTime::parse_from_str("2025-07-28 00:00:00", "%Y-%m-%d %H:%M:%S")?;

    let deployments = vec![DeploymentMetadata {
        datalogger_id: logger_id.to_string(),
        site_id,
        start_timestamp_local: start_local,
        end_timestamp_local: None,
    }];

    let timestamp_sites = vec![SiteMetadata { site_id, timezone }];

    let sdi_series = flattened.column("sdi12_address")?.str()?;
    let mut unique_addresses = sdi_series
        .into_iter()
        .flatten()
        .collect::<std::collections::HashSet<_>>();
    // Reuse an address value from the iterator after collect.
    let mut enrichment_deployments = Vec::with_capacity(unique_addresses.len());

    let project_id = Uuid::new_v4();
    let plant_id = Uuid::new_v4();
    let species_id = Uuid::new_v4();
    let stem_id = Uuid::new_v4();
    let project_code = "PRJ";
    let site_code = "SITE";
    let plant_code = "PLANT";
    let stem_code_str = "STEM1";
    let species_code = "SPEC";
    let zone_name = Some("Zone A".to_string());
    let plot_name = Some("Plot 1".to_string());
    let species_scientific_name = Some("Specia example".to_string());

    let start_utc = timezone
        .from_local_datetime(&start_local)
        .single()
        .expect("unambiguous start timestamp")
        .with_timezone(&Utc);

    for address in unique_addresses.drain() {
        enrichment_deployments.push(DeploymentRow {
            deployment_id: Uuid::new_v4(),
            datalogger_id: logger_id.to_string(),
            sdi_address: address.to_string(),
            project_id,
            project_code: Some(project_code.to_string()),
            project_name: Some("Project Name".to_string()),
            site_id,
            site_code: Some(site_code.to_string()),
            site_name: Some("Site Name".to_string()),
            zone_id: None,
            zone_name: zone_name.clone(),
            plot_id: None,
            plot_name: plot_name.clone(),
            plant_id: Some(plant_id),
            plant_code: Some(plant_code.to_string()),
            species_id: Some(species_id),
            species_code: Some(species_code.to_string()),
            species_scientific_name: species_scientific_name.clone(),
            stem_id,
            stem_code: Some(stem_code_str.to_string()),
            start_timestamp_utc: start_utc.timestamp_micros(),
            end_timestamp_utc: None,
            installation_metadata: Default::default(),
        });
    }

    let mut context = ExecutionContext::default();
    context.timestamp_sites = timestamp_sites;
    context.timestamp_deployments = deployments;
    context.enrichment_deployments = enrichment_deployments;
    context.datalogger_aliases = Vec::new();

    let batch: [&dyn sapflux_core::parsers::ParsedData; 2] = [&parsed_a, &parsed_b];

    let pipeline = all_pipelines()
        .iter()
        .find(|p| p.code_identifier() == "standard_v1_dst_fix")
        .expect("standard pipeline registered");

    let output = pipeline.run_batch(&context, &batch)?;
    assert!(output.skipped_chunks.is_empty());
    let df = output.dataframe;

    let signature_series = df.column("file_set_signature")?.str()?;
    let expected = {
        let mut hashes = vec!["hash_a", "hash_b"];
        hashes.sort();
        hashes.join("+")
    };

    let unique_signatures: std::collections::HashSet<_> =
        signature_series.into_iter().filter_map(|opt| opt).collect();

    assert_eq!(unique_signatures.len(), 1);
    assert_eq!(unique_signatures.iter().next().unwrap(), &expected);

    let ts_utc = df.column("timestamp_utc")?.datetime()?;
    assert_eq!(ts_utc.null_count(), 0);

    assert!(df.column("calculation_method_used").is_ok());
    assert!(df.column("sap_flux_density_j_dma_cm_hr").is_ok());
    assert!(df.column("quality").is_ok());

    assert!(df.height() > 0);

    let records = df.column("record")?.i64()?;
    let loggers = df.column("logger_id")?.str()?;
    let depths = df.column("thermistor_depth")?.str()?;
    let addresses = df.column("sdi12_address")?.str()?;
    let signatures = df.column("file_set_signature")?.str()?;
    let mut unique_pairs: HashSet<(String, i64, String, String, String)> = HashSet::new();
    for idx in 0..df.height() {
        let logger = loggers
            .get(idx)
            .expect("logger_id should be present in pipeline output");
        let record = records
            .get(idx)
            .expect("record should be present in pipeline output");
        let depth = depths
            .get(idx)
            .expect("thermistor_depth should be present in pipeline output");
        let address = addresses
            .get(idx)
            .expect("sdi12_address should be present in pipeline output");
        let signature = signatures
            .get(idx)
            .expect("file_set_signature should be present in pipeline output");
        unique_pairs.insert((
            logger.to_string(),
            record,
            depth.to_string(),
            address.to_string(),
            signature.to_string(),
        ));
    }
    assert_eq!(unique_pairs.len() * 2, df.height());

    let mut duplicate_counts: HashMap<(String, i64, String, String), usize> = HashMap::new();
    for idx in 0..df.height() {
        let key = (
            loggers.get(idx).expect("logger_id").to_string(),
            records.get(idx).expect("record"),
            depths.get(idx).expect("thermistor_depth").to_string(),
            addresses.get(idx).expect("sdi12_address").to_string(),
        );
        *duplicate_counts.entry(key).or_default() += 1;
    }

    assert!(duplicate_counts.values().all(|&count| count == 2));

    Ok(())
}
