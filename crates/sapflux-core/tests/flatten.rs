use std::fs;
use std::path::PathBuf;

use sapflux_core::flatten::flatten_parsed_files;
use sapflux_parser::parse_sapflow_file;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../sapflux-parser/tests/data")
        .join(name)
}

#[test]
fn flatten_creates_rows_for_each_thermistor_pair() {
    let content = fs::read_to_string(fixture_path("CR300Series_420_SapFlowAll.dat"))
        .expect("failed to read fixture");
    let parsed = parse_sapflow_file(&content).expect("parse failed");

    let df = flatten_parsed_files(&[&parsed]).expect("flatten failed");

    assert!(df.column("file_hash").is_ok(), "expected file_hash column");
    assert!(df.column("sdi12_address").is_ok(), "expected sdi12_address column");
    assert!(df.column("thermistor_depth").is_ok(), "expected depth column");

    // Expect two sensors * two depths for the first fixture (4 groups)
    let expected_pairs = parsed
        .logger
        .sensors
        .iter()
        .map(|sensor| sensor.thermistor_pairs.len())
        .sum::<usize>();

    assert_eq!(df.height(), parsed.logger.df.height() * expected_pairs);
}
