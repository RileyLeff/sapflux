use std::fs;
use std::path::PathBuf;

use crate::errors::ParserError;
use crate::formats::schema::{required_thermistor_metrics, LOGGER_COLUMNS};
use crate::formats::{build_logger_dataframe, Cr300TableParser, LoggerColumns, SapFlowAllParser};
use crate::model::{ParsedFileData, ThermistorDepth};
use crate::parse_sapflow_file;
use crate::registry::SapflowParser;
use csv::ReaderBuilder;

fn fixture(path: &str) -> String {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let full_path = base.join("tests/data").join(path);
    fs::read_to_string(&full_path)
        .unwrap_or_else(|err| panic!("failed to read fixture {}: {}", full_path.display(), err))
}

#[test]
fn parses_sapflow_all_multi_sensor() {
    let content = fixture("CR300Series_420_SapFlowAll.dat");
    let parsed = parse_sapflow_file(&content).expect("SapFlowAll parse failed");

    assert_eq!(parsed.file_metadata.table_name, "SapFlowAll");
    assert_eq!(parsed.logger.sensors.len(), 2);
    assert_eq!(parsed.logger.df.get_column_names(), LOGGER_COLUMNS);

    let sensor = &parsed.logger.sensors[0];
    assert!(sensor.sensor_df.is_none());

    let inner = sensor
        .thermistor_pairs
        .iter()
        .find(|pair| pair.depth == ThermistorDepth::Inner)
        .expect("missing inner thermistor data");
    let outer = sensor
        .thermistor_pairs
        .iter()
        .find(|pair| pair.depth == ThermistorDepth::Outer)
        .expect("missing outer thermistor data");
    let expected_columns: Vec<&str> = required_thermistor_metrics()
        .iter()
        .map(|metric| metric.canonical_name())
        .collect();

    assert_eq!(inner.df.get_column_names(), expected_columns);
    assert_eq!(outer.df.get_column_names(), expected_columns);
    assert!(inner.df.column("sap_flux_density_cmh").is_err());
    assert!(outer.df.column("sap_flux_density_cmh").is_err());
    assert_eq!(inner.df.height(), parsed.logger.df.height());
}

#[test]
fn sapflow_all_parses_three_sensor_file() {
    let content = fixture("CR300Series_423_SapFlowAll.dat");
    let parsed = parse_sapflow_file(&content).expect("three sensor SapFlowAll parse failed");

    assert_eq!(parsed.logger.df.get_column_names(), LOGGER_COLUMNS);
    assert_eq!(parsed.logger.df.height(), 2);
    assert_eq!(parsed.logger.sensors.len(), 3);

    let expected_columns: Vec<&str> = required_thermistor_metrics()
        .iter()
        .map(|metric| metric.canonical_name())
        .collect();

    for sensor in &parsed.logger.sensors {
        assert!(sensor.sensor_df.is_none());
        assert_eq!(sensor.thermistor_pairs.len(), 2);
        for pair in &sensor.thermistor_pairs {
            assert_eq!(pair.df.get_column_names(), expected_columns);
            assert_eq!(pair.df.height(), parsed.logger.df.height());
        }
    }
}

#[test]
fn parses_cr300_table_file() {
    let content = fixture("CR300Series_402_Table2.dat");
    let parsed = parse_sapflow_file(&content).expect("CR300 table parse failed");

    assert!(parsed
        .file_metadata
        .table_name
        .to_ascii_lowercase()
        .starts_with("table"));
    assert_eq!(parsed.logger.sensors.len(), 1);
    assert_eq!(parsed.logger.df.get_column_names(), LOGGER_COLUMNS);

    let sensor = &parsed.logger.sensors[0];
    assert!(sensor.sensor_df.is_none());

    let outer = sensor
        .thermistor_pairs
        .iter()
        .find(|pair| pair.depth == ThermistorDepth::Outer)
        .expect("missing outer pair");
    let expected_columns: Vec<&str> = required_thermistor_metrics()
        .iter()
        .map(|metric| metric.canonical_name())
        .collect();
    assert_eq!(outer.df.get_column_names(), expected_columns);
    assert_eq!(outer.df.height(), parsed.logger.df.height());
    assert!(outer.df.column("sap_flux_density_cmh").is_err());

    let logger_id_col = parsed
        .logger
        .df
        .column("logger_id")
        .expect("logger_id column missing");
    assert_eq!(logger_id_col.str().unwrap().get(0), Some("402"));
}

#[test]
fn parses_cr300_legacy_table_file() {
    let content = fixture("CR300Series_502_Legacy.dat");
    let parsed = parse_sapflow_file(&content).expect("CR300 legacy parse failed");

    assert!(parsed
        .file_metadata
        .table_name
        .to_ascii_lowercase()
        .starts_with("table"));
    assert_eq!(parsed.logger.sensors.len(), 1);
    assert_eq!(parsed.logger.df.get_column_names(), LOGGER_COLUMNS);

    let sensor = &parsed.logger.sensors[0];
    assert!(sensor.sensor_df.is_none());

    let inner = sensor
        .thermistor_pairs
        .iter()
        .find(|pair| pair.depth == ThermistorDepth::Inner)
        .expect("missing inner thermistor data");
    let outer = sensor
        .thermistor_pairs
        .iter()
        .find(|pair| pair.depth == ThermistorDepth::Outer)
        .expect("missing outer thermistor data");
    let expected_columns: Vec<&str> = required_thermistor_metrics()
        .iter()
        .map(|metric| metric.canonical_name())
        .collect();

    assert_eq!(inner.df.get_column_names(), expected_columns);
    assert_eq!(outer.df.get_column_names(), expected_columns);
    assert!(inner.df.column("sap_flux_density_cmh").is_err());
    assert!(outer.df.column("sap_flux_density_cmh").is_err());

    let logger_id_col = parsed
        .logger
        .df
        .column("logger_id")
        .expect("logger_id column missing");
    assert_eq!(logger_id_col.str().unwrap().get(0), Some("502"));
}

#[test]
fn cr300_table_skips_rows_with_invalid_sdi() {
    let content = fixture("CR300Series_502_InvalidSdi.csv");
    let parsed = parse_sapflow_file(&content).expect("CR300 invalid SDI parse failed");

    assert_eq!(parsed.logger.df.height(), 4);

    let records = parsed
        .logger
        .df
        .column("record")
        .expect("record column missing")
        .i64()
        .expect("record column not integer")
        .into_no_null_iter()
        .collect::<Vec<_>>();
    assert_eq!(records, vec![0, 1, 3, 4]);
}

#[test]
fn parses_cr200_table_file() {
    let content = fixture("CR200Series_304_Table2.dat");
    let parsed = parse_sapflow_file(&content).expect("CR200 table parse failed");

    assert!(parsed
        .file_metadata
        .table_name
        .to_ascii_lowercase()
        .starts_with("table"));
    assert_eq!(parsed.logger.sensors.len(), 1);
    assert_eq!(parsed.logger.df.get_column_names(), LOGGER_COLUMNS);

    let panel_col = parsed
        .logger
        .df
        .column("panel_temperature_c")
        .expect("panel_temperature_c column missing");
    assert!(panel_col
        .f64()
        .unwrap()
        .into_iter()
        .all(|entry| entry.is_none()));

    let sensor = &parsed.logger.sensors[0];
    assert!(sensor.sensor_df.is_none());

    let inner = sensor
        .thermistor_pairs
        .iter()
        .find(|pair| pair.depth == ThermistorDepth::Inner)
        .expect("missing inner thermistor data");
    let outer = sensor
        .thermistor_pairs
        .iter()
        .find(|pair| pair.depth == ThermistorDepth::Outer)
        .expect("missing outer thermistor data");
    let expected_columns: Vec<&str> = required_thermistor_metrics()
        .iter()
        .map(|metric| metric.canonical_name())
        .collect();

    assert_eq!(inner.df.get_column_names(), expected_columns);
    assert_eq!(outer.df.get_column_names(), expected_columns);
    assert!(inner.df.column("sap_flux_density_cmh").is_err());
    assert!(outer.df.column("sap_flux_density_cmh").is_err());

    let logger_id_col = parsed
        .logger
        .df
        .column("logger_id")
        .expect("logger_id column missing");
    assert_eq!(logger_id_col.str().unwrap().get(0), Some("304"));
}

#[test]
fn parses_cr200_table1_truncated_units() {
    let content = fixture("CR200Series_304_Table1_Truncated.csv");
    let parsed = parse_sapflow_file(&content).expect("CR200 truncated header parse failed");

    assert_eq!(parsed.logger.df.get_column_names(), LOGGER_COLUMNS);

    let ids = parsed
        .logger
        .df
        .column("logger_id")
        .expect("logger_id column missing")
        .str()
        .expect("logger_id column not utf8");
    assert!(ids.into_iter().all(|value| value == Some("304")));
}

#[test]
fn cr200_logger_id_tolerates_sparse_values() {
    let content = fixture("CR200Series_305_Table2_NanId.csv");
    let parsed = parse_sapflow_file(&content).expect("CR200 sparse id parse failed");

    let ids = parsed
        .logger
        .df
        .column("logger_id")
        .expect("logger_id column missing")
        .str()
        .expect("logger_id column not utf8");
    assert!(ids.into_iter().all(|value| value == Some("305")));
}

#[test]
fn thermistor_schema_matches_between_formats() {
    let sap_content = fixture("CR300Series_420_SapFlowAll.dat");
    let sap_parsed = parse_sapflow_file(&sap_content).expect("SapFlowAll parse failed");

    let cr_content = fixture("CR300Series_402_Table2.dat");
    let cr_parsed = parse_sapflow_file(&cr_content).expect("CR300 parse failed");

    assert_eq!(sap_parsed.logger.df.get_column_names(), LOGGER_COLUMNS);
    assert_eq!(cr_parsed.logger.df.get_column_names(), LOGGER_COLUMNS);

    let expected_columns: Vec<&str> = required_thermistor_metrics()
        .iter()
        .map(|metric| metric.canonical_name())
        .collect();

    for depth in [ThermistorDepth::Inner, ThermistorDepth::Outer] {
        let sap_pair = sap_parsed
            .logger
            .sensors
            .iter()
            .flat_map(|sensor| sensor.thermistor_pairs.iter())
            .find(|pair| pair.depth == depth);
        let cr_pair = cr_parsed
            .logger
            .sensors
            .iter()
            .flat_map(|sensor| sensor.thermistor_pairs.iter())
            .find(|pair| pair.depth == depth);

        if let (Some(sap_pair), Some(cr_pair)) = (sap_pair, cr_pair) {
            assert_eq!(sap_pair.df.get_column_names(), expected_columns);
            assert_eq!(cr_pair.df.get_column_names(), expected_columns);
        }
    }
}

#[test]
fn archive_round_trip_preserves_data() {
    let content = fixture("CR300Series_420_SapFlowAll.dat");
    let parsed = parse_sapflow_file(&content).expect("initial parse failed");

    let archive = parsed.to_zip_archive().expect("zip serialization failed");
    let restored = ParsedFileData::from_zip_archive(&archive, parsed.raw_text.clone())
        .expect("zip roundtrip failed");

    assert!(parsed.logger.df.equals_missing(&restored.logger.df));
    assert_eq!(parsed.logger.sensors.len(), restored.logger.sensors.len());

    for (lhs, rhs) in parsed
        .logger
        .sensors
        .iter()
        .zip(restored.logger.sensors.iter())
    {
        assert_eq!(lhs.sdi12_address, rhs.sdi12_address);
        match (&lhs.sensor_df, &rhs.sensor_df) {
            (Some(left_df), Some(right_df)) => {
                assert!(left_df.equals_missing(right_df));
            }
            (None, None) => {}
            _ => panic!("sensor dataframe presence mismatch"),
        }
        assert_eq!(lhs.thermistor_pairs.len(), rhs.thermistor_pairs.len());
        for (lp, rp) in lhs.thermistor_pairs.iter().zip(rhs.thermistor_pairs.iter()) {
            assert_eq!(lp.depth, rp.depth);
            assert!(lp.df.equals_missing(&rp.df));
        }
    }
}

#[test]
fn sapflow_all_rejects_wrong_table_name() {
    let content = fixture("CR300Series_420_SapFlowAll.dat");
    let mutated = content.replacen("\"SapFlowAll\"", "\"SapFlowBogus\"", 1);

    let parser = SapFlowAllParser::default();
    let err = parser
        .parse(&mutated)
        .expect_err("parser should reject files with unexpected table names");

    match err {
        ParserError::FormatMismatch { reason, .. } => {
            assert!(reason.contains("SapFlow"), "unexpected reason: {reason}");
        }
        other => panic!("expected FormatMismatch error, got {other:?}"),
    }
}

#[test]
fn cr300_rejects_row_with_missing_columns() {
    let content = fixture("CR300Series_402_Table2.dat");
    let mut lines: Vec<String> = content.lines().map(|s| s.to_string()).collect();
    if let Some((prefix, _)) = lines[4].rsplit_once(',') {
        lines[4] = prefix.to_string();
    }
    let invalid_content = lines.join("\r\n") + "\r\n";

    let parser = Cr300TableParser::default();
    let err = parser
        .parse(&invalid_content)
        .expect_err("parser should flag data rows with missing columns");

    match err {
        ParserError::DataRow { .. } => {}
        other => panic!("expected DataRow error, got {other:?}"),
    }
}

#[test]
fn cr300_invalid_units_row_triggers_invalid_header() {
    let content = fixture("CR300Series_402_Table2.dat");
    let mut lines: Vec<String> = content.lines().map(|s| s.to_string()).collect();
    lines[2] = lines[2].replacen("literPerHour", "litersPerHour", 1);
    let invalid_content = lines.join("\r\n") + "\r\n";

    let parser = Cr300TableParser::default();
    let err = parser
        .parse(&invalid_content)
        .expect_err("parser should reject unexpected units row");

    match err {
        ParserError::InvalidHeader { row_index, .. } => assert_eq!(row_index, 3),
        other => panic!("expected InvalidHeader error, got {other:?}"),
    }
}

#[test]
fn sapflow_all_reports_csv_error() {
    let malformed = concat!(
        r#""TOA5","CR300Series_420","CR300","1740","CR300.Std.11.00","CPU:sapflux_2sensor_CR300_30min.cr300","60975","SapFlowAll""#,
        "\r\n",
        r#""TIMESTAMP","RECORD","Batt_volt","PTemp_C","S0_AlpOut","S0_AlpInn","S0_BetOut","S0_BetInn","S0_tMxTout","S0_tMxTinn","S0_TpDsOut","S0_dTDsOut","S0_TsDsOut","S0_TpUsOut","S0_dTUsOut","S0_TsUsOut","S0_TpDsInn","S0_dTDsInn","S0_TsDsInn","S0_TpUsInn","S0_dTUsInn","S0_TsUsInn","S0_tMxTUsO","S0_tMxTUsI","S1_AlpOut","S1_AlpInn","S1_BetOut","S1_BetInn","S1_tMxTout","S1_tMxTinn","S1_TpDsOut","S1_dTDsOut","S1_TsDsOut","S1_TpUsOut","S1_dTUsOut","S1_TsUsOut","S1_TpDsInn","S1_dTDsInn","S1_TsDsInn","S1_TpUsInn","S1_dTUsInn","S1_TsUsInn","S1_tMxTUsO","S1_tMxTUsI""#,
        "\r\n",
        r#""TS","RN","","","ratio","ratio","ratio","ratio","sec","sec","degC","degC","degC","degC","degC","degC","degC","degC","degC","degC","degC","degC","sec","sec","ratio","ratio","ratio","ratio","sec","sec","degC","degC","degC","degC","degC","degC","degC","degC","degC","degC","degC","degC","sec","sec""#,
        "\r\n",
        r#""","","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp","Smp""#,
        "\r\n",
        r#""2025-07-29 20:00:00",3420,13.03,33.1,0.04496,0.068,0.04836,0.06304"#,
        "\r\n"
    );
    let mut builder = ReaderBuilder::new();
    builder.has_headers(false);
    builder.flexible(false);
    let parser = SapFlowAllParser::default();
    let err = parser
        .parse_with_custom_builder(builder, malformed)
        .expect_err("parser should propagate CSV reader errors");

    match err {
        ParserError::Csv { .. } => {}
        other => panic!("expected Csv error, got {other:?}"),
    }
}

#[test]
fn build_logger_dataframe_detects_mismatched_lengths() {
    let mut columns = LoggerColumns::new(0);
    columns.timestamp.push(0);
    let err = build_logger_dataframe("TEST", columns).expect_err("expected validation failure");
    match err {
        ParserError::Validation { .. } => {}
        other => panic!("expected Validation error, got {other:?}"),
    }
}

#[test]
fn cr300_empty_data_triggers_empty_error() {
    let content = fixture("CR300Series_402_Table2.dat");
    let header_only = content.lines().take(4).collect::<Vec<_>>().join("\r\n") + "\r\n";

    let parser = Cr300TableParser::default();
    match parser.parse(&header_only) {
        Err(ParserError::EmptyData { .. }) => {}
        other => panic!("expected EmptyData error, got {other:?}"),
    }
}

#[test]
fn parse_unknown_format_returns_no_matching_parser() {
    let content = fixture("CR300Series_420_SapFlowAll.dat");
    let mutated = content.replacen("\"SapFlowAll\"", "\"MysteryTable\"", 1);

    match parse_sapflow_file(&mutated) {
        Err(ParserError::NoMatchingParser { attempts }) => {
            assert!(!attempts.is_empty());
        }
        other => panic!("expected NoMatchingParser error, got {other:?}"),
    }
}

#[test]
fn sapflow_all_derives_logger_id_from_header() {
    let content = fixture("CR300Series_420_SapFlowAll.dat");
    let parsed = parse_sapflow_file(&content).expect("SapFlowAll parse failed");

    let logger_id_col = parsed
        .logger
        .df
        .column("logger_id")
        .expect("logger_id column missing");

    let values = logger_id_col.str().unwrap();
    assert_eq!(values.get(0), Some("420"));
    assert!(values.into_iter().all(|opt| opt == Some("420")));
}

#[test]
fn non_sequential_records_are_rejected() {
    let content = fixture("CR300Series_402_Table2.dat");
    let mut lines: Vec<String> = content.lines().map(|s| s.to_string()).collect();
    if let Some(line) = lines.get_mut(5) {
        // Replace the record value "1" with "3" while leaving the rest of the row intact.
        *line = line.replacen("\",1,12.73", "\",3,12.73", 1);
    }
    let mutated = lines.join("\r\n") + "\r\n";

    match parse_sapflow_file(&mutated) {
        Err(ParserError::DataRow { message, .. }) => {
            assert!(message.contains("record column must increment"));
        }
        other => panic!("expected DataRow error, got {other:?}"),
    }
}
