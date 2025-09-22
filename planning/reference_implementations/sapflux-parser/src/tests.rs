use std::fs;
use std::path::PathBuf;

use crate::errors::ParserError;
use crate::formats::{Cr300TableParser, LoggerColumns, SapFlowAllParser, build_logger_dataframe};
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

    assert!(inner.df.column("alpha").is_ok());
    assert_eq!(inner.df.height(), parsed.logger.df.height());
    assert!(outer.df.column("alpha").is_ok());
}

#[test]
fn parses_cr300_table_file() {
    let content = fixture("CR300Series_402_Table2.dat");
    let parsed = parse_sapflow_file(&content).expect("CR300 table parse failed");

    assert!(
        parsed
            .file_metadata
            .table_name
            .to_ascii_lowercase()
            .starts_with("table")
    );
    assert_eq!(parsed.logger.sensors.len(), 1);

    let sensor = &parsed.logger.sensors[0];
    let sensor_df = sensor.sensor_df.as_ref().expect("missing sensor dataframe");
    assert!(sensor_df.column("total_sap_flow_lph").is_ok());

    let outer = sensor
        .thermistor_pairs
        .iter()
        .find(|pair| pair.depth == ThermistorDepth::Outer)
        .expect("missing outer pair");
    assert!(outer.df.column("sap_flux_density_cmh").is_ok());
    assert_eq!(outer.df.height(), parsed.logger.df.height());
    assert!(
        outer
            .df
            .column("sap_flux_density_cmh")
            .unwrap()
            .null_count()
            > 0
    );

    let logger_id_col = parsed
        .logger
        .df
        .column("logger_id")
        .expect("logger_id column missing");
    assert_eq!(logger_id_col.str().unwrap().get(0), Some("402"));
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
