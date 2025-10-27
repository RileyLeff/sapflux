use chrono::NaiveDateTime;
use chrono_tz::America::New_York;
use polars::lazy::dsl::col;
use polars::prelude::*;
use sapflux_core::timestamp_fixer::{self, DeploymentMetadata, SiteMetadata};
use uuid::Uuid;

fn parse_naive(ts: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S").expect("parse timestamp")
}

fn naive_to_micros(dt: NaiveDateTime) -> i64 {
    let dt_utc = dt.and_utc();
    dt_utc.timestamp() * 1_000_000 + i64::from(dt_utc.timestamp_subsec_nanos() / 1_000)
}

fn make_observations(times: &[NaiveDateTime], file_hashes: &[&str]) -> DataFrame {
    let logger_ids: Vec<&str> = vec!["420"; times.len()];
    let records: Vec<i64> = (0..times.len()).map(|idx| idx as i64).collect();
    let timestamps: Vec<i64> = times.iter().copied().map(naive_to_micros).collect();
    let file_hash_vec: Vec<String> = file_hashes.iter().map(|s| (*s).to_string()).collect();

    df![
        "logger_id" => logger_ids,
        "record" => records,
        "timestamp_local" => timestamps,
        "file_hash" => file_hash_vec,
    ]
    .expect("df")
    .lazy()
    .with_column(
        col("timestamp_local")
            .cast(DataType::Datetime(TimeUnit::Microseconds, None))
            .alias("timestamp"),
    )
    .select([
        col("logger_id"),
        col("record"),
        col("timestamp"),
        col("file_hash"),
    ])
    .collect()
    .expect("collect")
}

#[test]
fn timestamp_fixer_converts_to_utc() {
    let times = [
        parse_naive("2024-07-01 12:00:00"),
        parse_naive("2024-07-01 13:00:00"),
    ];
    let df = make_observations(&times, &["fileA", "fileB"]);

    let site_id = Uuid::new_v4();
    let sites = vec![SiteMetadata {
        site_id,
        timezone: New_York,
    }];

    let deployments = vec![DeploymentMetadata {
        datalogger_id: "420".to_string(),
        site_id,
        start_timestamp_local: parse_naive("2024-01-01 00:00:00"),
        end_timestamp_local: None,
    }];

    let result =
        timestamp_fixer::correct_timestamps(&df, &sites, &deployments).expect("fix timestamps");
    assert!(result.skipped_chunks.is_empty());
    let corrected = result.dataframe;

    let timestamp_utc = corrected
        .column("timestamp_utc")
        .expect("timestamp_utc column")
        .datetime()
        .expect("datetime");

    let first = timestamp_utc.get(0).expect("first row");
    let first_naive = parse_naive("2024-07-01 16:00:00");
    assert_eq!(naive_to_micros(first_naive), first);

    let raw_local = corrected
        .column("raw_local_timestamp_often_wrong")
        .expect("raw timestamp column")
        .datetime()
        .expect("datetime");
    let raw_first = raw_local.get(0).expect("raw value");
    assert_eq!(naive_to_micros(times[0]), raw_first);
}

#[test]
fn timestamp_fixer_handles_dst_ambiguous_time() {
    // Ambiguous local time during fall DST transition in America/New_York.
    let times = [parse_naive("2024-11-03 01:30:00")];
    let df = make_observations(&times, &["fileA"]);

    let site_id = Uuid::new_v4();
    let sites = vec![SiteMetadata {
        site_id,
        timezone: New_York,
    }];

    let deployments = vec![DeploymentMetadata {
        datalogger_id: "420".to_string(),
        site_id,
        start_timestamp_local: parse_naive("2024-01-01 00:00:00"),
        end_timestamp_local: None,
    }];

    let result =
        timestamp_fixer::correct_timestamps(&df, &sites, &deployments).expect("fix timestamps");
    assert!(result.skipped_chunks.is_empty());
    let corrected = result.dataframe;

    let ts_utc = corrected
        .column("timestamp_utc")
        .expect("timestamp_utc column")
        .datetime()
        .expect("datetime");

    let utc_value = ts_utc.get(0).expect("value");
    let expected = parse_naive("2024-11-03 05:30:00");
    assert_eq!(naive_to_micros(expected), utc_value);
}

#[test]
fn timestamp_fixer_skips_chunks_without_deployment() {
    let times = [parse_naive("2024-07-01 12:00:00")];
    let df = make_observations(&times, &["fileA"]);

    let site_id = Uuid::new_v4();
    let sites = vec![SiteMetadata {
        site_id,
        timezone: New_York,
    }];

    let deployments: Vec<DeploymentMetadata> = Vec::new();

    let result = timestamp_fixer::correct_timestamps(&df, &sites, &deployments)
        .expect("timestamp fixer should not error");

    assert!(result.dataframe.is_empty());
    assert_eq!(result.skipped_chunks.len(), 1);
    let skipped = &result.skipped_chunks[0];
    assert_eq!(skipped.row_count, 1);
    assert!(matches!(
        skipped.reason,
        timestamp_fixer::SkippedChunkReason::NoActiveDeployment
    ));
}
