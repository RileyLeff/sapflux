use chrono::{Duration, TimeZone, Utc};
use polars::prelude::*;

use sapflux_core::quality_filters::apply_quality_filters;

#[test]
fn quality_filters_flag_expected_rules() -> PolarsResult<()> {
    let base = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let now = base + Duration::hours(1);

    let timestamp_values = vec![
        base + Duration::minutes(15), // good
        base - Duration::minutes(20), // before start
        base + Duration::hours(2),    // after end
        now + Duration::minutes(20),  // future
        base + Duration::days(800),   // record gap
        base + Duration::minutes(10), // high flux
        base + Duration::minutes(12), // low flux
        base - Duration::minutes(30), // before earliest start without deployment metadata
    ];

    let start_times = vec![
        Some(base - Duration::minutes(10)),
        Some(base),
        Some(base - Duration::minutes(10)),
        Some(base - Duration::minutes(10)),
        Some(base - Duration::minutes(10)),
        Some(base - Duration::minutes(10)),
        Some(base - Duration::minutes(10)),
        None,
    ];

    let end_times = vec![
        Some(base + Duration::minutes(60)),
        Some(base + Duration::minutes(60)),
        Some(base + Duration::minutes(30)),
        Some(base + Duration::hours(3)),
        Some(base + Duration::hours(3)),
        Some(base + Duration::hours(3)),
        Some(base + Duration::hours(3)),
        Some(base + Duration::hours(3)),
    ];

    let timestamp_series = Series::new(
        "timestamp_utc".into(),
        timestamp_values
            .iter()
            .map(|dt| dt.timestamp_micros())
            .collect::<Vec<_>>(),
    )
    .cast(&DataType::Datetime(
        TimeUnit::Microseconds,
        Some(polars::prelude::TimeZone::UTC),
    ))?;

    let datetime_dtype = timestamp_series.dtype().clone();

    let start_series = Series::new(
        "deployment_start_timestamp_utc".into(),
        start_times
            .iter()
            .map(|dt| dt.map(|value| value.timestamp_micros()))
            .collect::<Vec<_>>(),
    )
    .cast(&datetime_dtype)?;

    let end_series = Series::new(
        "deployment_end_timestamp_utc".into(),
        end_times
            .iter()
            .map(|dt| dt.map(|value| value.timestamp_micros()))
            .collect::<Vec<_>>(),
    )
    .cast(&datetime_dtype)?;

    let df = DataFrame::new(vec![
        timestamp_series.into(),
        start_series.into(),
        end_series.into(),
        Series::new(
            "quality_deployment_start_grace_minutes".into(),
            vec![0.0f64; timestamp_values.len()],
        )
        .into(),
        Series::new(
            "quality_deployment_end_grace_minutes".into(),
            vec![0.0f64; timestamp_values.len()],
        )
        .into(),
        Series::new(
            "quality_future_lead_minutes".into(),
            vec![5.0f64; timestamp_values.len()],
        )
        .into(),
        Series::new(
            "quality_gap_years".into(),
            vec![0.01f64; timestamp_values.len()],
        )
        .into(),
        Series::new(
            "quality_max_flux_cm_hr".into(),
            vec![20.0f64; timestamp_values.len()],
        )
        .into(),
        Series::new(
            "quality_min_flux_cm_hr".into(),
            vec![-10.0f64; timestamp_values.len()],
        )
        .into(),
        Series::new(
            "sap_flux_density_j_dma_cm_hr".into(),
            vec![5.0, 5.0, 5.0, 5.0, 5.0, 30.0, -20.0, 5.0],
        )
        .into(),
        Series::new(
            "record".into(),
            (1..=timestamp_values.len() as i64).collect::<Vec<_>>(),
        )
        .into(),
        Series::new("logger_id".into(), vec!["logger"; timestamp_values.len()]).into(),
        Series::new("sdi12_address".into(), vec!["0"; timestamp_values.len()]).into(),
    ])?;

    let result = apply_quality_filters(&df, now).unwrap();
    let quality = result.column("quality").unwrap().str().unwrap();
    let explanation = result.column("quality_explanation").unwrap().str().unwrap();

    assert!(quality.get(0).is_none());
    assert_eq!(quality.get(1), Some("SUSPECT"));
    assert!(explanation
        .get(1)
        .unwrap()
        .contains("timestamp_before_deployment"));
    assert!(explanation
        .get(2)
        .unwrap()
        .contains("timestamp_after_deployment"));
    assert!(explanation.get(3).unwrap().contains("timestamp_future"));
    assert!(explanation
        .get(4)
        .unwrap()
        .contains("record_gap_gt_quality_gap_years"));
    assert!(explanation
        .get(5)
        .unwrap()
        .contains("sap_flux_density_above_quality_max_flux_cm_hr"));
    assert!(explanation
        .get(6)
        .unwrap()
        .contains("sap_flux_density_below_quality_min_flux_cm_hr"));
    assert!(quality.get(7) == Some("SUSPECT"));
    assert!(explanation
        .get(7)
        .unwrap()
        .contains("timestamp_before_first_deployment"));

    Ok(())
}

#[test]
fn record_gap_respects_record_sorting() -> PolarsResult<()> {
    let base = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let now = base + Duration::days(1);

    // DataFrame order intentionally scrambles record order to ensure sorting occurs.
    let records = vec![11i64, 9, 10];
    let timestamps = vec![
        (base + Duration::days(730)).timestamp_micros(), // record 11 - far future
        base.timestamp_micros(),                         // record 9 - baseline
        (base + Duration::minutes(1)).timestamp_micros(), // record 10 - near baseline
    ];

    let ts_series = Series::new("timestamp_utc".into(), timestamps.clone()).cast(
        &DataType::Datetime(TimeUnit::Microseconds, Some(polars::prelude::TimeZone::UTC)),
    )?;

    let df = DataFrame::new(vec![
        ts_series.into(),
        Series::new(
            "deployment_start_timestamp_utc".into(),
            vec![base.timestamp_micros(); records.len()],
        )
        .cast(&DataType::Datetime(
            TimeUnit::Microseconds,
            Some(polars::prelude::TimeZone::UTC),
        ))?
        .into(),
        Series::new(
            "deployment_end_timestamp_utc".into(),
            vec![(base + Duration::days(800)).timestamp_micros(); records.len()],
        )
        .cast(&DataType::Datetime(
            TimeUnit::Microseconds,
            Some(polars::prelude::TimeZone::UTC),
        ))?
        .into(),
        Series::new(
            "quality_deployment_start_grace_minutes".into(),
            vec![0.0f64; records.len()],
        )
        .into(),
        Series::new(
            "quality_deployment_end_grace_minutes".into(),
            vec![0.0f64; records.len()],
        )
        .into(),
        Series::new(
            "quality_future_lead_minutes".into(),
            vec![0.0f64; records.len()],
        )
        .into(),
        Series::new("quality_gap_years".into(), vec![0.1f64; records.len()]).into(),
        Series::new(
            "quality_max_flux_cm_hr".into(),
            vec![100.0f64; records.len()],
        )
        .into(),
        Series::new(
            "quality_min_flux_cm_hr".into(),
            vec![-100.0f64; records.len()],
        )
        .into(),
        Series::new(
            "sap_flux_density_j_dma_cm_hr".into(),
            vec![0.0f64; records.len()],
        )
        .into(),
        Series::new("record".into(), records.clone()).into(),
        Series::new("logger_id".into(), vec!["logger"; records.len()]).into(),
        Series::new("sdi12_address".into(), vec!["0"; records.len()]).into(),
    ])?;

    let result = apply_quality_filters(&df, now)?;
    let quality = result.column("quality")?.str()?;
    let explanation = result.column("quality_explanation")?.str()?;

    assert_eq!(quality.get(0), Some("SUSPECT"));
    assert!(explanation
        .get(0)
        .unwrap()
        .contains("record_gap_gt_quality_gap_years"));

    assert!(quality.get(1).is_none());
    assert!(quality.get(2).is_none());

    Ok(())
}
