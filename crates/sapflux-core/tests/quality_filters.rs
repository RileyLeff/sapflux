use chrono::{Duration, TimeZone, Utc};
use polars::prelude::*;

use sapflux_core::quality_filters::apply_quality_filters;

#[test]
fn quality_filters_flag_expected_rules() -> PolarsResult<()> {
    let base = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let now = base + Duration::hours(1);

    let timestamps = vec![
        base + Duration::minutes(15), // good
        base - Duration::minutes(20), // before start
        base + Duration::hours(2),    // after end
        now + Duration::minutes(20),  // future
        base + Duration::days(800),   // record gap
        base + Duration::minutes(10), // high flux
        base + Duration::minutes(12), // low flux
    ];

    let start_times = vec![
        base - Duration::minutes(10),
        base,
        base - Duration::minutes(10),
        base - Duration::minutes(10),
        base - Duration::minutes(10),
        base - Duration::minutes(10),
        base - Duration::minutes(10),
    ];

    let end_times = vec![
        base + Duration::minutes(60),
        base + Duration::minutes(60),
        base + Duration::minutes(30),
        base + Duration::hours(3),
        base + Duration::hours(3),
        base + Duration::hours(3),
        base + Duration::hours(3),
    ];

    let timestamp_series = Series::new(
        "timestamp_utc",
        timestamps
            .iter()
            .map(|dt| dt.timestamp_micros())
            .collect::<Vec<_>>(),
    )
    .cast(&DataType::Datetime(
        TimeUnit::Microseconds,
        Some(polars::prelude::TimeZone::UTC),
    ))?;

    let start_series = Series::new(
        "deployment_start_timestamp_utc",
        start_times
            .iter()
            .map(|dt| dt.timestamp_micros())
            .collect::<Vec<_>>(),
    )
    .cast(&timestamp_series.dtype().clone())?;

    let end_series = Series::new(
        "deployment_end_timestamp_utc",
        end_times
            .iter()
            .map(|dt| dt.timestamp_micros())
            .collect::<Vec<_>>(),
    )
    .cast(&timestamp_series.dtype().clone())?;

    let df = DataFrame::new(vec![
        timestamp_series,
        start_series,
        end_series,
        Series::new(
            "quality_deployment_start_grace_minutes",
            vec![0.0f64; timestamps.len()],
        ),
        Series::new(
            "quality_deployment_end_grace_minutes",
            vec![0.0f64; timestamps.len()],
        ),
        Series::new(
            "quality_future_lead_minutes",
            vec![5.0f64; timestamps.len()],
        ),
        Series::new("quality_gap_years", vec![0.01f64; timestamps.len()]),
        Series::new("quality_max_flux_cm_hr", vec![20.0f64; timestamps.len()]),
        Series::new("quality_min_flux_cm_hr", vec![-10.0f64; timestamps.len()]),
        Series::new(
            "sap_flux_density_j_dma_cm_hr",
            vec![5.0, 5.0, 5.0, 5.0, 5.0, 30.0, -20.0],
        ),
        Series::new("record", (1..=timestamps.len() as i64).collect::<Vec<_>>()),
        Series::new("logger_id", vec!["logger"; timestamps.len()]),
    ])?;

    let result = apply_quality_filters(&df, now).unwrap();
    let quality = result.column("quality").unwrap().utf8().unwrap();
    let explanation = result
        .column("quality_explanation")
        .unwrap()
        .utf8()
        .unwrap();

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

    Ok(())
}
