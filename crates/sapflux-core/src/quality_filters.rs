use std::collections::HashMap;

use chrono::{DateTime, Utc};
use polars::prelude::*;

const MINUTES_TO_MICROS: f64 = 60.0 * 1_000_000.0;
const YEARS_TO_MICROS: f64 = 365.25 * 24.0 * 60.0 * 60.0 * 1_000_000.0;

pub fn apply_quality_filters(df: &DataFrame, now: DateTime<Utc>) -> Result<DataFrame, PolarsError> {
    let len = df.height();

    let timestamp = df.column("timestamp_utc")?.datetime()?;
    let start_ts = df.column("deployment_start_timestamp_utc")?.datetime()?;
    let end_ts = df.column("deployment_end_timestamp_utc")?.datetime()?;
    let start_grace = df.column("quality_deployment_start_grace_minutes")?.f64()?;
    let end_grace = df.column("quality_deployment_end_grace_minutes")?.f64()?;
    let future_lead = df.column("quality_future_lead_minutes")?.f64()?;
    let gap_years = df.column("quality_gap_years")?.f64()?;
    let flux_max = df.column("quality_max_flux_cm_hr")?.f64()?;
    let flux_min = df.column("quality_min_flux_cm_hr")?.f64()?;
    let sap_flux = df.column("sap_flux_density_j_dma_cm_hr")?.f64()?;
    let logger_id = df.column("logger_id")?.str()?;
    let sdi_series = df.column("sdi12_address")?.str()?;
    let record_series = df.column("record")?.i64()?;

    let mut earliest_start_by_sensor: HashMap<(String, String), i64> = HashMap::new();
    for idx in 0..len {
        let (Some(logger), Some(address), Some(start_value)) =
            (logger_id.get(idx), sdi_series.get(idx), start_ts.get(idx))
        else {
            continue;
        };

        let key = (logger.to_string(), address.to_string());
        earliest_start_by_sensor
            .entry(key)
            .and_modify(|existing| {
                if start_value < *existing {
                    *existing = start_value;
                }
            })
            .or_insert(start_value);
    }

    let mut per_logger: HashMap<&str, Vec<(i64, i64, usize)>> = HashMap::new();

    for idx in 0..len {
        if let (Some(logger), Some(record), Some(ts)) = (
            logger_id.get(idx),
            record_series.get(idx),
            timestamp.get(idx),
        ) {
            per_logger
                .entry(logger)
                .or_default()
                .push((record, ts, idx));
        }
    }

    let mut record_gap_flags = vec![false; len];

    for entries in per_logger.values_mut() {
        entries.sort_by_key(|(record, _, _)| *record);
        for window in entries.windows(2) {
            let (_, prev_ts, _) = window[0];
            let (_, curr_ts, curr_idx) = window[1];
            let delta_years = (curr_ts - prev_ts).abs() as f64 / YEARS_TO_MICROS;
            let threshold = gap_years.get(curr_idx).unwrap_or(0.0);
            if delta_years > threshold {
                record_gap_flags[curr_idx] = true;
            }
        }
    }

    let mut quality: Vec<Option<String>> = Vec::with_capacity(len);
    let mut explanations: Vec<Option<String>> = Vec::with_capacity(len);

    let now_micros = now.timestamp_micros();

    for (idx, &has_gap) in record_gap_flags.iter().enumerate() {
        let ts = timestamp.get(idx);
        let deployment_start = start_ts.get(idx);
        let mut reasons = Vec::new();

        if let (Some(ts_val), Some(start_val)) = (ts, deployment_start) {
            let grace = start_grace.get(idx).unwrap_or(0.0);
            let limit = (start_val as f64) - grace * MINUTES_TO_MICROS;
            if (ts_val as f64) < limit {
                reasons.push("timestamp_before_deployment");
            }
        }

        if let Some(ts_val) = ts {
            if deployment_start.is_none() {
                if let (Some(logger), Some(address)) = (logger_id.get(idx), sdi_series.get(idx)) {
                    let key = (logger.to_string(), address.to_string());
                    if let Some(&earliest_start) = earliest_start_by_sensor.get(&key) {
                        let grace = start_grace.get(idx).unwrap_or(0.0);
                        let limit = (earliest_start as f64) - grace * MINUTES_TO_MICROS;
                        if (ts_val as f64) < limit {
                            reasons.push("timestamp_before_first_deployment");
                        }
                    }
                }
            }

            let effective_end = end_ts.get(idx).unwrap_or(now_micros);
            let grace = end_grace.get(idx).unwrap_or(0.0);
            let limit = (effective_end as f64) + grace * MINUTES_TO_MICROS;
            if (ts_val as f64) > limit {
                reasons.push("timestamp_after_deployment");
            }
        }

        if let Some(ts_val) = ts {
            let lead = future_lead.get(idx).unwrap_or(0.0);
            let future_limit = (now_micros as f64) + lead * MINUTES_TO_MICROS;
            if (ts_val as f64) > future_limit {
                reasons.push("timestamp_future");
            }
        }

        if has_gap {
            reasons.push("record_gap_gt_quality_gap_years");
        }

        if let Some(flux) = sap_flux.get(idx) {
            if let Some(max_val) = flux_max.get(idx) {
                if flux > max_val {
                    reasons.push("sap_flux_density_above_quality_max_flux_cm_hr");
                }
            }
            if let Some(min_val) = flux_min.get(idx) {
                if flux < min_val {
                    reasons.push("sap_flux_density_below_quality_min_flux_cm_hr");
                }
            }
        }

        if reasons.is_empty() {
            quality.push(None);
            explanations.push(None);
        } else {
            quality.push(Some("SUSPECT".to_string()));
            explanations.push(Some(reasons.join("|")));
        }
    }

    let quality_series = Series::new(
        "quality".into(),
        quality
            .iter()
            .map(|opt| opt.as_deref())
            .collect::<Vec<Option<&str>>>(),
    );
    let explanation_series = Series::new(
        "quality_explanation".into(),
        explanations
            .iter()
            .map(|opt| opt.as_deref())
            .collect::<Vec<Option<&str>>>(),
    );

    let mut output = df.clone();
    let mut columns = [quality_series.into(), explanation_series.into()];
    output.hstack_mut(columns.as_mut_slice())?;

    Ok(output)
}
