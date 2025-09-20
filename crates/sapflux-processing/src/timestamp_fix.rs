use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use chrono::{Duration as ChronoDuration, NaiveDateTime};
use chrono_tz::Tz;
use polars::prelude::*;
use sapflux_parser::ParsedFileData;
use sapflux_repository::metadata::{Deployment, MetadataRepository, Site};
use serde_json::Value;

use crate::ProcessingError;

pub async fn apply(
    metadata_repo: Arc<MetadataRepository>,
    parsed: ParsedFileData,
) -> Result<ParsedFileData, ProcessingError> {
    let sites = metadata_repo.fetch_all_sites().await?;
    let deployments = metadata_repo.fetch_all_deployments().await?;

    let site_timezones = build_site_timezone_map(&sites)?;
    let logger_deployments = build_logger_deployments(&deployments);

    fix_dataframe(parsed, &site_timezones, &logger_deployments)
}

fn build_site_timezone_map(sites: &[Site]) -> Result<HashMap<UuidWrapper, Tz>, ProcessingError> {
    let mut map = HashMap::new();
    for site in sites {
        let tz: Tz = site
            .timezone
            .parse()
            .map_err(|err| ProcessingError::Stage(format!("invalid timezone {}: {err}", site.timezone)))?;
        map.insert(UuidWrapper(site.site_id), tz);
    }
    Ok(map)
}

fn build_logger_deployments(deployments: &[Deployment]) -> HashMap<String, Vec<Deployment>> {
    let mut map: HashMap<String, Vec<Deployment>> = HashMap::new();
    for deployment in deployments.iter() {
        map.entry(deployment.logger_id.clone())
            .or_default()
            .push(deployment.clone());
    }
    for deps in map.values_mut() {
        deps.sort_by_key(|d| d.start_timestamp_utc);
    }
    map
}

fn fix_dataframe(
    mut parsed: ParsedFileData,
    site_timezones: &HashMap<UuidWrapper, Tz>,
    logger_deployments: &HashMap<String, Vec<Deployment>>,
) -> Result<ParsedFileData, ProcessingError> {
    let mut logger_df = parsed.logger.df.clone();
    if !logger_df.get_column_names().iter().any(|name| *name == "logger_id") {
        return Err(ProcessingError::Stage("logger dataframe missing 'logger_id' column".into()));
    }
    if !logger_df.get_column_names().iter().any(|name| *name == "timestamp") {
        return Err(ProcessingError::Stage("logger dataframe missing 'timestamp' column".into()));
    }

    // Precompute per timestamp file sets if available
    let has_file_hash = logger_df.get_column_names().iter().any(|name| *name == "file_hash");

    let logger_series = logger_df.column("logger_id")?.utf8()?;
    let timestamp_series = logger_df.column("timestamp")?.datetime()?;

    let file_hash_series = if has_file_hash {
        Some(logger_df.column("file_hash")?.utf8()?)
    } else {
        None
    };

    let mut timestamp_files: HashMap<(String, i64), BTreeSet<String>> = HashMap::new();
    if let Some(file_hashes) = &file_hash_series {
        for idx in 0..logger_df.height() {
            let logger = logger_series.get(idx).unwrap_or("").to_string();
            let ts = timestamp_series.get(idx).unwrap_or(0);
            let file_hash = file_hashes.get(idx).unwrap_or("").to_string();
            timestamp_files
                .entry((logger, ts))
                .or_default()
                .insert(file_hash);
        }
    }

    let mut chunk_ids: Vec<u32> = Vec::with_capacity(logger_df.height());
    let mut chunk_logger: HashMap<u32, String> = HashMap::new();
    let mut chunk_first_index: HashMap<u32, usize> = HashMap::new();
    let mut chunk_key_map: HashMap<String, u32> = HashMap::new();
    let mut next_chunk_id: u32 = 0;

    for idx in 0..logger_df.height() {
        let logger = logger_series.get(idx).unwrap_or("").to_string();
        let ts = timestamp_series.get(idx).unwrap_or(0);

        let file_key = if let Some(_) = file_hash_series {
            let set = timestamp_files
                .get(&(logger.clone(), ts))
                .cloned()
                .unwrap_or_default();
            set.into_iter().collect::<Vec<_>>().join(",")
        } else {
            "single_file".to_string()
        };

        let chunk_key = format!("{}|{}", logger, file_key);
        let chunk_id = chunk_key_map.entry(chunk_key).or_insert_with(|| {
            let id = next_chunk_id;
            next_chunk_id += 1;
            id
        });

        chunk_ids.push(*chunk_id);
        chunk_logger.entry(*chunk_id).or_insert(logger.clone());
        chunk_first_index.entry(*chunk_id).or_insert(idx);
    }

    let mut chunk_offsets: HashMap<u32, i32> = HashMap::new();

    for (chunk_id, first_idx) in chunk_first_index.iter() {
        let logger = chunk_logger
            .get(chunk_id)
            .cloned()
            .unwrap_or_else(|| "".to_string());
        let naive_ts = timestamp_series
            .get(*first_idx)
            .and_then(|v| NaiveDateTime::from_timestamp_micros(v));
        if let Some(naive_ts) = naive_ts {
            let offset = determine_offset(
                &logger,
                naive_ts,
                logger_deployments,
                site_timezones,
            )?;
            chunk_offsets.insert(*chunk_id, offset);
        } else {
            chunk_offsets.insert(*chunk_id, 0);
        }
    }

    let mut utc_values: Vec<Option<i64>> = Vec::with_capacity(logger_df.height());
    for (idx, chunk_id) in chunk_ids.iter().enumerate() {
        let ts = timestamp_series.get(idx);
        let Some(ts) = ts else {
            utc_values.push(None);
            continue;
        };
        let naive = NaiveDateTime::from_timestamp_micros(ts);
        let Some(naive) = naive else {
            utc_values.push(None);
            continue;
        };
        let offset_seconds = chunk_offsets.get(chunk_id).copied().unwrap_or(0);
        let utc = naive - ChronoDuration::seconds(offset_seconds as i64);
        utc_values.push(Some(utc.timestamp_micros()));
    }

    let chunk_series = Series::new("chunk_id", chunk_ids);
    let utc_series = Series::new(
        "timestamp_utc",
        utc_values.into_iter().map(|opt| opt.map(|v| v as i64)).collect::<Vec<_>>(),
    )
    .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;

    logger_df.with_column(chunk_series)?;
    logger_df.with_column(utc_series)?;

    parsed.logger.df = logger_df;
    Ok(parsed)
}

fn determine_offset(
    logger: &str,
    chunk_start: NaiveDateTime,
    logger_deployments: &HashMap<String, Vec<Deployment>>,
    site_timezones: &HashMap<UuidWrapper, Tz>,
) -> Result<i32, ProcessingError> {
    let deployments = logger_deployments
        .get(logger)
        .ok_or_else(|| ProcessingError::Stage(format!("no deployment metadata for logger {logger}")))?;

    for deployment in deployments {
        if let Some(site_tz) = site_timezones.get(&UuidWrapper(deployment.site_id)) {
            let start_local = deployment
                .start_timestamp_utc
                .with_timezone(site_tz)
                .naive_local();
            let end_local = deployment
                .end_timestamp_utc
                .map(|dt| dt.with_timezone(site_tz).naive_local());

            let within = chunk_start >= start_local
                && end_local.map(|end| chunk_start < end).unwrap_or(true);

            if within {
                return offset_from_timezone(site_tz, chunk_start);
            }
        }
    }

    // Fallback to the first deployment's timezone if no interval matched
    if let Some(deployment) = deployments.first() {
        if let Some(site_tz) = site_timezones.get(&UuidWrapper(deployment.site_id)) {
            return offset_from_timezone(site_tz, chunk_start);
        }
    }

    Ok(0)
}

fn offset_from_timezone(tz: &Tz, naive: NaiveDateTime) -> Result<i32, ProcessingError> {
    use chrono::offset::LocalResult;

    let local_result = tz.from_local_datetime(&naive);
    let datetime = match local_result {
        LocalResult::Single(dt) => dt,
        LocalResult::Ambiguous(dt1, dt2) => {
            // prefer the later offset (standard time) to match post-transition behavior
            if dt1.offset().fix().local_minus_utc() < dt2.offset().fix().local_minus_utc() {
                dt2
            } else {
                dt1
            }
        }
        LocalResult::None => {
            let adjusted = naive + ChronoDuration::hours(1);
            tz.from_local_datetime(&adjusted)
                .single()
                .ok_or_else(|| ProcessingError::Stage("unable to resolve DST gap".into()))?
                - ChronoDuration::hours(1)
        }
    };

    Ok(datetime.offset().fix().local_minus_utc())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct UuidWrapper(uuid::Uuid);
