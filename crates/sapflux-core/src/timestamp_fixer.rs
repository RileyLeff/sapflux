use std::collections::{HashMap, HashSet};

use chrono::{Duration, NaiveDateTime, Offset, TimeZone as _, Utc};
use chrono_tz::Tz;
use polars::df;
use polars::lazy::dsl::*;
use polars::prelude::*;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum TimestampFixerError {
    #[error("polars operation failed: {0}")]
    Polars(#[from] PolarsError),
    #[error("logger {logger_id} had no active deployment for anchor {anchor_time}")]
    NoActiveDeployment {
        logger_id: String,
        anchor_time: NaiveDateTime,
    },
    #[error("site metadata missing for site {0}")]
    SiteNotFound(Uuid),
    #[error("invalid anchor timestamp micros {0}")]
    InvalidAnchor(i64),
    #[error("missing utc offset for logger {logger_id} / signature {file_set_signature}")]
    MissingUtcOffset {
        logger_id: String,
        file_set_signature: String,
    },
}

#[derive(Debug, Clone)]
pub struct TimestampFixResult {
    pub dataframe: DataFrame,
    pub skipped_chunks: Vec<SkippedChunk>,
}

#[derive(Debug, Clone)]
pub struct SkippedChunk {
    pub logger_id: String,
    pub file_set_signature: String,
    pub anchor_timestamp: NaiveDateTime,
    pub row_count: usize,
    pub reason: SkippedChunkReason,
}

#[derive(Debug, Clone, Copy)]
pub enum SkippedChunkReason {
    NoActiveDeployment,
    MissingUtcOffset,
}

type ChunkOffsets = HashMap<(String, String), i32>;
type ChunkSkipDetails = Vec<(String, String, NaiveDateTime, SkippedChunkReason)>;

#[derive(Debug, Clone)]
pub struct SiteMetadata {
    pub site_id: Uuid,
    pub timezone: Tz,
}

#[derive(Debug, Clone)]
pub struct DeploymentMetadata {
    pub datalogger_id: String,
    pub site_id: Uuid,
    pub start_timestamp_local: NaiveDateTime,
    pub end_timestamp_local: Option<NaiveDateTime>,
}

struct RecordEntry {
    timestamp_micros: i64,
    file_hashes: HashSet<String>,
}

#[derive(Clone)]
struct RecordRow {
    logger_id: String,
    record: i64,
    timestamp_micros: i64,
    file_set_signature: String,
}

/// Attaches a `timestamp_utc` column to the provided observation DataFrame using the algorithm
/// described in the planning docs (grouping by logger/file-set signature and resolving TZ offsets).
pub fn correct_timestamps(
    observations: &DataFrame,
    sites: &[SiteMetadata],
    deployments: &[DeploymentMetadata],
) -> Result<TimestampFixResult, TimestampFixerError> {
    if observations.is_empty() {
        return Ok(TimestampFixResult {
            dataframe: observations.clone(),
            skipped_chunks: Vec::new(),
        });
    }

    let mut records_map: HashMap<(String, i64), RecordEntry> = HashMap::new();
    populate_records_map(observations, &mut records_map)?;

    let mut record_rows: Vec<RecordRow> = records_map
        .into_iter()
        .map(|((logger_id, record), entry)| {
            let mut hashes: Vec<String> = entry.file_hashes.into_iter().collect();
            hashes.sort();
            let signature = hashes.join("+");
            RecordRow {
                logger_id,
                record,
                timestamp_micros: entry.timestamp_micros,
                file_set_signature: signature,
            }
        })
        .collect();

    record_rows.sort_by_key(|row| (row.logger_id.clone(), row.record));

    let mut chunk_row_counts: HashMap<(String, String), usize> = HashMap::new();
    for row in &record_rows {
        *chunk_row_counts
            .entry((row.logger_id.clone(), row.file_set_signature.clone()))
            .or_insert(0) += 1;
    }

    let site_map: HashMap<Uuid, &SiteMetadata> = sites.iter().map(|s| (s.site_id, s)).collect();
    let deployment_map = build_deployment_map(deployments);

    let (chunk_offsets, skipped_info) =
        compute_chunk_offsets(&record_rows, &site_map, &deployment_map)?;

    let row_count = record_rows.len();
    let mut logger_ids = Vec::with_capacity(row_count);
    let mut records = Vec::with_capacity(row_count);
    let mut timestamps = Vec::with_capacity(row_count);
    let mut signatures = Vec::with_capacity(row_count);
    let mut offsets_per_record = Vec::with_capacity(row_count);
    let mut timestamp_utc = Vec::with_capacity(row_count);

    for row in record_rows.iter() {
        let key = (row.logger_id.clone(), row.file_set_signature.clone());
        if let Some(offset) = chunk_offsets.get(&key) {
            let local_dt = naive_from_micros(row.timestamp_micros)?;
            let utc_dt = local_dt - Duration::seconds(*offset as i64);

            logger_ids.push(row.logger_id.clone());
            records.push(row.record);
            timestamps.push(row.timestamp_micros);
            signatures.push(row.file_set_signature.clone());
            offsets_per_record.push(*offset);
            timestamp_utc.push(naive_to_micros(utc_dt));
        }
    }

    let record_df = df![
        "logger_id" => logger_ids,
        "record" => records,
        "file_set_signature" => signatures,
        "utc_offset_seconds" => offsets_per_record,
        "timestamp_local" => timestamps,
        "timestamp_utc_raw" => timestamp_utc,
    ]?
    .lazy()
    .with_column(
        col("timestamp_local")
            .cast(DataType::Datetime(TimeUnit::Microseconds, None))
            .alias("timestamp"),
    )
    .with_column(
        col("timestamp_utc_raw")
            .cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(polars::prelude::TimeZone::UTC),
            ))
            .alias("timestamp_utc"),
    )
    .select([
        col("logger_id"),
        col("record"),
        col("file_set_signature"),
        col("utc_offset_seconds"),
        col("timestamp"),
        col("timestamp_utc"),
    ])
    .collect()?;

    let joined_df = observations
        .clone()
        .lazy()
        .join(
            record_df.clone().lazy().select([
                col("logger_id"),
                col("record"),
                col("file_set_signature"),
                col("timestamp_utc"),
            ]),
            [col("logger_id"), col("record")],
            [col("logger_id"), col("record")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()?;
    let filtered_df = joined_df
        .lazy()
        .filter(col("timestamp_utc").is_not_null())
        .collect()?;

    if filtered_df.column("timestamp_utc")?.null_count() > 0 {
        return Err(TimestampFixerError::MissingUtcOffset {
            logger_id: String::new(),
            file_set_signature: String::new(),
        });
    }

    let mut skipped_chunks: Vec<SkippedChunk> = Vec::new();
    for (logger_id, signature, anchor_ts, reason) in skipped_info {
        let row_count = chunk_row_counts
            .get(&(logger_id.clone(), signature.clone()))
            .copied()
            .unwrap_or(0);
        skipped_chunks.push(SkippedChunk {
            logger_id,
            file_set_signature: signature,
            anchor_timestamp: anchor_ts,
            row_count,
            reason,
        });
    }

    Ok(TimestampFixResult {
        dataframe: filtered_df,
        skipped_chunks,
    })
}

fn populate_records_map(
    observations: &DataFrame,
    map: &mut HashMap<(String, i64), RecordEntry>,
) -> Result<(), TimestampFixerError> {
    let logger_ids = observations
        .column("logger_id")?
        .as_materialized_series()
        .str()?;
    let records = observations
        .column("record")?
        .as_materialized_series()
        .i64()?;
    let timestamps = observations
        .column("timestamp")?
        .as_materialized_series()
        .datetime()?;
    let file_hashes = observations
        .column("file_hash")?
        .as_materialized_series()
        .str()?;

    for idx in 0..observations.height() {
        let logger_id = logger_ids
            .get(idx)
            .ok_or_else(|| TimestampFixerError::MissingUtcOffset {
                logger_id: "".into(),
                file_set_signature: "".into(),
            })?
            .to_string();
        let record = records
            .get(idx)
            .ok_or_else(|| TimestampFixerError::MissingUtcOffset {
                logger_id: logger_id.clone(),
                file_set_signature: "".into(),
            })?;
        let timestamp_micros = timestamps
            .get(idx)
            .ok_or(TimestampFixerError::InvalidAnchor(0))?;
        let file_hash = file_hashes.get(idx).unwrap_or("").to_string();

        map.entry((logger_id.clone(), record))
            .and_modify(|entry| {
                entry.file_hashes.insert(file_hash.clone());
                if timestamp_micros < entry.timestamp_micros {
                    entry.timestamp_micros = timestamp_micros;
                }
            })
            .or_insert_with(|| RecordEntry {
                timestamp_micros,
                file_hashes: {
                    let mut set = HashSet::new();
                    set.insert(file_hash);
                    set
                },
            });
    }

    Ok(())
}

fn build_deployment_map<'a>(
    deployments: &'a [DeploymentMetadata],
) -> HashMap<&'a str, Vec<&'a DeploymentMetadata>> {
    let mut map: HashMap<&'a str, Vec<&'a DeploymentMetadata>> = HashMap::new();
    for deployment in deployments {
        map.entry(deployment.datalogger_id.as_str())
            .or_default()
            .push(deployment);
    }
    map
}

fn compute_chunk_offsets(
    entries: &[RecordRow],
    site_map: &HashMap<Uuid, &SiteMetadata>,
    deployments: &HashMap<&str, Vec<&DeploymentMetadata>>,
) -> Result<(ChunkOffsets, ChunkSkipDetails), TimestampFixerError> {
    let mut anchor_map: HashMap<(String, String), (i64, i64)> = HashMap::new();

    for row in entries.iter() {
        anchor_map
            .entry((row.logger_id.clone(), row.file_set_signature.clone()))
            .and_modify(|(min_record, anchor)| {
                if row.record < *min_record {
                    *min_record = row.record;
                    *anchor = row.timestamp_micros;
                }
            })
            .or_insert((row.record, row.timestamp_micros));
    }

    let mut offsets = HashMap::with_capacity(anchor_map.len());
    let mut skipped = Vec::new();

    for ((logger_id, signature), (_, anchor_micros)) in anchor_map.into_iter() {
        let anchor_time = naive_from_micros(anchor_micros)?;
        match find_offset_for_chunk(&logger_id, anchor_time, site_map, deployments)? {
            OffsetLookup::Offset(offset) => {
                offsets.insert((logger_id, signature), offset);
            }
            OffsetLookup::Skipped(reason) => {
                skipped.push((logger_id, signature, anchor_time, reason));
            }
        }
    }

    Ok((offsets, skipped))
}

enum OffsetLookup {
    Offset(i32),
    Skipped(SkippedChunkReason),
}

fn find_offset_for_chunk(
    logger_id: &str,
    anchor_time: NaiveDateTime,
    site_map: &HashMap<Uuid, &SiteMetadata>,
    deployments: &HashMap<&str, Vec<&DeploymentMetadata>>,
) -> Result<OffsetLookup, TimestampFixerError> {
    let deployment_opt = deployments.get(logger_id).and_then(|deps| {
        deps.iter().find(|d| {
            anchor_time >= d.start_timestamp_local
                && d.end_timestamp_local.is_none_or(|end| anchor_time < end)
        })
    });

    let deployment = match deployment_opt {
        Some(dep) => dep,
        None => {
            return Ok(OffsetLookup::Skipped(
                SkippedChunkReason::NoActiveDeployment,
            ));
        }
    };

    let site = site_map
        .get(&deployment.site_id)
        .ok_or(TimestampFixerError::SiteNotFound(deployment.site_id))?;

    use chrono::offset::LocalResult;
    let local_result = site.timezone.from_local_datetime(&anchor_time);
    let offset_seconds = match local_result {
        LocalResult::Single(dt) => dt.offset().fix().local_minus_utc(),
        LocalResult::Ambiguous(a, b) => {
            let off_a = a.offset().fix().local_minus_utc();
            let off_b = b.offset().fix().local_minus_utc();
            off_a.max(off_b)
        }
        LocalResult::None => 0,
    };

    Ok(OffsetLookup::Offset(offset_seconds))
}

fn naive_from_micros(value: i64) -> Result<NaiveDateTime, TimestampFixerError> {
    let secs = value.div_euclid(1_000_000);
    let micros = value.rem_euclid(1_000_000) as u32;
    chrono::DateTime::<Utc>::from_timestamp(secs, micros * 1_000)
        .map(|dt| dt.naive_utc())
        .ok_or(TimestampFixerError::InvalidAnchor(value))
}

fn naive_to_micros(value: NaiveDateTime) -> i64 {
    let dt_utc = value.and_utc();
    dt_utc.timestamp() * 1_000_000 + i64::from(dt_utc.timestamp_subsec_nanos() / 1_000)
}
