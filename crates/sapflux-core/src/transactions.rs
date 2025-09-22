#![cfg(feature = "runtime")]

use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use sqlx::Row;
use tokio::task;
use uuid::Uuid;

use crate::db::DbPool;
use crate::ingestion::{self, FileInput, FileReport, FileStatus, IngestionBatch};
use crate::object_store::ObjectStore;
use crate::pipelines::{all_pipelines, ExecutionContext};
use polars::prelude::{ChunkAgg, DataFrame};

#[derive(Debug)]
pub struct TransactionFile {
    pub path: String,
    pub contents: Vec<u8>,
}

#[derive(Debug)]
pub struct TransactionRequest {
    pub user_id: String,
    pub message: Option<String>,
    pub dry_run: bool,
    pub files: Vec<TransactionFile>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PipelineStatus {
    Skipped,
    Success,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct PipelineSummary {
    pub pipeline: Option<String>,
    pub status: PipelineStatus,
    pub row_count: Option<usize>,
    pub error: Option<String>,
    pub quality_summary: Option<QualitySummary>,
    pub provenance_summary: Option<ProvenanceSummary>,
    pub record_summary: Option<RecordSummary>,
}

#[derive(Debug, Serialize)]
pub struct TransactionReceipt {
    pub transaction_id: Option<Uuid>,
    pub dry_run: bool,
    pub files: Vec<FileReport>,
    pub ingestion_summary: IngestionSummary,
    pub pipeline: PipelineSummary,
}

#[derive(Debug, Clone, Serialize)]
pub struct IngestionSummary {
    pub total: usize,
    pub parsed: usize,
    pub duplicates: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct QualitySummary {
    pub total_rows: usize,
    pub suspect_rows: usize,
    pub good_rows: usize,
    pub suspect_ratio: f64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub top_reasons: Vec<QualityReasonCount>,
}

#[derive(Debug, Clone, Serialize)]
pub struct QualityReasonCount {
    pub reason: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProvenanceSummary {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub top_overrides: Vec<ParameterProvenanceEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParameterProvenanceEntry {
    pub parameter: String,
    pub source: String,
    pub count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecordSummary {
    pub logger_count: usize,
    pub sensor_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeframe_utc: Option<TimeframeUtc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimeframeUtc {
    pub start: String,
    pub end: String,
}

const TRANSACTION_LOCK_KEY: i64 = 0x5350464C5558; // "SPFLUX"

pub async fn execute_transaction(
    pool: &DbPool,
    object_store: &ObjectStore,
    request: TransactionRequest,
) -> Result<TransactionReceipt> {
    let lock = AdvisoryLock::acquire(pool, TRANSACTION_LOCK_KEY).await?;
    let result = execute_transaction_locked(pool, object_store, request).await;
    lock.release().await?;
    result
}

async fn execute_transaction_locked(
    pool: &DbPool,
    object_store: &ObjectStore,
    request: TransactionRequest,
) -> Result<TransactionReceipt> {
    let TransactionRequest {
        user_id,
        message,
        dry_run,
        files,
    } = request;

    if files.is_empty() {
        return Err(anyhow!("transaction must include at least one file"));
    }

    let transaction_id = if dry_run {
        None
    } else {
        let id = Uuid::new_v4();
        sqlx::query(
            r#"
                INSERT INTO transactions (transaction_id, user_id, message, outcome)
                VALUES ($1, $2, $3, 'PENDING')
            "#,
        )
        .bind(id)
        .bind(&user_id)
        .bind(&message)
        .execute(pool)
        .await?;
        Some(id)
    };

    let existing_hashes = load_existing_hashes(pool).await?;

    let file_inputs: Vec<FileInput<'_>> = files
        .iter()
        .map(|f| FileInput {
            path: f.path.as_str(),
            contents: f.contents.as_slice(),
        })
        .collect();

    let ingestion_batch = ingestion::ingest_files(&file_inputs, &existing_hashes);

    let ingestion_summary = IngestionSummary {
        total: ingestion_batch.reports.len(),
        parsed: ingestion_batch
            .reports
            .iter()
            .filter(|r| r.status == FileStatus::Parsed)
            .count(),
        duplicates: ingestion_batch
            .reports
            .iter()
            .filter(|r| r.status == FileStatus::Duplicate)
            .count(),
        failed: ingestion_batch
            .reports
            .iter()
            .filter(|r| r.status == FileStatus::Failed)
            .count(),
    };

    let mut pipeline_summary = if ingestion_batch.parsed.is_empty() {
        PipelineSummary {
            pipeline: None,
            status: PipelineStatus::Skipped,
            row_count: None,
            error: None,
            quality_summary: None,
            provenance_summary: None,
            record_summary: None,
        }
    } else {
        match ExecutionContext::load_from_db(pool).await {
            Ok(context) => run_pipeline(&context, &ingestion_batch),
            Err(err) => PipelineSummary {
                pipeline: None,
                status: PipelineStatus::Failed,
                row_count: None,
                error: Some(err.to_string()),
                quality_summary: None,
                provenance_summary: None,
                record_summary: None,
            },
        }
    };

    if !dry_run {
        if let Some(id) = transaction_id {
            if pipeline_summary.status != PipelineStatus::Failed {
                if let Err(err) = upload_new_raw_files(object_store, &files, &ingestion_batch).await
                {
                    pipeline_summary = PipelineSummary {
                        status: PipelineStatus::Failed,
                        error: Some(format!("object store upload failed: {err}")),
                        ..pipeline_summary.clone()
                    };

                    let receipt = TransactionReceipt {
                        transaction_id: Some(id),
                        dry_run,
                        files: ingestion_batch.reports.clone(),
                        ingestion_summary: ingestion_summary.clone(),
                        pipeline: pipeline_summary.clone(),
                    };

                    sqlx::query(
                        r#"
                            UPDATE transactions
                            SET outcome = 'REJECTED',
                                receipt = $1
                            WHERE transaction_id = $2
                        "#,
                    )
                    .bind(serde_json::to_value(&receipt)?)
                    .bind(id)
                    .execute(pool)
                    .await?;

                    return Err(err.context("object store upload failed"));
                }
            }

            let outcome = match pipeline_summary.status {
                PipelineStatus::Failed => "REJECTED",
                _ => "ACCEPTED",
            };

            if outcome == "ACCEPTED" {
                persist_ingestion(pool, id, &ingestion_batch).await?;
            }

            let receipt = TransactionReceipt {
                transaction_id: Some(id),
                dry_run,
                files: ingestion_batch.reports.clone(),
                ingestion_summary: ingestion_summary.clone(),
                pipeline: pipeline_summary.clone(),
            };

            sqlx::query(
                r#"
                    UPDATE transactions
                    SET outcome = $1,
                        receipt = $2
                    WHERE transaction_id = $3
                "#,
            )
            .bind(outcome)
            .bind(serde_json::to_value(&receipt)?)
            .bind(id)
            .execute(pool)
            .await?;

            return Ok(receipt);
        }
    }

    Ok(TransactionReceipt {
        transaction_id,
        dry_run,
        files: ingestion_batch.reports,
        ingestion_summary,
        pipeline: pipeline_summary,
    })
}

async fn load_existing_hashes(pool: &DbPool) -> Result<HashSet<String>> {
    let rows = sqlx::query(r#"SELECT file_hash FROM raw_files"#)
        .fetch_all(pool)
        .await?;

    let mut hashes = HashSet::with_capacity(rows.len());
    for row in rows {
        hashes.insert(row.try_get("file_hash")?);
    }

    Ok(hashes)
}

async fn persist_ingestion(
    pool: &DbPool,
    transaction_id: Uuid,
    batch: &IngestionBatch,
) -> Result<()> {
    let mut db_tx = pool.begin().await?;

    for parsed in batch.parsed.iter() {
        sqlx::query(
            r#"
                INSERT INTO raw_files (file_hash, ingesting_transaction_id, ingest_context, include_in_pipeline)
                VALUES ($1, $2, NULL, TRUE)
                ON CONFLICT (file_hash) DO NOTHING
            "#,
        )
        .bind(&parsed.hash)
        .bind(transaction_id)
        .execute(&mut *db_tx)
        .await?;
    }

    db_tx.commit().await?;
    Ok(())
}

fn run_pipeline(context: &ExecutionContext, batch: &IngestionBatch) -> PipelineSummary {
    if batch.parsed.is_empty() {
        return PipelineSummary {
            pipeline: None,
            status: PipelineStatus::Skipped,
            row_count: None,
            error: None,
            quality_summary: None,
            provenance_summary: None,
            record_summary: None,
        };
    }

    let parsed_refs: Vec<&dyn crate::parsers::ParsedData> =
        batch.parsed.iter().map(|p| p.data.as_ref()).collect();
    let pipeline = all_pipelines()
        .iter()
        .find(|p| p.input_data_format() == "sapflow_toa5_hierarchical_v1")
        .copied();

    if let Some(pipeline) = pipeline {
        match pipeline.run_batch(context, &parsed_refs) {
            Ok(df) => PipelineSummary {
                pipeline: Some(pipeline.code_identifier().to_string()),
                status: PipelineStatus::Success,
                row_count: Some(df.height()),
                error: None,
                quality_summary: compute_quality_summary(&df),
                provenance_summary: compute_provenance_summary(&df),
                record_summary: compute_record_summary(&df),
            },
            Err(err) => PipelineSummary {
                pipeline: Some(pipeline.code_identifier().to_string()),
                status: PipelineStatus::Failed,
                row_count: None,
                error: Some(err.to_string()),
                quality_summary: None,
                provenance_summary: None,
                record_summary: None,
            },
        }
    } else {
        PipelineSummary {
            pipeline: None,
            status: PipelineStatus::Skipped,
            row_count: None,
            error: Some("no pipeline registered for data format".into()),
            quality_summary: None,
            provenance_summary: None,
            record_summary: None,
        }
    }
}

fn compute_quality_summary(df: &DataFrame) -> Option<QualitySummary> {
    let quality_series = df.column("quality").ok()?.str().ok()?;
    let suspect_rows = quality_series
        .iter()
        .filter(|value| matches!(value, Some("SUSPECT")))
        .count();
    let total_rows = df.height();
    let good_rows = total_rows.saturating_sub(suspect_rows);
    let suspect_ratio = if total_rows == 0 {
        0.0
    } else {
        suspect_rows as f64 / total_rows as f64
    };

    let mut reason_counts: HashMap<String, usize> = HashMap::new();
    if let Ok(explanation_series) = df.column("quality_explanation") {
        if let Ok(explanations) = explanation_series.str() {
            for value in explanations.iter().flatten() {
                for reason in value.split('|').filter(|segment| !segment.is_empty()) {
                    *reason_counts.entry(reason.to_string()).or_insert(0) += 1;
                }
            }
        }
    }

    let mut reason_entries: Vec<(String, usize)> = reason_counts.into_iter().collect();
    reason_entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let top_reasons = reason_entries
        .into_iter()
        .take(5)
        .map(|(reason, count)| QualityReasonCount { reason, count })
        .collect();

    Some(QualitySummary {
        total_rows,
        suspect_rows,
        good_rows,
        suspect_ratio,
        top_reasons,
    })
}

fn compute_provenance_summary(df: &DataFrame) -> Option<ProvenanceSummary> {
    let mut entries: Vec<ParameterProvenanceEntry> = Vec::new();

    for column_name in df.get_column_names() {
        if !column_name.starts_with("parameter_source_") {
            continue;
        }

        let series = match df.column(column_name) {
            Ok(series) => series,
            Err(_) => continue,
        };

        let values = match series.str() {
            Ok(values) => values,
            Err(_) => continue,
        };

        let mut counts: HashMap<String, usize> = HashMap::new();
        for value in values.iter().flatten() {
            if value.is_empty() {
                continue;
            }
            if value.eq_ignore_ascii_case("default") || value.contains("default") {
                continue;
            }
            *counts.entry(value.to_string()).or_insert(0) += 1;
        }

        if counts.is_empty() {
            continue;
        }

        let mut overrides: Vec<(String, usize)> = counts.into_iter().collect();
        overrides.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let (source, count) = overrides[0].clone();
        entries.push(ParameterProvenanceEntry {
            parameter: column_name
                .trim_start_matches("parameter_source_")
                .to_string(),
            source,
            count,
        });
    }

    if entries.is_empty() {
        return None;
    }

    entries.sort_by(|a, b| {
        b.count
            .cmp(&a.count)
            .then_with(|| a.parameter.cmp(&b.parameter))
    });
    entries.truncate(3);

    Some(ProvenanceSummary {
        top_overrides: entries,
    })
}

fn compute_record_summary(df: &DataFrame) -> Option<RecordSummary> {
    let logger_series = df.column("logger_id").ok()?.str().ok()?;
    let sdi_series = df.column("sdi12_address").ok()?.str().ok()?;
    let depth_series = df.column("thermistor_depth").ok()?.str().ok()?;

    let mut logger_ids: HashSet<String> = HashSet::new();
    for value in logger_series.iter().flatten() {
        logger_ids.insert(value.to_string());
    }

    let mut sensors: HashSet<(String, String, String)> = HashSet::new();
    let height = df.height();
    for idx in 0..height {
        if let (Some(logger), Some(addr), Some(depth)) = (
            logger_series.get(idx),
            sdi_series.get(idx),
            depth_series.get(idx),
        ) {
            sensors.insert((logger.to_string(), addr.to_string(), depth.to_string()));
        }
    }

    let timeframe = compute_timeframe(df);

    Some(RecordSummary {
        logger_count: logger_ids.len(),
        sensor_count: sensors.len(),
        timeframe_utc: timeframe,
    })
}

fn compute_timeframe(df: &DataFrame) -> Option<TimeframeUtc> {
    let timestamp_series = df.column("timestamp_utc").ok()?.datetime().ok()?;
    let start = timestamp_series.min()?;
    let end = timestamp_series.max()?;
    let start_str = micros_to_rfc3339(start)?;
    let end_str = micros_to_rfc3339(end)?;
    Some(TimeframeUtc {
        start: start_str,
        end: end_str,
    })
}

fn micros_to_rfc3339(micros: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp_micros(micros)
        .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Micros, true))
}

async fn upload_new_raw_files(
    object_store: &ObjectStore,
    files: &[TransactionFile],
    batch: &IngestionBatch,
) -> Result<()> {
    use std::collections::{HashMap, HashSet};

    let path_map: HashMap<&str, &[u8]> = files
        .iter()
        .map(|file| (file.path.as_str(), file.contents.as_slice()))
        .collect();

    let mut uploaded = HashSet::new();

    for report in batch.reports.iter() {
        if report.status == FileStatus::Parsed && uploaded.insert(&report.hash) {
            if let Some(contents) = path_map.get(report.path.as_str()) {
                let key = ObjectStore::raw_file_key(&report.hash);
                object_store.put_raw_file(&key, contents).await?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    #[test]
    fn compute_quality_summary_counts_suspect_rows() {
        let df = df![
            "quality" => [Some("SUSPECT"), None, Some("SUSPECT"), None],
            "quality_explanation" => [
                Some("timestamp_before_deployment|timestamp_future"),
                None,
                Some("sap_flux_density_above_quality_max_flux_cm_hr"),
                None
            ]
        ]
        .expect("construct dataframe");

        let summary = compute_quality_summary(&df).expect("quality summary");
        assert_eq!(summary.total_rows, 4);
        assert_eq!(summary.suspect_rows, 2);
        assert_eq!(summary.good_rows, 2);
        assert!((summary.suspect_ratio - 0.5).abs() < f64::EPSILON);
        assert!(!summary.top_reasons.is_empty());
        assert!(summary
            .top_reasons
            .iter()
            .any(|entry| entry.reason == "timestamp_future"));
    }

    #[test]
    fn compute_record_summary_counts_loggers_and_sensors() {
        use polars::prelude::{DataType, TimeUnit};

        let mut df = df![
            "logger_id" => [Some("A"), Some("A"), Some("B")],
            "sdi12_address" => [Some("0"), Some("1"), Some("0")],
            "thermistor_depth" => [Some("inner"), Some("outer"), Some("inner")],
        ]
        .expect("construct record summary frame");

        let timestamp_series = Series::new(
            "timestamp_utc".into(),
            vec![1_000_000i64, 2_000_000i64, 3_000_000i64],
        )
        .cast(&DataType::Datetime(
            TimeUnit::Microseconds,
            Some(polars::prelude::TimeZone::UTC),
        ))
        .expect("cast timestamps to datetime");

        df.with_column(timestamp_series)
            .expect("add timestamp column");

        let summary = compute_record_summary(&df).expect("record summary");
        assert_eq!(summary.logger_count, 2);
        assert_eq!(summary.sensor_count, 3);
        let timeframe = summary.timeframe_utc.expect("timeframe present");
        assert!(timeframe.start.contains("00:00:01"));
        assert!(timeframe.end.contains("00:00:03"));
    }

    #[test]
    fn compute_provenance_summary_skips_defaults() {
        let df = df![
            "parameter_source_parameter_heat_pulse_duration_s" => [
                Some("default"),
                Some("stem_override"),
                Some("stem_override")
            ],
            "parameter_source_parameter_wood_density_kg_m3" => [
                Some("default"),
                Some("deployment_override"),
                Some("default")
            ]
        ]
        .expect("construct provenance frame");

        let summary = compute_provenance_summary(&df).expect("provenance summary");
        assert_eq!(summary.top_overrides.len(), 2);
        assert_eq!(
            summary.top_overrides[0].parameter,
            "parameter_heat_pulse_duration_s"
        );
        assert_eq!(summary.top_overrides[0].source, "stem_override");
        assert_eq!(summary.top_overrides[0].count, 2);
        assert!(summary
            .top_overrides
            .iter()
            .any(|entry| entry.source == "deployment_override"));
    }
}

struct AdvisoryLock {
    conn: Option<sqlx::pool::PoolConnection<sqlx::Postgres>>,
    key: i64,
}

impl AdvisoryLock {
    async fn acquire(pool: &DbPool, key: i64) -> Result<Self> {
        let mut conn = pool.acquire().await?;
        sqlx::query::<sqlx::Postgres>("SELECT pg_advisory_lock($1)")
            .bind(key)
            .execute(conn.as_mut())
            .await?;
        Ok(Self {
            conn: Some(conn),
            key,
        })
    }

    async fn release(mut self) -> Result<()> {
        if let Some(mut conn) = self.conn.take() {
            sqlx::query::<sqlx::Postgres>("SELECT pg_advisory_unlock($1)")
                .bind(self.key)
                .execute(conn.as_mut())
                .await?;
        }
        Ok(())
    }
}

impl Drop for AdvisoryLock {
    fn drop(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            let key = self.key;
            task::spawn(async move {
                if let Err(err) = sqlx::query::<sqlx::Postgres>("SELECT pg_advisory_unlock($1)")
                    .bind(key)
                    .execute(conn.as_mut())
                    .await
                {
                    tracing::warn!("failed to release advisory lock in drop: {err}");
                }
            });
        }
    }
}
