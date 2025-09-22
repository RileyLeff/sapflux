#![cfg(feature = "runtime")]

use std::collections::HashSet;

use anyhow::{anyhow, Result};
use serde::Serialize;
use sqlx::Row;
use tokio::task;
use uuid::Uuid;

use crate::db::DbPool;
use crate::ingestion::{self, FileInput, FileReport, FileStatus, IngestionBatch};
use crate::object_store::ObjectStore;
use crate::pipelines::{all_pipelines, ExecutionContext};

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
        }
    } else {
        match ExecutionContext::load_from_db(pool).await {
            Ok(context) => run_pipeline(&context, &ingestion_batch),
            Err(err) => PipelineSummary {
                pipeline: None,
                status: PipelineStatus::Failed,
                row_count: None,
                error: Some(err.to_string()),
            },
        }
    };

    if !dry_run {
        if let Some(id) = transaction_id {
            if pipeline_summary.status != PipelineStatus::Failed {
                if let Err(err) = upload_new_raw_files(object_store, &files, &ingestion_batch).await
                {
                    pipeline_summary = PipelineSummary {
                        pipeline: pipeline_summary.pipeline.clone(),
                        status: PipelineStatus::Failed,
                        row_count: pipeline_summary.row_count,
                        error: Some(format!("object store upload failed: {err}")),
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
            },
            Err(err) => PipelineSummary {
                pipeline: Some(pipeline.code_identifier().to_string()),
                status: PipelineStatus::Failed,
                row_count: None,
                error: Some(err.to_string()),
            },
        }
    } else {
        PipelineSummary {
            pipeline: None,
            status: PipelineStatus::Skipped,
            row_count: None,
            error: Some("no pipeline registered for data format".into()),
        }
    }
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
