#![cfg(feature = "runtime")]

use std::collections::HashSet;

use anyhow::{anyhow, Result};
use serde::Serialize;
use sqlx::Row;
use uuid::Uuid;

use crate::db::DbPool;
use crate::ingestion::{self, FileInput, FileReport, IngestionBatch};
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

#[derive(Debug, Clone, Serialize)]
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
    pub pipeline: PipelineSummary,
}

pub async fn execute_transaction(
    pool: &DbPool,
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

    let pipeline_summary = if ingestion_batch.parsed.is_empty() {
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
            let outcome = match pipeline_summary.status {
                PipelineStatus::Failed => "REJECTED",
                _ => "ACCEPTED",
            };

            if outcome == "ACCEPTED" {
                persist_ingestion(pool, id, &ingestion_batch.new_hashes).await?;
            }

            let receipt = TransactionReceipt {
                transaction_id: Some(id),
                dry_run,
                files: ingestion_batch.reports.clone(),
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
    hashes: &[String],
) -> Result<()> {
    let mut db_tx = pool.begin().await?;

    for hash in hashes {
        sqlx::query(
            r#"
                INSERT INTO raw_files (file_hash, ingesting_transaction_id, ingest_context, include_in_pipeline)
                VALUES ($1, $2, NULL, TRUE)
                ON CONFLICT (file_hash) DO NOTHING
            "#,
        )
        .bind(hash)
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
