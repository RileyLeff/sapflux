use std::io::{Cursor, Write};

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use polars::io::parquet::write::{ParquetCompression, ParquetWriter, StatisticsOptions};
use polars::prelude::DataFrame;
use serde_json::json;
use sqlx::{types::Json, Postgres, Row, Transaction};
use uuid::Uuid;
use zip::write::FileOptions;
use zip::{CompressionMethod, ZipWriter};

use crate::db::DbPool;
use crate::object_store::ObjectStore;
use crate::transactions::{IngestionSummary, PipelineSummary};

/// Resulting identifiers and object-store locations after publishing an output.
pub struct OutputArtifacts {
    pub run_id: Uuid,
    pub output_id: Uuid,
    pub parquet_key: String,
    pub cartridge_key: String,
}

#[derive(Debug, Clone)]
pub struct OutputPaths {
    pub object_store_path: String,
    pub cartridge_path: String,
}

/// Upload the pipeline dataframe and reproducibility cartridge, then upsert run/output rows.
pub async fn publish_output(
    pool: &DbPool,
    store: &ObjectStore,
    pipeline_code: &str,
    dataframe: &DataFrame,
    triggering_transaction: Uuid,
    summary: &PipelineSummary,
    ingestion_summary: &IngestionSummary,
) -> Result<OutputArtifacts> {
    let parquet_bytes =
        create_parquet_bytes(dataframe).context("failed to serialize output parquet")?;
    let cartridge_bytes = create_cartridge(summary, ingestion_summary)
        .context("failed to build reproducibility cartridge")?;

    let output_id = Uuid::new_v4();
    let run_id = Uuid::new_v4();
    let parquet_key = ObjectStore::output_parquet_key(&output_id);
    let cartridge_key = ObjectStore::cartridge_key(&output_id);

    store
        .put_object(&parquet_key, &parquet_bytes)
        .await
        .context("failed to upload output parquet")?;
    store
        .put_object(&cartridge_key, &cartridge_bytes)
        .await
        .context("failed to upload reproducibility cartridge")?;

    let mut tx = pool.begin().await?;
    let pipeline_id = fetch_pipeline_id(&mut tx, pipeline_code).await?;
    insert_run(
        &mut tx,
        run_id,
        pipeline_id,
        triggering_transaction,
        summary,
        ingestion_summary,
    )
    .await?;
    insert_output(
        &mut tx,
        output_id,
        run_id,
        &parquet_key,
        &cartridge_key,
        summary.row_count,
    )
    .await?;
    tx.commit().await?;

    Ok(OutputArtifacts {
        run_id,
        output_id,
        parquet_key,
        cartridge_key,
    })
}

pub async fn fetch_output_paths(pool: &DbPool, output_id: Uuid) -> Result<Option<OutputPaths>> {
    let record = sqlx::query(
        r#"
            SELECT object_store_path, reproducibility_cartridge_path
            FROM outputs
            WHERE output_id = $1
        "#,
    )
    .bind(output_id)
    .fetch_optional(pool)
    .await?;

    if let Some(row) = record {
        let object_store_path: String = row.try_get("object_store_path")?;
        let cartridge_path: String = row.try_get("reproducibility_cartridge_path")?;
        Ok(Some(OutputPaths {
            object_store_path,
            cartridge_path,
        }))
    } else {
        Ok(None)
    }
}

fn create_parquet_bytes(df: &DataFrame) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    {
        let mut cursor = Cursor::new(&mut buffer);
        let mut clone = df.clone();
        ParquetWriter::new(&mut cursor)
            .with_compression(ParquetCompression::Zstd(None))
            .with_statistics(StatisticsOptions::default())
            .finish(&mut clone)
            .context("failed to write parquet to buffer")?;
    }
    Ok(buffer)
}

fn create_cartridge(
    summary: &PipelineSummary,
    ingestion_summary: &IngestionSummary,
) -> Result<Vec<u8>> {
    let metadata = json!({
        "generated_at": Utc::now().to_rfc3339(),
        "pipeline": summary,
        "ingestion": ingestion_summary,
    });

    let metadata_bytes = serde_json::to_vec_pretty(&metadata)?;
    let mut buffer = Vec::new();
    {
        let mut cursor = Cursor::new(&mut buffer);
        let mut zip = ZipWriter::new(&mut cursor);
        let options = FileOptions::default().compression_method(CompressionMethod::Deflated);

        zip.start_file("metadata.json", options)
            .context("failed to start metadata.json in cartridge")?;
        zip.write_all(&metadata_bytes)
            .context("failed to write metadata.json")?;

        zip.start_file("README.txt", options)
            .context("failed to start README.txt in cartridge")?;
        zip.write_all(
            b"Sapflux reproducibility cartridge placeholder. Attach downstream scripts here.",
        )
        .context("failed to write README.txt")?;

        zip.finish()
            .context("failed to finalize cartridge archive")?;
    }
    Ok(buffer)
}

async fn fetch_pipeline_id(tx: &mut Transaction<'_, Postgres>, code: &str) -> Result<Uuid> {
    sqlx::query_scalar(
        r#"
            SELECT pipeline_id
            FROM processing_pipelines
            WHERE code_identifier = $1
        "#,
    )
    .bind(code)
    .fetch_optional(tx.as_mut())
    .await?
    .ok_or_else(|| anyhow!("processing pipeline '{}' not found", code))
}

async fn insert_run(
    tx: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    pipeline_id: Uuid,
    triggering_transaction: Uuid,
    summary: &PipelineSummary,
    ingestion_summary: &IngestionSummary,
) -> Result<()> {
    let git_hash =
        std::env::var("SAPFLUX_GIT_COMMIT_HASH").unwrap_or_else(|_| "unknown".to_string());
    let run_log = json!({
        "pipeline_summary": summary,
        "ingestion_summary": ingestion_summary,
    });

    sqlx::query(
        r#"
            INSERT INTO runs (
                run_id,
                triggering_transaction_id,
                processing_pipeline_id,
                finished_at,
                status,
                git_commit_hash,
                run_log
            )
            VALUES ($1, $2, $3, NOW(), 'SUCCESS', $4, $5)
        "#,
    )
    .bind(run_id)
    .bind(triggering_transaction)
    .bind(pipeline_id)
    .bind(git_hash)
    .bind(Json(run_log))
    .execute(tx.as_mut())
    .await?;

    Ok(())
}

async fn insert_output(
    tx: &mut Transaction<'_, Postgres>,
    output_id: Uuid,
    run_id: Uuid,
    parquet_key: &str,
    cartridge_key: &str,
    row_count: Option<usize>,
) -> Result<()> {
    sqlx::query("UPDATE outputs SET is_latest = FALSE WHERE is_latest = TRUE")
        .execute(tx.as_mut())
        .await?;

    sqlx::query(
        r#"
            INSERT INTO outputs (
                output_id,
                run_id,
                object_store_path,
                reproducibility_cartridge_path,
                row_count,
                is_latest
            ) VALUES ($1, $2, $3, $4, $5, TRUE)
        "#,
    )
    .bind(output_id)
    .bind(run_id)
    .bind(parquet_key)
    .bind(cartridge_key)
    .bind(row_count.map(|value| value as i32))
    .execute(tx.as_mut())
    .await?;

    Ok(())
}
