use std::sync::Arc;

use chrono::Utc;
use polars::prelude::DataFrame;
use sapflux_bucket::{BucketStore, S3BucketStore, S3Config};
use sapflux_parser::{parse_sapflow_file, ParsedFileData};
use sapflux_repository::metadata::{MetadataRepository, ParserConfigRecord};
use sapflux_repository::{ProcessedFileRecord, ProcessedFileRepository, ProcessingStatus};
use sapflux_repository::PostgresRepository;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub bucket: S3Config,
    pub parser_config_id: Uuid,
    pub parser_config_version: String,
}

#[derive(Debug, Error)]
pub enum ProcessingError {
    #[error("repository error: {0}")]
    Repository(#[from] sapflux_repository::RepositoryError),
    #[error("metadata error: {0}")]
    Metadata(#[from] sapflux_repository::metadata::MetadataError),
    #[error("bucket error: {0}")]
    Bucket(#[from] sapflux_bucket::BucketError),
    #[error("parser error: {0}")]
    Parser(String),
    #[error("processing stage failed: {0}")]
    Stage(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowResult {
    pub dataset_version_id: Uuid,
    pub manifest_key: String,
    pub parquet_key: String,
}

pub struct ProcessingPipeline {
    repository: Arc<PostgresRepository>,
    metadata_repo: Arc<MetadataRepository>,
    bucket: Arc<dyn BucketStore + Send + Sync>,
    config: PipelineConfig,
}

impl ProcessingPipeline {
    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    pub async fn new(
        repository: Arc<PostgresRepository>,
        metadata_repo: Arc<MetadataRepository>,
        config: PipelineConfig,
    ) -> Result<Self, ProcessingError> {
        let bucket_store = S3BucketStore::new(config.bucket.clone()).await?;
        Ok(Self {
            repository,
            metadata_repo,
            bucket: Arc::new(bucket_store),
            config,
        })
    }

    pub async fn run(&self, raw_text: &str, file_hash: &str, raw_filename: &str) -> Result<WorkflowResult, ProcessingError> {
        // 1. mark processing state
        let mut record = ProcessedFileRecord::new(
            file_hash.to_string(),
            raw_filename,
            self.config.parser_config_version.clone(),
            json!({ "status": "processing" }),
        );
        record.status = ProcessingStatus::Processing;
        self.repository.upsert(&record).await?;

        // 2. fetch parser config (for future use)
        let _parser_config: ParserConfigRecord = self
            .metadata_repo
            .fetch_parser_config(self.config.parser_config_id)
            .await?;

        // 3. parse raw data (placeholder
        let parsed = parse_sapflow_file(raw_text).map_err(|err| ProcessingError::Parser(err.to_string()))?;

        // 4. timestamp fixer stub
        let parsed = self.timestamp_fix(parsed).await?;

        // 5. metadata join stub
        let parsed = self.attach_metadata(parsed).await?;

        // 6. calculations stub
        let processed_df = self.calculate(parsed).await?;

        // 7. publish dataset (stub)
        let workflow_result = self.publish(processed_df).await?;

        // 8. final status update
        record.status = ProcessingStatus::Ready;
        record.processed_at = Utc::now();
        record.artifact_manifest = json!({
            "manifest_key": workflow_result.manifest_key,
            "parquet_key": workflow_result.parquet_key,
        });
        self.repository.upsert(&record).await?;

        Ok(workflow_result)
    }

    async fn timestamp_fix(&self, parsed: ParsedFileData) -> Result<ParsedFileData, ProcessingError> {
        // TODO: implement real timestamp fixing
        Ok(parsed)
    }

    async fn attach_metadata(&self, parsed: ParsedFileData) -> Result<ParsedFileData, ProcessingError> {
        // TODO: join metadata from repository tables
        Ok(parsed)
    }

    async fn calculate(&self, parsed: ParsedFileData) -> Result<DataFrame, ProcessingError> {
        // TODO: run calculations, currently serializes logger df as placeholder
        Ok(parsed.logger.df)
    }

    async fn publish(&self, _dataframe: DataFrame) -> Result<WorkflowResult, ProcessingError> {
        // TODO: serialize DataFrame to Parquet/manifest and upload
        let dataset_version_id = Uuid::new_v4();
        let manifest_key = format!("datasets/{}/manifest.json", dataset_version_id);
        let parquet_key = format!("datasets/{}/data.parquet", dataset_version_id);

        // Placeholder: store empty files for now
        self
            .bucket
            .put_object(&manifest_key, Bytes::from_static(b"{}"), "application/json")
            .await?;
        self
            .bucket
            .put_object(&parquet_key, Bytes::from_static(b""), "application/octet-stream")
            .await?;

        Ok(WorkflowResult {
            dataset_version_id,
            manifest_key,
            parquet_key,
        })
    }
}

use bytes::Bytes;
