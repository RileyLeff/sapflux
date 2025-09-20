use std::sync::Arc;

use chrono::Utc;
use sapflux_bucket::S3Config;
use sapflux_processing::{PipelineConfig, ProcessingPipeline, WorkflowResult};
use sapflux_repository::metadata::MetadataRepository;
use sapflux_repository::{PostgresRepository, ProcessedFileRecord, ProcessedFileRepository, ProcessingStatus};
use serde_json::json;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Clone)]
pub struct PipelineState {
    pipeline: Arc<ProcessingPipeline>,
    repository: Arc<PostgresRepository>,
}

impl PipelineState {
    pub async fn new(
        database_url: &str,
        bucket_config: S3Config,
        parser_config_id: Uuid,
        parser_version: String,
    ) -> anyhow::Result<Arc<Mutex<Self>>> {
        let repository = Arc::new(PostgresRepository::connect(database_url, 5).await?);
        repository.run_migrations().await?;

        let metadata_repo = Arc::new(MetadataRepository::new(repository.pool().clone()));

        let pipeline_config = PipelineConfig {
            bucket: bucket_config,
            parser_config_id,
            parser_config_version: parser_version,
        };

        let pipeline = Arc::new(ProcessingPipeline::new(repository.clone(), metadata_repo, pipeline_config).await?);

        Ok(Arc::new(Mutex::new(Self { pipeline, repository })))
    }

    pub async fn run(&self, raw_text: &str, file_hash: &str, raw_filename: &str) -> Result<WorkflowResult, sapflux_processing::ProcessingError> {
        let mut record = ProcessedFileRecord::new(
            file_hash.to_string(),
            raw_filename,
            self.pipeline.config().parser_config_version.clone(),
            json!({"status": "processing"}),
        );
        record.status = ProcessingStatus::Processing;
        self.repository.upsert(&record).await?;

        match self
            .pipeline
            .run(raw_text, file_hash, raw_filename)
            .await
        {
            Ok(result) => {
                record.status = ProcessingStatus::Ready;
                record.processed_at = Utc::now();
                record.artifact_manifest = json!({
                    "manifest_key": result.manifest_key,
                    "parquet_key": result.parquet_key,
                });
                self.repository.upsert(&record).await?;
                Ok(result)
            }
            Err(err) => {
                let _ = self
                    .repository
                    .update_status(
                        file_hash,
                        ProcessingStatus::Failed,
                        Some(json!({ "error": err.to_string() })),
                    )
                    .await;
                Err(err)
            }
        }
    }

    pub fn repository(&self) -> &Arc<PostgresRepository> {
        &self.repository
    }

    pub fn pipeline(&self) -> &Arc<ProcessingPipeline> {
        &self.pipeline
    }
}
