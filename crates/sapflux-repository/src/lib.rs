//! Database repository for caching parsed sap flux files in Postgres.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::migrate::MigrateError;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProcessingStatus {
    Processing,
    Ready,
    Failed,
}

impl ProcessingStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProcessingStatus::Processing => "processing",
            ProcessingStatus::Ready => "ready",
            ProcessingStatus::Failed => "failed",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "processing" => Some(Self::Processing),
            "ready" => Some(Self::Ready),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

impl From<ProcessingStatus> for String {
    fn from(value: ProcessingStatus) -> Self {
        value.as_str().to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProcessedFileRecord {
    pub file_hash: String,
    pub raw_filename: String,
    pub ingested_at: DateTime<Utc>,
    pub processed_at: DateTime<Utc>,
    pub parser_version: String,
    pub artifact_manifest: Value,
    pub warnings: Option<Value>,
    pub status: ProcessingStatus,
    pub idempotency_key: Option<Uuid>,
}

impl ProcessedFileRecord {
    pub fn new(
        file_hash: impl Into<String>,
        raw_filename: impl Into<String>,
        parser_version: impl Into<String>,
        artifact_manifest: Value,
    ) -> Self {
        let now = Utc::now();
        Self {
            file_hash: file_hash.into(),
            raw_filename: raw_filename.into(),
            ingested_at: now,
            processed_at: now,
            parser_version: parser_version.into(),
            artifact_manifest,
            warnings: None,
            status: ProcessingStatus::Processing,
            idempotency_key: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("migration error: {0}")]
    Migration(#[from] MigrateError),

    #[error("invalid status value '{0}'")]
    InvalidStatus(String),

    #[error("record not found for hash '{0}'")]
    NotFound(String),
}

#[async_trait]
pub trait ProcessedFileRepository: Send + Sync {
    async fn upsert(&self, record: &ProcessedFileRecord) -> Result<(), RepositoryError>;
    async fn fetch_by_hash(&self, file_hash: &str) -> Result<ProcessedFileRecord, RepositoryError>;
    async fn update_status(
        &self,
        file_hash: &str,
        status: ProcessingStatus,
        warnings: Option<Value>,
    ) -> Result<(), RepositoryError>;
}

#[derive(Clone)]
pub struct PostgresRepository {
    pool: PgPool,
}

impl PostgresRepository {
    pub async fn connect(
        database_url: &str,
        max_connections: u32,
    ) -> Result<Self, RepositoryError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn run_migrations(&self) -> Result<(), RepositoryError> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }
}

#[async_trait]
impl ProcessedFileRepository for PostgresRepository {
    async fn upsert(&self, record: &ProcessedFileRecord) -> Result<(), RepositoryError> {
        let warnings_json = record.warnings.clone();

        sqlx::query(
            r#"
            INSERT INTO processed_files (
                file_hash,
                raw_filename,
                ingested_at,
                processed_at,
                parser_version,
                artifact_manifest,
                warnings,
                status,
                idempotency_key
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (file_hash) DO UPDATE SET
                raw_filename = EXCLUDED.raw_filename,
                processed_at = EXCLUDED.processed_at,
                parser_version = EXCLUDED.parser_version,
                artifact_manifest = EXCLUDED.artifact_manifest,
                warnings = EXCLUDED.warnings,
                status = EXCLUDED.status,
                idempotency_key = EXCLUDED.idempotency_key
            "#,
        )
        .bind(&record.file_hash)
        .bind(&record.raw_filename)
        .bind(record.ingested_at)
        .bind(record.processed_at)
        .bind(&record.parser_version)
        .bind(record.artifact_manifest.clone())
        .bind(warnings_json)
        .bind(record.status.as_str())
        .bind(record.idempotency_key)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn fetch_by_hash(&self, file_hash: &str) -> Result<ProcessedFileRecord, RepositoryError> {
        let row = sqlx::query(
            r#"
            SELECT
                file_hash,
                raw_filename,
                ingested_at,
                processed_at,
                parser_version,
                artifact_manifest,
                warnings,
                status,
                idempotency_key
            FROM processed_files
            WHERE file_hash = $1
            "#,
        )
        .bind(file_hash)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            let status_str: String = row.try_get("status")?;
            let status = ProcessingStatus::from_str(&status_str)
                .ok_or_else(|| RepositoryError::InvalidStatus(status_str.clone()))?;

            let artifact_manifest: Value = row.try_get("artifact_manifest")?;
            let warnings: Option<Value> = row.try_get("warnings")?;

            Ok(ProcessedFileRecord {
                file_hash: row.try_get("file_hash")?,
                raw_filename: row.try_get("raw_filename")?,
                ingested_at: row.try_get("ingested_at")?,
                processed_at: row.try_get("processed_at")?,
                parser_version: row.try_get("parser_version")?,
                artifact_manifest,
                warnings,
                status,
                idempotency_key: row.try_get("idempotency_key")?,
            })
        } else {
            Err(RepositoryError::NotFound(file_hash.to_string()))
        }
    }

    async fn update_status(
        &self,
        file_hash: &str,
        status: ProcessingStatus,
        warnings: Option<Value>,
    ) -> Result<(), RepositoryError> {
        let result = sqlx::query(
            r#"
            UPDATE processed_files
            SET status = $1,
                processed_at = $2,
                warnings = $3
            WHERE file_hash = $4
            "#,
        )
        .bind(status.as_str())
        .bind(Utc::now())
        .bind(warnings)
        .bind(file_hash)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(RepositoryError::NotFound(file_hash.to_string()));
        }

        Ok(())
    }
}
