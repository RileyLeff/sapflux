//! Abstractions over S3-compatible storage backends used for parsed sap flux artifacts.

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use std::fmt;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub force_path_style: bool,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: "sapflux-parsed".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            force_path_style: false,
        }
    }
}

#[derive(Debug, Error)]
pub enum BucketError {
    #[error("configuration error: {0}")]
    Configuration(String),
    #[error("sdk error: {0}")]
    Sdk(String),
    #[error("object not found: {0}")]
    NotFound(String),
}

impl BucketError {
    fn from_sdk(err: impl fmt::Display) -> Self {
        Self::Sdk(err.to_string())
    }
}

#[async_trait]
pub trait BucketStore: Send + Sync {
    async fn put_object(
        &self,
        key: &str,
        bytes: Bytes,
        content_type: &str,
    ) -> Result<(), BucketError>;
    async fn get_object(&self, key: &str) -> Result<Bytes, BucketError>;
    async fn delete_object(&self, key: &str) -> Result<(), BucketError>;
}

#[derive(Clone)]
pub struct S3BucketStore {
    client: Client,
    bucket: String,
}

impl S3BucketStore {
    pub async fn new(config: S3Config) -> Result<Self, BucketError> {
        if config.bucket.is_empty() {
            return Err(BucketError::Configuration(
                "bucket name cannot be empty".into(),
            ));
        }

        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()));

        if let (Some(access_key), Some(secret_key)) =
            (&config.access_key_id, &config.secret_access_key)
        {
            let credentials = Credentials::new(access_key, secret_key, None, None, "static");
            loader = loader.credentials_provider(SharedCredentialsProvider::new(credentials));
        }

        let shared_config = loader.load().await;
        let mut builder = aws_sdk_s3::config::Builder::from(&shared_config);

        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint_url(endpoint);
        }

        if config.force_path_style {
            builder = builder.force_path_style(true);
        }

        let client = Client::from_conf(builder.build());
        Ok(Self {
            client,
            bucket: config.bucket,
        })
    }
}

#[async_trait]
impl BucketStore for S3BucketStore {
    async fn put_object(
        &self,
        key: &str,
        bytes: Bytes,
        content_type: &str,
    ) -> Result<(), BucketError> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes.to_vec()))
            .content_type(content_type)
            .send()
            .await
            .map_err(BucketError::from_sdk)?;
        Ok(())
    }

    async fn get_object(&self, key: &str) -> Result<Bytes, BucketError> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| match err {
                SdkError::ServiceError(service_err) => {
                    let message = service_err.err().to_string();
                    if message.contains("NoSuchKey") {
                        BucketError::NotFound(key.to_string())
                    } else {
                        BucketError::from_sdk(message)
                    }
                }
                other => BucketError::from_sdk(other),
            })?;

        let data = output.body.collect().await.map_err(BucketError::from_sdk)?;
        Ok(Bytes::from(data.into_bytes()))
    }

    async fn delete_object(&self, key: &str) -> Result<(), BucketError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(BucketError::from_sdk)?;
        Ok(())
    }
}
