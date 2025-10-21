use anyhow::{anyhow, Context, Result};
use std::path::PathBuf;
use uuid::Uuid;

#[cfg(feature = "runtime")]
use std::time::Duration;

#[cfg(feature = "runtime")]
use std::path::Path;

#[cfg(feature = "runtime")]
use aws_config::BehaviorVersion;
#[cfg(feature = "runtime")]
use aws_credential_types::Credentials;
#[cfg(feature = "runtime")]
use aws_sdk_s3::{
    config::Builder as S3ConfigBuilder,
    error::SdkError,
    operation::{
        delete_object::DeleteObjectError, head_object::HeadObjectError,
        list_objects_v2::ListObjectsV2Error, put_object::PutObjectError,
    },
    presigning::PresigningConfig,
    Client as S3Client,
};
#[cfg(feature = "runtime")]
use tokio::fs;
#[cfg(feature = "runtime")]
use tracing::warn;

/// Simple object-store abstraction used by the transaction orchestrator.
#[derive(Debug, Clone)]
pub enum ObjectStore {
    /// No-op store used when no configuration is provided.
    Noop,
    /// Store that writes objects to a local filesystem directory (content-addressed).
    LocalDir(LocalDirStore),
    #[cfg(feature = "runtime")]
    /// Store backed by an S3-compatible object store (R2/MinIO/AWS).
    S3(S3Store),
}

#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "runtime"), allow(dead_code))]
pub struct LocalDirStore {
    root: PathBuf,
}

#[cfg(feature = "runtime")]
#[derive(Clone)]
pub struct S3Store {
    client: S3Client,
    bucket: String,
}

impl ObjectStore {
    /// Construct an object store from environment variables or fall back to a no-op store.
    pub fn from_env() -> Result<Self> {
        let (kind, dir_env) = Self::env_kind_and_dir();

        match kind.as_str() {
            "noop" => Ok(Self::noop()),
            "local" => {
                let dir = dir_env.clone().ok_or_else(|| {
                    anyhow!("SAPFLUX_OBJECT_STORE_DIR must be set for local store")
                })?;
                Ok(Self::LocalDir(LocalDirStore::new(dir.into())?))
            }
            "s3" => {
                #[cfg(feature = "runtime")]
                {
                    Err(anyhow!(
                        "S3 object store requires async initialization; call ObjectStore::from_env_async().await"
                    ))
                }
                #[cfg(not(feature = "runtime"))]
                {
                    Err(anyhow!(
                        "S3 object store requested but sapflux-core built without runtime feature"
                    ))
                }
            }
            other => Err(anyhow!(
                "unsupported SAPFLUX_OBJECT_STORE_KIND '{}'; expected noop, local, or s3",
                other
            )),
        }
    }

    #[cfg(feature = "runtime")]
    /// Async constructor that supports S3 initialization inside an existing Tokio runtime.
    pub async fn from_env_async() -> Result<Self> {
        let (kind, dir_env) = Self::env_kind_and_dir();

        match kind.as_str() {
            "noop" => Ok(Self::noop()),
            "local" => {
                let dir = dir_env.ok_or_else(|| {
                    anyhow!("SAPFLUX_OBJECT_STORE_DIR must be set for local store")
                })?;
                Ok(Self::LocalDir(LocalDirStore::new(dir.into())?))
            }
            "s3" => {
                let store = S3Store::from_env_async().await?;
                Ok(Self::S3(store))
            }
            other => Err(anyhow!(
                "unsupported SAPFLUX_OBJECT_STORE_KIND '{}'; expected noop, local, or s3",
                other
            )),
        }
    }

    /// Explicitly construct a no-op store.
    pub fn noop() -> Self {
        ObjectStore::Noop
    }

    /// Deterministic key used for raw file uploads.
    pub fn raw_file_key(hash: &str) -> String {
        format!("raw-files/{hash}")
    }

    /// Key used for storing output parquet files.
    pub fn output_parquet_key(output_id: &Uuid) -> String {
        format!("outputs/{output_id}.parquet")
    }

    /// Key used for storing reproducibility cartridges.
    pub fn cartridge_key(output_id: &Uuid) -> String {
        format!("repro-cartridges/{output_id}.zip")
    }

    #[cfg(feature = "runtime")]
    pub async fn put_raw_file(&self, key: &str, contents: &[u8]) -> Result<()> {
        self.put_object(key, contents).await
    }

    #[cfg(not(feature = "runtime"))]
    pub async fn put_raw_file(&self, _key: &str, _contents: &[u8]) -> Result<()> {
        Ok(())
    }

    #[cfg(feature = "runtime")]
    pub async fn put_object(&self, key: &str, contents: &[u8]) -> Result<()> {
        match self {
            ObjectStore::Noop => Ok(()),
            ObjectStore::LocalDir(store) => store.put(key, contents).await,
            ObjectStore::S3(store) => store.put(key, contents).await,
        }
    }

    #[cfg(not(feature = "runtime"))]
    pub async fn put_object(&self, _key: &str, _contents: &[u8]) -> Result<()> {
        Ok(())
    }

    #[cfg(feature = "runtime")]
    pub async fn presign_get(&self, key: &str, expires: Duration) -> Result<Option<String>> {
        match self {
            ObjectStore::Noop | ObjectStore::LocalDir(_) => Ok(None),
            ObjectStore::S3(store) => store.presign_get(key, expires).await,
        }
    }

    #[cfg(feature = "runtime")]
    pub async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        match self {
            ObjectStore::Noop => Ok(Vec::new()),
            ObjectStore::LocalDir(store) => store.list(prefix).await,
            ObjectStore::S3(store) => store.list(prefix).await,
        }
    }

    #[cfg(feature = "runtime")]
    pub async fn delete(&self, key: &str) -> Result<()> {
        match self {
            ObjectStore::Noop => Ok(()),
            ObjectStore::LocalDir(store) => store.delete(key).await,
            ObjectStore::S3(store) => store.delete(key).await,
        }
    }

    fn env_kind_and_dir() -> (String, Option<String>) {
        let kind_env = std::env::var("SAPFLUX_OBJECT_STORE_KIND").ok();
        let dir_env = std::env::var("SAPFLUX_OBJECT_STORE_DIR").ok();

        let kind = kind_env
            .as_ref()
            .map(|s| s.to_lowercase())
            .or_else(|| dir_env.as_ref().map(|_| "local".to_string()))
            .unwrap_or_else(|| "noop".to_string());

        (kind, dir_env)
    }
}

impl LocalDirStore {
    fn new(root: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root).with_context(|| {
            format!("failed to create object store directory {}", root.display())
        })?;
        Ok(Self { root })
    }

    #[cfg(feature = "runtime")]
    async fn put(&self, key: &str, contents: &[u8]) -> Result<()> {
        let path = self.root.join(Path::new(key));
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }

        if fs::metadata(&path).await.is_ok() {
            return Ok(());
        }

        fs::write(&path, contents).await?;
        Ok(())
    }

    #[cfg(feature = "runtime")]
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let root = self.root.clone();
        let prefix = prefix.trim_start_matches('/').to_string();
        tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            let base = if prefix.is_empty() {
                root.clone()
            } else {
                root.join(&prefix)
            };

            if !base.exists() {
                return Ok(Vec::new());
            }

            let mut stack = vec![base];
            let mut keys = Vec::new();
            while let Some(path) = stack.pop() {
                if path.is_dir() {
                    for entry in std::fs::read_dir(&path)? {
                        let entry = entry?;
                        stack.push(entry.path());
                    }
                } else if let Ok(rel) = path.strip_prefix(&root) {
                    let key = rel
                        .components()
                        .map(|comp| comp.as_os_str().to_string_lossy())
                        .collect::<Vec<_>>()
                        .join("/");
                    keys.push(key);
                }
            }

            Ok(keys)
        })
        .await?
    }

    #[cfg(feature = "runtime")]
    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.root.join(Path::new(key));
        if fs::metadata(&path).await.is_ok() {
            if let Err(err) = fs::remove_file(&path).await {
                warn!(%key, "failed to remove local object: {err}");
            }
        }
        Ok(())
    }
}

#[cfg(feature = "runtime")]
impl std::fmt::Debug for S3Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Store")
            .field("bucket", &self.bucket)
            .finish()
    }
}

#[cfg(feature = "runtime")]
impl S3Store {
    pub async fn from_env_async() -> Result<Self> {
        let config = S3Config::from_env()?;
        Self::from_config(config).await
    }

    async fn from_config(config: S3Config) -> Result<Self> {
        let creds = Credentials::new(
            config.access_key_id,
            config.secret_access_key,
            config.session_token,
            None,
            "sapflux",
        );

        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(config.region.clone()))
            .credentials_provider(creds);

        if let Some(endpoint) = &config.endpoint_url {
            loader = loader.endpoint_url(endpoint.clone());
        }

        let base_config = loader.load().await;
        let mut builder = S3ConfigBuilder::from(&base_config);
        builder = builder.force_path_style(config.force_path_style);
        if let Some(endpoint) = config.endpoint_url {
            builder = builder.endpoint_url(endpoint);
        }

        let client = S3Client::from_conf(builder.build());
        Ok(Self {
            client,
            bucket: config.bucket,
        })
    }

    async fn put(&self, key: &str, contents: &[u8]) -> Result<()> {
        if self.object_exists(key).await? {
            return Ok(());
        }

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(contents.to_vec().into())
            .send()
            .await
            .map(|_| ())
            .map_err(|err| map_put_err(err, key))
    }

    async fn presign_get(&self, key: &str, expires: Duration) -> Result<Option<String>> {
        if !self.object_exists(key).await? {
            return Ok(None);
        }

        let config = PresigningConfig::builder()
            .expires_in(expires)
            .build()
            .context("failed to build presigning config")?;

        let request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(config)
            .await
            .context("failed to presign S3 GET request")?;

        Ok(Some(request.uri().to_string()))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);

            if let Some(token) = continuation.take() {
                request = request.continuation_token(token);
            }

            let response = request
                .send()
                .await
                .map_err(|err| map_list_err(err, prefix))?;

            let next_token = response
                .next_continuation_token()
                .map(|value| value.to_string());

            if let Some(contents) = response.contents {
                for object in contents {
                    if let Some(key) = object.key() {
                        keys.push(key.to_string());
                    }
                }
            }

            if let Some(next_token) = next_token {
                continuation = Some(next_token);
            } else {
                break;
            }
        }

        Ok(keys)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map(|_| ())
            .map_err(|err| map_delete_err(err, key))
    }

    async fn object_exists(&self, key: &str) -> Result<bool> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(service_err)) if service_err.err().is_not_found() => {
                Ok(false)
            }
            Err(err) => Err(map_head_err(err, key)),
        }
    }
}

#[cfg(feature = "runtime")]
fn parse_bool(value: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" => Ok(true),
        "0" | "false" | "no" | "n" => Ok(false),
        other => Err(anyhow!("invalid boolean value '{other}'")),
    }
}

#[cfg(feature = "runtime")]
struct S3Config {
    bucket: String,
    region: String,
    endpoint_url: Option<String>,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    force_path_style: bool,
}

#[cfg(feature = "runtime")]
impl S3Config {
    fn from_env() -> Result<Self> {
        let bucket = std::env::var("S3_BUCKET")
            .context("S3_BUCKET must be set when SAPFLUX_OBJECT_STORE_KIND=s3")?;
        let region = std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let endpoint_url = std::env::var("S3_ENDPOINT_URL").ok();
        let access_key_id = std::env::var("S3_ACCESS_KEY_ID")
            .context("S3_ACCESS_KEY_ID must be set when SAPFLUX_OBJECT_STORE_KIND=s3")?;
        let secret_access_key = std::env::var("S3_SECRET_ACCESS_KEY")
            .context("S3_SECRET_ACCESS_KEY must be set when SAPFLUX_OBJECT_STORE_KIND=s3")?;
        let session_token = std::env::var("S3_SESSION_TOKEN").ok();
        let force_path_style = match std::env::var("S3_FORCE_PATH_STYLE") {
            Ok(value) => parse_bool(&value)?,
            Err(_) => false,
        };

        Ok(Self {
            bucket,
            region,
            endpoint_url,
            access_key_id,
            secret_access_key,
            session_token,
            force_path_style,
        })
    }
}

#[cfg(feature = "runtime")]
fn map_head_err(err: SdkError<HeadObjectError>, key: &str) -> anyhow::Error {
    anyhow!("failed to check existence of '{key}' in object store: {err:?}")
}

#[cfg(feature = "runtime")]
fn map_put_err(err: SdkError<PutObjectError>, key: &str) -> anyhow::Error {
    anyhow!("failed to upload '{key}' to object store: {err:?}")
}

#[cfg(feature = "runtime")]
fn map_list_err(err: SdkError<ListObjectsV2Error>, prefix: &str) -> anyhow::Error {
    anyhow!("failed to list objects under prefix '{prefix}': {err:?}")
}

#[cfg(feature = "runtime")]
fn map_delete_err(err: SdkError<DeleteObjectError>, key: &str) -> anyhow::Error {
    anyhow!("failed to delete '{key}' from object store: {err:?}")
}
