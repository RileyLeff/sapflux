use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use sapflux_core::object_store::ObjectStore;
use uuid::Uuid;

const REQUIRED_VARS: &[&str] = &[
    "SAPFLUX_TEST_S3_BUCKET",
    "SAPFLUX_TEST_S3_ENDPOINT",
    "SAPFLUX_TEST_S3_ACCESS_KEY_ID",
    "SAPFLUX_TEST_S3_SECRET_ACCESS_KEY",
];

#[tokio::test]
async fn s3_object_store_upload_and_presign() -> Result<()> {
    let Some(config) = S3TestConfig::from_env() else {
        eprintln!(
            "Skipping S3 object store test; set {} to enable",
            REQUIRED_VARS.join(", ")
        );
        return Ok(());
    };

    let _guard = EnvGuard::apply(&config)?;

    let store = ObjectStore::from_env_async()
        .await
        .context("failed to build S3 object store")?;
    let key = ObjectStore::raw_file_key(&Uuid::new_v4().to_string());
    let payload = b"sapflux integration test";

    store
        .put_raw_file(&key, payload)
        .await
        .context("upload to S3 failed")?;

    let listed = store
        .list_prefix("raw-files/")
        .await
        .context("list prefix failed")?;
    assert!(listed.iter().any(|entry| entry == &key));

    let presigned = store
        .presign_get(&key, Duration::from_secs(60))
        .await
        .context("presign failed")?;
    assert!(presigned.is_some());

    store.delete(&key).await.context("cleanup delete failed")?;

    Ok(())
}

struct EnvGuard {
    previous: HashMap<&'static str, Option<String>>,
}

impl EnvGuard {
    fn apply(config: &S3TestConfig) -> Result<Self> {
        let mut previous = HashMap::new();

        let mut set = |key: &'static str, value: Option<&str>| {
            let prior = std::env::var(key).ok();
            previous.insert(key, prior);
            if let Some(new) = value {
                std::env::set_var(key, new);
            } else {
                std::env::remove_var(key);
            }
        };

        set("SAPFLUX_OBJECT_STORE_KIND", Some("s3"));
        set("S3_BUCKET", Some(&config.bucket));
        set("S3_REGION", config.region.as_deref());
        set("S3_ENDPOINT_URL", Some(&config.endpoint));
        set("S3_ACCESS_KEY_ID", Some(&config.access_key_id));
        set("S3_SECRET_ACCESS_KEY", Some(&config.secret_access_key));
        set("S3_SESSION_TOKEN", config.session_token.as_deref());
        set("S3_FORCE_PATH_STYLE", config.force_path_style.as_deref());

        Ok(Self { previous })
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (&key, value) in &self.previous {
            if let Some(val) = value {
                std::env::set_var(key, val);
            } else {
                std::env::remove_var(key);
            }
        }
    }
}

struct S3TestConfig {
    bucket: String,
    region: Option<String>,
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    force_path_style: Option<String>,
}

impl S3TestConfig {
    fn from_env() -> Option<Self> {
        for &var in REQUIRED_VARS {
            if std::env::var(var)
                .ok()
                .filter(|value| !value.is_empty())
                .is_none()
            {
                return None;
            }
        }

        Some(Self {
            bucket: std::env::var("SAPFLUX_TEST_S3_BUCKET").ok()?,
            region: std::env::var("SAPFLUX_TEST_S3_REGION").ok(),
            endpoint: std::env::var("SAPFLUX_TEST_S3_ENDPOINT").ok()?,
            access_key_id: std::env::var("SAPFLUX_TEST_S3_ACCESS_KEY_ID").ok()?,
            secret_access_key: std::env::var("SAPFLUX_TEST_S3_SECRET_ACCESS_KEY").ok()?,
            session_token: std::env::var("SAPFLUX_TEST_S3_SESSION_TOKEN").ok(),
            force_path_style: std::env::var("SAPFLUX_TEST_S3_FORCE_PATH_STYLE").ok(),
        })
    }
}
