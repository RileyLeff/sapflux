use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

/// Simple object-store abstraction used by the transaction orchestrator.
#[derive(Debug, Clone)]
pub enum ObjectStore {
    /// No-op store used when no configuration is provided.
    Noop,
    /// Store that writes objects to a local filesystem directory (content-addressed).
    LocalDir(LocalDirStore),
}

#[derive(Debug, Clone)]
pub(crate) struct LocalDirStore {
    root: PathBuf,
}

impl ObjectStore {
    /// Construct an object store from environment variables or fall back to a no-op store.
    pub fn from_env() -> Result<Self> {
        match std::env::var("SAPFLUX_OBJECT_STORE_DIR") {
            Ok(path) => Ok(Self::LocalDir(LocalDirStore::new(path.into())?)),
            Err(_) => Ok(Self::noop()),
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

    #[cfg(feature = "runtime")]
    pub async fn put_raw_file(&self, key: &str, contents: &[u8]) -> Result<()> {
        match self {
            ObjectStore::Noop => Ok(()),
            ObjectStore::LocalDir(store) => store.put(key, contents).await,
        }
    }

    #[cfg(not(feature = "runtime"))]
    pub async fn put_raw_file(&self, _key: &str, _contents: &[u8]) -> Result<()> {
        Ok(())
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
        use tokio::fs;

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
}
