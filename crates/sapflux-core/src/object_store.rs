use anyhow::Result;

#[derive(Debug, Clone, Default)]
pub struct ObjectStore {
    inner: ObjectStoreKind,
}

#[derive(Debug, Clone, Default)]
enum ObjectStoreKind {
    #[default]
    Noop,
}

impl ObjectStore {
    pub fn noop() -> Self {
        ObjectStore {
            inner: ObjectStoreKind::Noop,
        }
    }

    pub async fn put_raw_file(&self, hash: &str, contents: &[u8]) -> Result<()> {
        match &self.inner {
            ObjectStoreKind::Noop => {
                let _ = (hash, contents);
                Ok(())
            }
        }
    }

    pub fn raw_file_key(hash: &str) -> String {
        format!("raw-files/{hash}")
    }
}
