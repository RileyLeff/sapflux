#[cfg(feature = "runtime")]
use std::collections::HashSet;

#[cfg(feature = "runtime")]
use anyhow::{Context, Result};
#[cfg(feature = "runtime")]
use sqlx::Row;

#[cfg(feature = "runtime")]
use crate::{db::DbPool, object_store::ObjectStore};

#[cfg(feature = "runtime")]
const RAW_FILES_PREFIX: &str = "raw-files/";
#[cfg(feature = "runtime")]
const OUTPUTS_PREFIX: &str = "outputs/";
#[cfg(feature = "runtime")]
const CARTRIDGES_PREFIX: &str = "repro-cartridges/";

#[cfg(feature = "runtime")]
#[derive(Debug, Clone)]
pub struct GcPlanEntry {
    pub prefix: &'static str,
    pub orphaned: Vec<String>,
}

#[cfg(feature = "runtime")]
#[derive(Debug, Clone)]
pub struct GcReport {
    pub entries: Vec<GcPlanEntry>,
}

#[cfg(feature = "runtime")]
impl GcReport {
    pub fn total_orphaned(&self) -> usize {
        self.entries.iter().map(|entry| entry.orphaned.len()).sum()
    }
}

#[cfg(feature = "runtime")]
pub async fn plan_gc(pool: &DbPool, store: &ObjectStore) -> Result<GcReport> {
    let raw_existing = store.list_prefix(RAW_FILES_PREFIX).await?;
    let output_existing = store.list_prefix(OUTPUTS_PREFIX).await?;
    let cartridge_existing = store.list_prefix(CARTRIDGES_PREFIX).await?;

    let raw_referenced = load_raw_file_keys(pool).await?;
    let (output_referenced, cartridge_referenced) = load_output_keys(pool).await?;

    let entries = vec![
        GcPlanEntry {
            prefix: RAW_FILES_PREFIX,
            orphaned: diff(&raw_existing, &raw_referenced),
        },
        GcPlanEntry {
            prefix: OUTPUTS_PREFIX,
            orphaned: diff(&output_existing, &output_referenced),
        },
        GcPlanEntry {
            prefix: CARTRIDGES_PREFIX,
            orphaned: diff(&cartridge_existing, &cartridge_referenced),
        },
    ];

    Ok(GcReport { entries })
}

#[cfg(feature = "runtime")]
pub async fn apply_gc(store: &ObjectStore, report: &GcReport) -> Result<()> {
    for entry in &report.entries {
        for key in &entry.orphaned {
            store
                .delete(key)
                .await
                .with_context(|| format!("failed to delete orphaned object '{}'", key))?;
        }
    }

    Ok(())
}

#[cfg(feature = "runtime")]
fn diff(existing: &[String], referenced: &HashSet<String>) -> Vec<String> {
    existing
        .iter()
        .filter(|key| !referenced.contains(*key))
        .cloned()
        .collect()
}

#[cfg(feature = "runtime")]
async fn load_raw_file_keys(pool: &DbPool) -> Result<HashSet<String>> {
    let hashes = sqlx::query("SELECT file_hash FROM raw_files")
        .fetch_all(pool)
        .await?;

    let mut keys = HashSet::with_capacity(hashes.len());
    for row in hashes {
        let hash: String = row.try_get("file_hash")?;
        keys.insert(ObjectStore::raw_file_key(&hash));
    }

    Ok(keys)
}

#[cfg(feature = "runtime")]
async fn load_output_keys(pool: &DbPool) -> Result<(HashSet<String>, HashSet<String>)> {
    let rows = sqlx::query(
        r#"
            SELECT object_store_path, reproducibility_cartridge_path
            FROM outputs
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut outputs = HashSet::new();
    let mut cartridges = HashSet::new();

    for row in rows {
        let object_path: Option<String> = row.try_get("object_store_path")?;
        if let Some(path) = object_path {
            outputs.insert(path);
        }

        let cartridge_path: Option<String> = row.try_get("reproducibility_cartridge_path")?;
        if let Some(path) = cartridge_path {
            cartridges.insert(path);
        }
    }

    Ok((outputs, cartridges))
}
