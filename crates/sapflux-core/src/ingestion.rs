// crates/sapflux-core/src/ingestion.rs

use crate::error::{PipelineError, Result};
use crate::types::FileSchema;
use sha2::{Digest, Sha256};
use sqlx::PgPool;

/// Detects the schema of a file based on its header content.
/// A real implementation would be more robust, but this is a great start.
fn detect_schema(file_content: &[u8]) -> Result<FileSchema> {
    // Use the csv crate to read from the in-memory byte slice.
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(file_content);

    // Read the second line (the headers).
    let headers = reader.records().nth(1).ok_or_else(|| {
        PipelineError::Processing("File has less than two lines.".to_string())
    })??;

    // Check for a tell-tale sign of the multi-sensor format.
    let has_multi_sensor_col = headers.iter().any(|h| h.contains("S1_"));
    if has_multi_sensor_col {
        Ok(FileSchema::CR300MultiSensor)
    } else {
        Ok(FileSchema::CRLegacySingleSensor)
    }
}

/// Ingests a single file's content into the database.
///
/// This function is idempotent: if a file with the same content (hash)
/// already exists, it will do nothing and return success.
///
/// # Arguments
/// * `db_pool`: A connection pool to the database.
/// * `file_content`: The raw bytes of the file to ingest.
///
/// # Returns
/// The database `id` of the ingested (or pre-existing) file.
pub async fn ingest_file(db_pool: &PgPool, file_content: &[u8]) -> Result<i64> {
    // 1. Calculate the SHA-256 hash of the file content.
    let hash = Sha256::digest(file_content);
    let hash_hex = format!("{:x}", hash);

    // 2. Check if this hash already exists in the database.
    let existing_id: Option<i64> = sqlx::query_scalar("SELECT id FROM raw_files WHERE file_hash = $1")
        .bind(&hash_hex)
        .fetch_optional(db_pool)
        .await?;

    if let Some(id) = existing_id {
        println!("  -> File with hash {} already exists with id {}. Skipping.", &hash_hex[..8], id);
        return Ok(id);
    }

    // 3. If it's a new file, detect its schema.
    let schema = detect_schema(file_content)?;
    println!("  -> New file detected. Schema: {:?}", schema);

    // 4. Insert the new file record into the database.
    let new_id: i64 = sqlx::query_scalar(
        "INSERT INTO raw_files (file_hash, file_content, detected_schema_name)
         VALUES ($1, $2, $3)
         RETURNING id"
    )
    .bind(&hash_hex)
    .bind(file_content)
    .bind(&schema) // Our custom type impl allows sqlx to handle this.
    .fetch_one(db_pool)
    .await?;

    println!("  -> Successfully ingested as new file with id {}.", new_id);
    Ok(new_id)
}