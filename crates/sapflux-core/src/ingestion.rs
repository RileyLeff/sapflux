// crates/sapflux-core/src/ingestion.rs

use crate::error::{PipelineError, Result};
use crate::types::FileSchema;
use crate::validation::{LegacySingleSensorValidator, CR300MultiSensorValidator, SchemaValidator}; // Import our new tools
use sha2::{Digest, Sha256};
use sqlx::PgPool;

/// Attempts to validate the file against a series of known schemas.
/// Returns the first schema that successfully validates.
fn validate_and_identify_schema(file_content: &[u8]) -> Result<FileSchema> {
    // Create a list of all validators to try.
    // The order matters if a file could potentially match multiple schemas.
    let validators: Vec<Box<dyn SchemaValidator>> = vec![
        Box::new(LegacySingleSensorValidator),
        Box::new(CR300MultiSensorValidator)
        // Add other validators here=
    ];

    for validator in validators {
        if validator.validate(file_content).is_ok() {
            // As soon as one validator passes, we've found our schema.
            return Ok(validator.schema());
        }
    }

    // If no validators passed, the file is not of a known valid format.
    Err(PipelineError::Processing(
        "File did not match any known valid schema.".to_string(),
    ))
}


/// Ingests a single file's content into the database.
pub async fn ingest_file(db_pool: &PgPool, file_content: &[u8], quiet: bool) -> Result<i64> {
    let hash = Sha256::digest(file_content);
    let hash_hex = format!("{:x}", hash);

    let existing_id: Option<i64> = sqlx::query_scalar("SELECT id FROM raw_files WHERE file_hash = $1")
        .bind(&hash_hex)
        .fetch_optional(db_pool)
        .await?;

    if let Some(id) = existing_id {
        if !quiet {
            println!("  -> File with hash {}... already exists with id {}. Skipping.", &hash_hex[..8], id);
        }
        
        return Ok(id);
    }

    // Use our new, powerful validation function.
    let schema = validate_and_identify_schema(file_content)?;
    if !quiet{
        println!("  -> New file validated. Schema: {:?}", schema);
    }
    
    let new_id: i64 = sqlx::query_scalar(
        "INSERT INTO raw_files (file_hash, file_content, detected_schema_name)
         VALUES ($1, $2, $3)
         RETURNING id"
    )
    .bind(&hash_hex)
    .bind(file_content)
    .bind(&schema)
    .fetch_one(db_pool)
    .await?;
    if !quiet{
        println!("  -> Successfully ingested as new file with id {}.", new_id);
    }
    
    Ok(new_id)
}