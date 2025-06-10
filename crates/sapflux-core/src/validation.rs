// In crates/sapflux-core/src/validation.rs

use crate::error::{PipelineError, Result};
use crate::types::FileSchema;
use csv::StringRecord; // We'll need this type for our helper methods

// The trait definition remains the same
pub trait SchemaValidator {
    fn schema(&self) -> FileSchema;
    fn validate(&self, file_content: &[u8]) -> Result<()>;
}

// --- Validator for the CR200 Legacy Schema ---
pub struct CR200LegacyValidator;

impl SchemaValidator for CR200LegacyValidator {
    fn schema(&self) -> FileSchema {
        FileSchema::CRLegacySingleSensor
    }

    /// The main validation function. It orchestrates a series of checks.
    /// It now reads like a high-level summary of the validation logic.
    fn validate(&self, file_content: &[u8]) -> Result<()> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(file_content);

        let mut records = reader.records();

        // --- Execute each validation step in order ---
        let header_rec = records.next().ok_or(PipelineError::Processing(
            "CR200Legacy: File is empty or has no first row".to_string(),
        ))??;
        Self::validate_toa5_header(&header_rec)?;

        let column_rec = records.next().ok_or(PipelineError::Processing(
            "CR200Legacy: Missing header row (row 2)".to_string(),
        ))??;
        let sdi_col_index = Self::validate_column_headers(&column_rec)?;

        let junk_rec1 = records.next().ok_or(PipelineError::Processing(
            "CR200Legacy: Missing junk row (row 3)".to_string(),
        ))??;
        Self::validate_junk_rows(&junk_rec1)?;
        
        // We only need to check for the existence of the 4th row.
        records.next().ok_or(PipelineError::Processing(
            "CR200Legacy: Missing junk row (row 4)".to_string(),
        ))??;

        let data_rec = records.next().ok_or(PipelineError::Processing(
            "CR200Legacy: File has no data rows".to_string(),
        ))??;
        Self::validate_data_row(&data_rec, sdi_col_index)?;

        // If all checks pass, the file is valid.
        Ok(())
    }
}

// Private helper methods for the validator.
// By convention, these are placed in a separate `impl` block.
impl CR200LegacyValidator {
    /// Validates the first row (TOA5 Header).
    fn validate_toa5_header(record: &StringRecord) -> Result<()> {
        if record.get(0) != Some("TOA5") {
            return Err(PipelineError::Processing("CR200Legacy: Row 1 is not a TOA5 header".to_string()));
        }

        let logger_model = record.get(1).unwrap_or("");
        if !logger_model.contains("CR200") {
            return Err(PipelineError::Processing(format!(
               "CR200Legacy: Header model '{}' is not a CR200 series", logger_model
           )));
        }
        Ok(())
    }

    /// Validates the second row (Column Headers) and returns the index of the critical SDI column.
    fn validate_column_headers(record: &StringRecord) -> Result<usize> {
        let sdi_col_index = record.iter().position(|h| h.starts_with("SDI")).ok_or_else(|| {
            PipelineError::Processing("CR200Legacy: Could not find an SDI column in the header".to_string())
        })?;
        
        let sdi_col_name = record.get(sdi_col_index).unwrap();
        let sdi_digit_char = sdi_col_name.chars().nth(3).ok_or_else(|| {
            PipelineError::Processing(format!("CR200Legacy: Invalid SDI column name '{}'", sdi_col_name))
        })?;

        if !sdi_digit_char.is_ascii_digit() {
            return Err(PipelineError::Processing(format!(
                "CR200Legacy: SDI column '{}' is not followed by a digit", sdi_col_name
            )));
        }
        Ok(sdi_col_index)
    }

    /// Validates the third "junk" row.
    fn validate_junk_rows(record: &StringRecord) -> Result<()> {
        if record.get(0) != Some("TS") {
            return Err(PipelineError::Processing("CR200Legacy: Row 3 does not start with 'TS' as expected".to_string()));
       }
       Ok(())
    }

    /// Validates the structure and key values of the first data row.
    fn validate_data_row(record: &StringRecord, sdi_col_index: usize) -> Result<()> {
        let sdi_address = record.get(sdi_col_index).ok_or_else(|| {
            PipelineError::Processing(format!("CR200Legacy: Data row is missing field at SDI index {}", sdi_col_index))
        })?;

        if sdi_address.len() != 1 || !sdi_address.chars().next().unwrap().is_ascii_alphanumeric() {
            return Err(PipelineError::Processing(format!(
                "CR200Legacy: Invalid SDI-12 address '{}' found in data row", sdi_address
            )));
        }
        Ok(())
    }
}