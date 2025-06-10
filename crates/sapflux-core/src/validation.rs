// In crates/sapflux-core/src/validation.rs

use crate::error::{PipelineError, Result};
use crate::types::FileSchema;
use csv::StringRecord;

pub trait SchemaValidator {
    fn schema(&self) -> FileSchema;
    fn validate(&self, file_content: &[u8]) -> Result<()>;
}

// --- Validator for BOTH CR200 and CR300 Legacy Single-Sensor Formats ---
// RENAMED: from CR200LegacyValidator to LegacySingleSensorValidator
pub struct LegacySingleSensorValidator;

impl SchemaValidator for LegacySingleSensorValidator {
    fn schema(&self) -> FileSchema {
        FileSchema::CRLegacySingleSensor
    }

    fn validate(&self, file_content: &[u8]) -> Result<()> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(file_content);

        let mut records = reader.records();

        let header_rec = records.next().ok_or(PipelineError::Processing(
            "LegacySingleSensor: File is empty".to_string(),
        ))??;
        Self::validate_toa5_header(&header_rec)?; // Note: self becomes Self

        let column_rec = records.next().ok_or(PipelineError::Processing(
            "LegacySingleSensor: Missing header row".to_string(),
        ))??;
        let sdi_col_index = Self::validate_column_headers(&column_rec)?;

        let junk_rec1 = records.next().ok_or(PipelineError::Processing(
            "LegacySingleSensor: Missing junk row 3".to_string(),
        ))??;
        Self::validate_junk_rows(&junk_rec1)?;
        
        records.next().ok_or(PipelineError::Processing(
            "LegacySingleSensor: Missing junk row 4".to_string(),
        ))??;

        let data_rec = records.next().ok_or(PipelineError::Processing(
            "LegacySingleSensor: File has no data rows".to_string(),
        ))??;
        Self::validate_data_row(&data_rec, sdi_col_index)?;

        Ok(())
    }
}

// Helper methods for the validator
impl LegacySingleSensorValidator {
    fn validate_toa5_header(record: &StringRecord) -> Result<()> {
        if record.get(0) != Some("TOA5") {
            return Err(PipelineError::Processing("LegacySingleSensor: Row 1 is not a TOA5 header".to_string()));
        }

        // UPDATED LOGIC: Check for EITHER CR200 or CR300
        let logger_model = record.get(1).unwrap_or("");
        if !logger_model.contains("CR200") && !logger_model.contains("CR300") {
            return Err(PipelineError::Processing(format!(
               "LegacySingleSensor: Header model '{}' is not a recognized CR200 or CR300 series", logger_model
           )));
        }
        Ok(())
    }

    // This method is now more generic and doesn't need to change.
    fn validate_column_headers(record: &StringRecord) -> Result<usize> {
        let sdi_col_index = record.iter().position(|h| h.starts_with("SDI")).ok_or_else(|| {
            PipelineError::Processing("LegacySingleSensor: Could not find an SDI column in the header".to_string())
        })?;
        
        let sdi_col_name = record.get(sdi_col_index).unwrap();
        let sdi_digit_char = sdi_col_name.chars().nth(3).ok_or_else(|| {
            PipelineError::Processing(format!("LegacySingleSensor: Invalid SDI column name '{}'", sdi_col_name))
        })?;

        if !sdi_digit_char.is_ascii_digit() {
            return Err(PipelineError::Processing(format!(
                "LegacySingleSensor: SDI column '{}' is not followed by a digit", sdi_col_name
            )));
        }
        Ok(sdi_col_index)
    }

    // No changes needed here
    fn validate_junk_rows(record: &StringRecord) -> Result<()> {
        if record.get(0) != Some("TS") {
            return Err(PipelineError::Processing("LegacySingleSensor: Row 3 does not start with 'TS'".to_string()));
       }
       Ok(())
    }

    // No changes needed here
    fn validate_data_row(record: &StringRecord, sdi_col_index: usize) -> Result<()> {
        let sdi_address = record.get(sdi_col_index).ok_or_else(|| {
            PipelineError::Processing(format!("LegacySingleSensor: Data row missing field at SDI index {}", sdi_col_index))
        })?;

        if sdi_address.len() != 1 || !sdi_address.chars().next().unwrap().is_ascii_alphanumeric() {
            return Err(PipelineError::Processing(format!(
                "LegacySingleSensor: Invalid SDI-12 address '{}' in data row", sdi_address
            )));
        }
        Ok(())
    }
}