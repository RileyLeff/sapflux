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
pub struct CR300MultiSensorValidator;

impl SchemaValidator for CR300MultiSensorValidator {
    fn schema(&self) -> FileSchema {
        FileSchema::CR300MultiSensor
    }

    fn validate(&self, file_content: &[u8]) -> Result<()> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(file_content);

        let mut records = reader.records();

        let header_rec = records.next().ok_or(PipelineError::Processing("CR300Multi: File is empty".to_string()))??;
        Self::validate_toa5_header(&header_rec)?;

        let column_rec = records.next().ok_or(PipelineError::Processing("CR300Multi: Missing header row".to_string()))??;
        Self::validate_column_structure(&column_rec)?;

        Ok(())
    }
}

impl CR300MultiSensorValidator {
    fn validate_toa5_header(record: &StringRecord) -> Result<()> {
        if record.get(0) != Some("TOA5") {
            return Err(PipelineError::Processing("CR300Multi: Not a TOA5 header".to_string()));
        }
        let logger_model = record.get(1).unwrap_or("");
        if !logger_model.contains("CR300") {
            return Err(PipelineError::Processing(format!("CR300Multi: Not a CR300 logger model ('{}')", logger_model)));
        }
        Ok(())
    }

    /// This is the core of the new logic, implementing your proposal.
    fn validate_column_structure(record: &StringRecord) -> Result<()> {
        // Define the exact set of columns that must appear at the beginning.
        const PREAMBLE_COLS: &[&str] = &["TIMESTAMP", "RECORD", "Batt_volt", "PTemp_C"];
        
        // Define the number of data fields we expect for each sensor.
        // Based on the README, the new format has 20 fields per sensor.
        const FIELDS_PER_SENSOR: usize = 20;

        let headers: Vec<&str> = record.iter().collect();

        // 1. Check if the preamble exists at the start.
        if headers.len() < PREAMBLE_COLS.len() || &headers[..PREAMBLE_COLS.len()] != PREAMBLE_COLS {
            return Err(PipelineError::Processing(
                "CR300Multi: Header does not start with the expected preamble (TIMESTAMP, RECORD, etc.)".to_string()
            ));
        }

        // 2. The rest of the columns must be sensor data.
        let sensor_cols = &headers[PREAMBLE_COLS.len()..];

        // 3. The number of sensor columns must be a perfect multiple of FIELDS_PER_SENSOR.
        if sensor_cols.is_empty() {
             return Err(PipelineError::Processing(
                "CR300Multi: Header contains preamble but no sensor columns.".to_string()
            ));
        }
        if sensor_cols.len() % FIELDS_PER_SENSOR != 0 {
            return Err(PipelineError::Processing(format!(
                "CR300Multi: Number of sensor columns ({}) is not a multiple of {} per sensor.",
                sensor_cols.len(), FIELDS_PER_SENSOR
            )));
        }

        // 4. (Optional but good) Check that the columns follow the S{i}_ pattern.
        let num_sensors = sensor_cols.len() / FIELDS_PER_SENSOR;
        for i in 0..num_sensors {
            let prefix = format!("S{}_", i);
            let start_index = i * FIELDS_PER_SENSOR;
            let first_col_in_block = sensor_cols[start_index];
            if !first_col_in_block.starts_with(&prefix) {
                 return Err(PipelineError::Processing(format!(
                    "CR300Multi: Sensor block {} does not start with the expected prefix '{}'. Found '{}'.",
                    i, prefix, first_col_in_block
                )));
            }
        }

        Ok(())
    }
}