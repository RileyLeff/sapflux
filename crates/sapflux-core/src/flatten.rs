use polars::prelude::{Column, DataFrame, NamedFrom, PolarsError, Series};
use sapflux_parser::ParsedFileData;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FlattenError {
    #[error("column length mismatch for {column}: expected {expected}, found {found}")]
    LengthMismatch {
        column: &'static str,
        expected: usize,
        found: usize,
    },
    #[error(transparent)]
    Polars(#[from] PolarsError),
}

/// Converts a batch of parsed files into a single observation DataFrame with one row per
/// (timestamp, record, logger_id, sdi12_address, thermistor_depth).
pub fn flatten_parsed_files(files: &[&ParsedFileData]) -> Result<DataFrame, FlattenError> {
    let mut frames: Vec<DataFrame> = Vec::new();

    for file in files {
        let logger_df = &file.logger.df;
        let rows = logger_df.height();

        for sensor in &file.logger.sensors {
            if let Some(sensor_df) = sensor.sensor_df.as_ref() {
                if sensor_df.height() != rows {
                    return Err(FlattenError::LengthMismatch {
                        column: "sensor_df",
                        expected: rows,
                        found: sensor_df.height(),
                    });
                }
            }

            let address_str = sensor.sdi12_address.as_char().to_string();

            for pair in &sensor.thermistor_pairs {
                if pair.df.height() != rows {
                    return Err(FlattenError::LengthMismatch {
                        column: "thermistor_pair",
                        expected: rows,
                        found: pair.df.height(),
                    });
                }

                let mut columns: Vec<Column> = logger_df.get_columns().iter().cloned().collect();

                let file_hash = Series::new(
                    "file_hash".into(),
                    vec![file.file_hash.as_str(); rows],
                );
                columns.push(file_hash.into());

                let sdi12 = Series::new(
                    "sdi12_address".into(),
                    vec![address_str.as_str(); rows],
                );
                columns.push(sdi12.into());

                let depth = Series::new(
                    "thermistor_depth".into(),
                    vec![pair.depth.as_str(); rows],
                );
                columns.push(depth.into());

                if let Some(sensor_df) = sensor.sensor_df.as_ref() {
                    for col in sensor_df.get_columns() {
                        columns.push(col.clone());
                    }
                }

                for col in pair.df.get_columns() {
                    columns.push(col.clone());
                }

                let df = DataFrame::new(columns)?;
                frames.push(df);
            }
        }
    }

    match frames.len() {
        0 => Ok(DataFrame::default()),
        1 => Ok(frames.remove(0)),
        _ => {
            let mut iter = frames.into_iter();
            let mut combined = iter.next().unwrap();
            for df in iter {
                combined.vstack_mut(&df)?;
            }
            Ok(combined)
        }
    }
}
