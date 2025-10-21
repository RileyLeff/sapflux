use std::fmt;
use std::io::{Cursor, Read, Write};

use ::zip::{write::FileOptions, CompressionMethod, ZipArchive, ZipWriter};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ThermistorDepth {
    Inner,
    Outer,
}

impl ThermistorDepth {
    pub fn as_str(&self) -> &'static str {
        match self {
            ThermistorDepth::Inner => "inner",
            ThermistorDepth::Outer => "outer",
        }
    }
}

impl fmt::Display for ThermistorDepth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for ThermistorDepth {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim().to_ascii_lowercase().as_str() {
            "inner" | "in" | "i" => Ok(ThermistorDepth::Inner),
            "outer" | "out" | "o" => Ok(ThermistorDepth::Outer),
            other => Err(format!("unknown thermistor depth '{other}'")),
        }
    }
}

impl TryFrom<char> for ThermistorDepth {
    type Error = String;

    fn try_from(value: char) -> Result<Self, Self::Error> {
        match value {
            'i' | 'I' => Ok(ThermistorDepth::Inner),
            'o' | 'O' => Ok(ThermistorDepth::Outer),
            _ => Err(format!("unknown thermistor depth '{value}'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Sdi12Address(pub char);

impl Sdi12Address {
    pub fn new(ch: char) -> Result<Self, String> {
        if ch.is_ascii_alphanumeric() {
            Ok(Self(ch))
        } else {
            Err(format!("invalid SDI-12 address '{ch}'"))
        }
    }

    pub fn as_char(&self) -> char {
        self.0
    }
}

impl fmt::Display for Sdi12Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.to_string())
    }
}

impl TryFrom<&str> for Sdi12Address {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let trimmed = value.trim();
        let mut chars = trimmed.chars();
        let ch = chars
            .next()
            .ok_or_else(|| "missing SDI-12 address".to_string())?;
        if chars.next().is_some() {
            return Err(format!(
                "SDI-12 address must be a single character, got '{trimmed}'"
            ));
        }
        Sdi12Address::new(ch)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub file_format: String,
    pub logger_name: String,
    pub logger_type: String,
    pub serial_number: Option<String>,
    pub os_version: Option<String>,
    pub program_name: String,
    pub signature: Option<String>,
    pub table_name: String,
}

impl FileMetadata {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        file_format: impl Into<String>,
        logger_name: impl Into<String>,
        logger_type: impl Into<String>,
        serial_number: Option<String>,
        os_version: Option<String>,
        program_name: impl Into<String>,
        signature: Option<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            file_format: file_format.into(),
            logger_name: logger_name.into(),
            logger_type: logger_type.into(),
            serial_number,
            os_version,
            program_name: program_name.into(),
            signature,
            table_name: table_name.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParsedFileData {
    pub file_hash: String,
    pub raw_text: String,
    pub file_metadata: FileMetadata,
    pub logger: LoggerData,
}

#[derive(Debug, Clone)]
pub struct LoggerData {
    pub df: DataFrame,
    pub sensors: Vec<SensorData>,
}

#[derive(Debug, Clone)]
pub struct SensorData {
    pub sdi12_address: Sdi12Address,
    pub sensor_df: Option<DataFrame>,
    pub thermistor_pairs: Vec<ThermistorPairData>,
}

#[derive(Debug, Clone)]
pub struct ThermistorPairData {
    pub depth: ThermistorDepth,
    pub df: DataFrame,
}

#[derive(Debug, Error)]
pub enum ArchiveError {
    #[error("Polars operation failed: {0}")]
    Polars(#[from] PolarsError),
    #[error("JSON operation failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("ZIP operation failed: {0}")]
    Zip(#[from] ::zip::result::ZipError),
    #[error("IO operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Manifest is missing or corrupt")]
    MissingManifest,
    #[error("Data file '{0}' is missing from archive")]
    MissingDataFile(String),
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    file_hash: String,
    file_metadata: FileMetadata,
    structure: ManifestLogger,
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestLogger {
    logger_df_path: String,
    sensors: Vec<ManifestSensor>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestSensor {
    sdi12_address: Sdi12Address,
    sensor_df_path: Option<String>,
    thermistor_pairs: Vec<ManifestThermistorPair>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestThermistorPair {
    depth: ThermistorDepth,
    df_path: String,
}

impl ParsedFileData {
    pub fn to_zip_archive(&self) -> Result<Vec<u8>, ArchiveError> {
        let manifest = self.to_manifest();
        let manifest_bytes = serde_json::to_vec(&manifest)?;

        let cursor = Cursor::new(Vec::new());
        let mut zip = ZipWriter::new(cursor);
        let options = FileOptions::default().compression_method(CompressionMethod::Deflated);

        zip.start_file("manifest.json", options)?;
        zip.write_all(&manifest_bytes)?;

        // logger dataframe
        let mut logger_df = self.logger.df.clone();
        let mut logger_bytes = Vec::new();
        ParquetWriter::new(&mut logger_bytes).finish(&mut logger_df)?;
        zip.start_file(&manifest.structure.logger_df_path, options)?;
        zip.write_all(&logger_bytes)?;

        for sensor in &self.logger.sensors {
            if let Some(df) = &sensor.sensor_df {
                let mut df_clone = df.clone();
                let mut buffer = Vec::new();
                let path = format!("sensor_{}_meta.parquet", sensor.sdi12_address.as_char());
                ParquetWriter::new(&mut buffer).finish(&mut df_clone)?;
                zip.start_file(&path, options)?;
                zip.write_all(&buffer)?;
            }

            for pair in &sensor.thermistor_pairs {
                let mut df_clone = pair.df.clone();
                let mut buffer = Vec::new();
                let path = format!(
                    "sensor_{}_{}.parquet",
                    sensor.sdi12_address.as_char(),
                    pair.depth.as_str()
                );
                ParquetWriter::new(&mut buffer).finish(&mut df_clone)?;
                zip.start_file(&path, options)?;
                zip.write_all(&buffer)?;
            }
        }

        let cursor = zip.finish()?;
        let zip_buffer = cursor.into_inner();
        Ok(zip_buffer)
    }

    fn to_manifest(&self) -> Manifest {
        let sensors = self
            .logger
            .sensors
            .iter()
            .map(|sensor| {
                let sensor_df_path = sensor
                    .sensor_df
                    .as_ref()
                    .map(|_| format!("sensor_{}_meta.parquet", sensor.sdi12_address.as_char()));

                let thermistor_pairs = sensor
                    .thermistor_pairs
                    .iter()
                    .map(|pair| ManifestThermistorPair {
                        depth: pair.depth,
                        df_path: format!(
                            "sensor_{}_{}.parquet",
                            sensor.sdi12_address.as_char(),
                            pair.depth.as_str()
                        ),
                    })
                    .collect();

                ManifestSensor {
                    sdi12_address: sensor.sdi12_address,
                    sensor_df_path,
                    thermistor_pairs,
                }
            })
            .collect();

        Manifest {
            file_hash: self.file_hash.clone(),
            file_metadata: self.file_metadata.clone(),
            structure: ManifestLogger {
                logger_df_path: "logger.parquet".to_string(),
                sensors,
            },
        }
    }

    pub fn from_zip_archive(zip_bytes: &[u8], raw_text: String) -> Result<Self, ArchiveError> {
        let cursor = Cursor::new(zip_bytes);
        let mut archive = ZipArchive::new(cursor)?;

        let manifest: Manifest = {
            let mut manifest_file = archive
                .by_name("manifest.json")
                .map_err(|_| ArchiveError::MissingManifest)?;
            let mut manifest_bytes = Vec::new();
            manifest_file.read_to_end(&mut manifest_bytes)?;
            serde_json::from_slice(&manifest_bytes)?
        };

        let logger_df = {
            let mut logger_df_file = archive
                .by_name(&manifest.structure.logger_df_path)
                .map_err(|_| {
                    ArchiveError::MissingDataFile(manifest.structure.logger_df_path.clone())
                })?;
            let mut logger_df_bytes = Vec::new();
            logger_df_file.read_to_end(&mut logger_df_bytes)?;
            ParquetReader::new(Cursor::new(logger_df_bytes)).finish()?
        };

        let mut sensors = Vec::new();
        for manifest_sensor in manifest.structure.sensors {
            let sensor_df = if let Some(path) = manifest_sensor.sensor_df_path.as_ref() {
                let mut file = archive
                    .by_name(path)
                    .map_err(|_| ArchiveError::MissingDataFile(path.clone()))?;
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)?;
                Some(ParquetReader::new(Cursor::new(bytes)).finish()?)
            } else {
                None
            };

            let mut thermistor_pairs = Vec::new();
            for manifest_pair in manifest_sensor.thermistor_pairs {
                let mut file = archive
                    .by_name(&manifest_pair.df_path)
                    .map_err(|_| ArchiveError::MissingDataFile(manifest_pair.df_path.clone()))?;
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)?;
                let df = ParquetReader::new(Cursor::new(bytes)).finish()?;
                thermistor_pairs.push(ThermistorPairData {
                    depth: manifest_pair.depth,
                    df,
                });
            }

            sensors.push(SensorData {
                sdi12_address: manifest_sensor.sdi12_address,
                sensor_df,
                thermistor_pairs,
            });
        }

        Ok(ParsedFileData {
            file_hash: manifest.file_hash,
            raw_text,
            file_metadata: manifest.file_metadata,
            logger: LoggerData {
                df: logger_df,
                sensors,
            },
        })
    }
}
