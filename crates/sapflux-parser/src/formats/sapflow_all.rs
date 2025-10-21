use std::collections::HashMap;

use csv::StringRecord;

use crate::errors::ParserError;
use crate::model::{ParsedFileData, Sdi12Address, ThermistorDepth};
use crate::registry::SapflowParser;

use super::{
    build_logger_dataframe, derive_logger_id_from_header, make_logger_data, parse_metadata,
    parse_optional_f64, parse_optional_i64, parse_required_i64, parse_timestamp, ColumnRole,
    LoggerColumnKind, LoggerColumns, SensorFrameBuilder, ThermistorMetric,
};

#[derive(Clone, Copy)]
struct SensorColumnSpec {
    suffix: &'static str,
    depth: ThermistorDepth,
    metric: ThermistorMetric,
    unit: &'static str,
}

const SENSOR_COLUMN_SPECS: [SensorColumnSpec; 20] = [
    SensorColumnSpec {
        suffix: "alpout",
        depth: ThermistorDepth::Outer,
        metric: ThermistorMetric::Alpha,
        unit: "ratio",
    },
    SensorColumnSpec {
        suffix: "alpinn",
        depth: ThermistorDepth::Inner,
        metric: ThermistorMetric::Alpha,
        unit: "ratio",
    },
    SensorColumnSpec {
        suffix: "betout",
        depth: ThermistorDepth::Outer,
        metric: ThermistorMetric::Beta,
        unit: "ratio",
    },
    SensorColumnSpec {
        suffix: "betinn",
        depth: ThermistorDepth::Inner,
        metric: ThermistorMetric::Beta,
        unit: "ratio",
    },
    SensorColumnSpec {
        suffix: "tmxtout",
        depth: ThermistorDepth::Outer,
        metric: ThermistorMetric::TimeToMaxDownstream,
        unit: "sec",
    },
    SensorColumnSpec {
        suffix: "tmxtinn",
        depth: ThermistorDepth::Inner,
        metric: ThermistorMetric::TimeToMaxDownstream,
        unit: "sec",
    },
    SensorColumnSpec {
        suffix: "tpdsout",
        depth: ThermistorDepth::Outer,
        metric: ThermistorMetric::PrePulseTempDownstream,
        unit: "degC",
    },
    SensorColumnSpec {
        suffix: "dtdsout",
        depth: ThermistorDepth::Outer,
        metric: ThermistorMetric::MaxTempRiseDownstream,
        unit: "degC",
    },
    SensorColumnSpec {
        suffix: "tsdsout",
        depth: ThermistorDepth::Outer,
        metric: ThermistorMetric::PostPulseTempDownstream,
        unit: "degC",
    },
    SensorColumnSpec {
        suffix: "tpusout",
        depth: ThermistorDepth::Outer,
        metric: ThermistorMetric::PrePulseTempUpstream,
        unit: "degC",
    },
    SensorColumnSpec {
        suffix: "dtusout",
        depth: ThermistorDepth::Outer,
        metric: ThermistorMetric::MaxTempRiseUpstream,
        unit: "degC",
    },
    SensorColumnSpec {
        suffix: "tsusout",
        depth: ThermistorDepth::Outer,
        metric: ThermistorMetric::PostPulseTempUpstream,
        unit: "degC",
    },
    SensorColumnSpec {
        suffix: "tpdsinn",
        depth: ThermistorDepth::Inner,
        metric: ThermistorMetric::PrePulseTempDownstream,
        unit: "degC",
    },
    SensorColumnSpec {
        suffix: "dtdsinn",
        depth: ThermistorDepth::Inner,
        metric: ThermistorMetric::MaxTempRiseDownstream,
        unit: "degC",
    },
    SensorColumnSpec {
        suffix: "tsdsinn",
        depth: ThermistorDepth::Inner,
        metric: ThermistorMetric::PostPulseTempDownstream,
        unit: "degC",
    },
    SensorColumnSpec {
        suffix: "tpusinn",
        depth: ThermistorDepth::Inner,
        metric: ThermistorMetric::PrePulseTempUpstream,
        unit: "degC",
    },
    SensorColumnSpec {
        suffix: "dtusinn",
        depth: ThermistorDepth::Inner,
        metric: ThermistorMetric::MaxTempRiseUpstream,
        unit: "degC",
    },
    SensorColumnSpec {
        suffix: "tsusinn",
        depth: ThermistorDepth::Inner,
        metric: ThermistorMetric::PostPulseTempUpstream,
        unit: "degC",
    },
    SensorColumnSpec {
        suffix: "tmxtuso",
        depth: ThermistorDepth::Outer,
        metric: ThermistorMetric::TimeToMaxUpstream,
        unit: "sec",
    },
    SensorColumnSpec {
        suffix: "tmxtusi",
        depth: ThermistorDepth::Inner,
        metric: ThermistorMetric::TimeToMaxUpstream,
        unit: "sec",
    },
];

pub struct SapFlowAllParser;

impl Default for SapFlowAllParser {
    fn default() -> Self {
        Self
    }
}

impl SapFlowAllParser {
    const NAME: &'static str = "SAPFLOW_ALL";

    fn classify_columns(columns: &StringRecord) -> Result<Vec<ColumnRole>, ParserError> {
        // NOTE: The strict checks in this reference parser mirror the fixtures used in tests.
        // For production ingestion we expect to replace these with pattern-based validations
        // (e.g., prefix/suffix checks, SDI-12 templates) so harmless header tweaks do not turn
        // into hard failures.
        columns.iter().map(Self::classify_column).collect()
    }

    fn classify_column(column: &str) -> Result<ColumnRole, ParserError> {
        let trimmed = column.trim();
        if trimmed.eq_ignore_ascii_case("timestamp") {
            return Ok(ColumnRole::Logger(LoggerColumnKind::Timestamp));
        }
        if trimmed.eq_ignore_ascii_case("record") {
            return Ok(ColumnRole::Logger(LoggerColumnKind::Record));
        }
        if trimmed.eq_ignore_ascii_case("batt_volt")
            || trimmed.eq_ignore_ascii_case("battv")
            || trimmed.eq_ignore_ascii_case("battv_min")
        {
            return Ok(ColumnRole::Logger(LoggerColumnKind::BatteryVoltage));
        }
        if trimmed.eq_ignore_ascii_case("ptemp_c") {
            return Ok(ColumnRole::Logger(LoggerColumnKind::PanelTemperature));
        }
        if trimmed.eq_ignore_ascii_case("id") {
            return Ok(ColumnRole::Logger(LoggerColumnKind::LoggerId));
        }

        if let Some((address, suffix)) = Self::split_sensor_column(trimmed) {
            let lower = suffix.to_ascii_lowercase();
            if let Some(spec) = Self::sensor_spec_by_suffix(&lower) {
                return Ok(ColumnRole::ThermistorMetric {
                    address,
                    depth: spec.depth,
                    metric: spec.metric,
                });
            }

            return Err(ParserError::FormatMismatch {
                parser: Self::NAME,
                reason: format!("unrecognized sensor column suffix '{lower}'"),
            });
        }

        Err(ParserError::FormatMismatch {
            parser: Self::NAME,
            reason: format!("unrecognized column '{trimmed}'"),
        })
    }

    fn split_sensor_column(name: &str) -> Option<(Sdi12Address, &str)> {
        if !name.starts_with('S') {
            return None;
        }
        let (prefix, suffix) = name.split_once('_')?; // e.g. S0_AlpOut
        if prefix.len() != 2 {
            return None;
        }
        let addr_char = prefix.chars().nth(1)?;
        let address = Sdi12Address::new(addr_char).ok()?;
        Some((address, suffix))
    }

    fn sensor_spec_by_suffix(lower: &str) -> Option<&'static SensorColumnSpec> {
        SENSOR_COLUMN_SPECS.iter().find(|spec| spec.suffix == lower)
    }

    fn logger_unit_matches(kind: LoggerColumnKind, value: &str) -> bool {
        match kind {
            LoggerColumnKind::Timestamp => value.eq_ignore_ascii_case("TS"),
            LoggerColumnKind::Record => value.eq_ignore_ascii_case("RN"),
            LoggerColumnKind::BatteryVoltage => {
                value.is_empty() || value.eq_ignore_ascii_case("Volts")
            }
            LoggerColumnKind::PanelTemperature => {
                value.is_empty() || value.eq_ignore_ascii_case("degC")
            }
            LoggerColumnKind::LoggerId => value.is_empty(),
        }
    }

    fn logger_characteristic_matches(kind: LoggerColumnKind, value: &str) -> bool {
        match kind {
            LoggerColumnKind::Timestamp | LoggerColumnKind::Record => value.is_empty(),
            LoggerColumnKind::BatteryVoltage
            | LoggerColumnKind::PanelTemperature
            | LoggerColumnKind::LoggerId => value.is_empty() || value.eq_ignore_ascii_case("Smp"),
        }
    }

    fn reader_builder() -> csv::ReaderBuilder {
        let mut builder = csv::ReaderBuilder::new();
        builder.has_headers(false).flexible(true);
        builder
    }

    fn parse_with_builder(
        &self,
        builder: csv::ReaderBuilder,
        content: &str,
    ) -> Result<ParsedFileData, ParserError> {
        let mut reader = builder.from_reader(content.as_bytes());

        let mut records = reader.records();

        let header = records
            .next()
            .ok_or(ParserError::FormatMismatch {
                parser: Self::NAME,
                reason: "file missing metadata header".to_string(),
            })?
            .map_err(|err| ParserError::Csv {
                parser: Self::NAME,
                source: err,
            })?;
        let metadata = parse_metadata(Self::NAME, &header)?;
        Self::ensure_table(&metadata)?;

        let columns = records
            .next()
            .ok_or(ParserError::FormatMismatch {
                parser: Self::NAME,
                reason: "file missing column header row".to_string(),
            })?
            .map_err(|err| ParserError::Csv {
                parser: Self::NAME,
                source: err,
            })?;

        let units = records
            .next()
            .ok_or(ParserError::FormatMismatch {
                parser: Self::NAME,
                reason: "file missing units row".to_string(),
            })?
            .map_err(|err| ParserError::Csv {
                parser: Self::NAME,
                source: err,
            })?;

        let characteristics = records
            .next()
            .ok_or(ParserError::FormatMismatch {
                parser: Self::NAME,
                reason: "file missing measurement characteristics row".to_string(),
            })?
            .map_err(|err| ParserError::Csv {
                parser: Self::NAME,
                source: err,
            })?;

        let column_roles = Self::classify_columns(&columns)?;

        Self::validate_units(&columns, &units, &column_roles)?;
        Self::validate_measurements(&columns, &characteristics, &column_roles)?;

        if units.len() != columns.len() || characteristics.len() != columns.len() {
            return Err(ParserError::FormatMismatch {
                parser: Self::NAME,
                reason: "header rows have inconsistent column counts".to_string(),
            });
        }

        let mut logger_columns = LoggerColumns::new(0);
        let mut sensor_builder = SensorFrameBuilder::new();
        let mut row_count = 0usize;

        let mut previous_record: Option<i64> = None;
        let mut canonical_logger_id: Option<String> = None;

        for (row_idx, record) in records.enumerate() {
            let record = record.map_err(|err| ParserError::Csv {
                parser: Self::NAME,
                source: err,
            })?;

            if record.len() != column_roles.len() {
                return Err(ParserError::DataRow {
                    parser: Self::NAME,
                    line_index: row_idx + 5,
                    message: format!(
                        "expected {} columns but found {}",
                        column_roles.len(),
                        record.len()
                    ),
                });
            }

            let line_index = row_idx + 5;

            for (idx, role) in column_roles.iter().enumerate() {
                let header_name = columns.get(idx).unwrap_or("");
                let value = record.get(idx).unwrap_or("");
                match role {
                    ColumnRole::Logger(kind) => match kind {
                        LoggerColumnKind::Timestamp => {
                            let micros = parse_timestamp(Self::NAME, value, line_index)?;
                            logger_columns.timestamp.push(micros);
                        }
                        LoggerColumnKind::Record => {
                            let record_value =
                                parse_required_i64(Self::NAME, value, line_index, header_name)?;

                            if let Some(prev) = previous_record {
                                if record_value != prev + 1 {
                                    return Err(ParserError::DataRow {
                                        parser: Self::NAME,
                                        line_index,
                                        message: format!(
                                            "record column must increment by 1 (expected {}, found {})",
                                            prev + 1,
                                            record_value
                                        ),
                                    });
                                }
                            }
                            previous_record = Some(record_value);
                            logger_columns.record.push(record_value);
                        }
                        LoggerColumnKind::BatteryVoltage => {
                            let parsed =
                                parse_optional_f64(Self::NAME, value, line_index, header_name)?;
                            logger_columns.battery_mut().push(parsed);
                        }
                        LoggerColumnKind::PanelTemperature => {
                            let parsed =
                                parse_optional_f64(Self::NAME, value, line_index, header_name)?;
                            logger_columns.panel_mut().push(parsed);
                        }
                        LoggerColumnKind::LoggerId => {
                            let parsed =
                                parse_optional_i64(Self::NAME, value, line_index, header_name)?;

                            let value = parsed.ok_or_else(|| ParserError::DataRow {
                                parser: Self::NAME,
                                line_index,
                                message: "logger id column contained NULL".to_string(),
                            })?;

                            let value_str = value.to_string();
                            if let Some(existing) = canonical_logger_id.as_ref() {
                                if existing != &value_str {
                                    return Err(ParserError::Validation {
                                        parser: Self::NAME,
                                        message: format!(
                                            "logger id column contained inconsistent values ({} != {})",
                                            existing,
                                            value_str
                                        ),
                                    });
                                }
                            } else {
                                canonical_logger_id = Some(value_str.clone());
                            }

                            logger_columns.logger_id_mut().push(Some(value_str));
                        }
                    },
                    ColumnRole::ThermistorMetric {
                        address,
                        depth,
                        metric,
                    } => {
                        let parsed =
                            parse_optional_f64(Self::NAME, value, line_index, header_name)?;
                        sensor_builder.push_thermistor_metric(*address, *depth, *metric, parsed);
                    }
                    ColumnRole::SensorAddress { .. } | ColumnRole::SensorMetric { .. } => {}
                }
            }

            row_count += 1;
        }

        if row_count == 0 {
            return Err(ParserError::EmptyData { parser: Self::NAME });
        }

        if logger_columns.logger_id.is_none() {
            let derived = derive_logger_id_from_header(Self::NAME, &metadata)?;
            logger_columns.logger_id = Some(vec![Some(derived); row_count]);
        }

        let logger_df = build_logger_dataframe(Self::NAME, logger_columns)?;
        let sensors = sensor_builder.build(Self::NAME, row_count)?;
        let logger = make_logger_data(logger_df, sensors);

        Ok(ParsedFileData {
            file_hash: String::new(),
            raw_text: content.to_string(),
            file_metadata: metadata,
            logger,
        })
    }

    #[cfg(test)]
    pub(crate) fn parse_with_custom_builder(
        &self,
        builder: csv::ReaderBuilder,
        content: &str,
    ) -> Result<ParsedFileData, ParserError> {
        self.parse_with_builder(builder, content)
    }

    fn ensure_table(metadata: &crate::model::FileMetadata) -> Result<(), ParserError> {
        if metadata.table_name.eq_ignore_ascii_case("sapflowall") {
            Ok(())
        } else {
            Err(ParserError::FormatMismatch {
                parser: Self::NAME,
                reason: format!(
                    "table name '{}' does not match SapFlowAll format",
                    metadata.table_name
                ),
            })
        }
    }
    fn validate_units(
        columns: &StringRecord,
        units: &StringRecord,
        roles: &[ColumnRole],
    ) -> Result<(), ParserError> {
        if units.len() != roles.len() {
            return Err(ParserError::InvalidHeader {
                parser: Self::NAME,
                row_index: 3,
                message: format!(
                    "expected {} unit columns, found {}",
                    roles.len(),
                    units.len()
                ),
            });
        }

        let mut sensor_counts: HashMap<Sdi12Address, usize> = HashMap::new();
        let mut current_address: Option<Sdi12Address> = None;
        let mut metric_index: usize = 0;

        for (idx, (role, unit)) in roles.iter().zip(units.iter()).enumerate() {
            match role {
                ColumnRole::Logger(kind) => {
                    current_address = None;
                    metric_index = 0;
                    if !Self::logger_unit_matches(*kind, unit) {
                        return Err(ParserError::InvalidHeader {
                            parser: Self::NAME,
                            row_index: 3,
                            message: format!(
                                "unexpected units '{unit}' for column '{}'",
                                columns.get(idx).unwrap_or("")
                            ),
                        });
                    }
                }
                ColumnRole::ThermistorMetric {
                    address,
                    depth,
                    metric,
                } => {
                    if current_address != Some(*address) {
                        if let Some(prev) = current_address {
                            if metric_index != SENSOR_COLUMN_SPECS.len() {
                                return Err(ParserError::InvalidHeader {
                                    parser: Self::NAME,
                                    row_index: 3,
                                    message: format!(
                                        "sensor {prev} had {metric_index} columns, expected {}",
                                        SENSOR_COLUMN_SPECS.len()
                                    ),
                                });
                            }
                        }
                        current_address = Some(*address);
                        metric_index = 0;
                    }

                    if metric_index >= SENSOR_COLUMN_SPECS.len() {
                        return Err(ParserError::InvalidHeader {
                            parser: Self::NAME,
                            row_index: 3,
                            message: format!(
                                "sensor {address} had more than {} columns",
                                SENSOR_COLUMN_SPECS.len()
                            ),
                        });
                    }

                    let spec = &SENSOR_COLUMN_SPECS[metric_index];
                    if spec.depth != *depth || spec.metric != *metric {
                        return Err(ParserError::InvalidHeader {
                            parser: Self::NAME,
                            row_index: 3,
                            message: format!(
                                "unexpected column '{}' at position {idx}",
                                columns.get(idx).unwrap_or("")
                            ),
                        });
                    }
                    if unit != spec.unit {
                        return Err(ParserError::InvalidHeader {
                            parser: Self::NAME,
                            row_index: 3,
                            message: format!(
                                "unexpected units '{unit}' for column '{}' (expected '{}')",
                                columns.get(idx).unwrap_or(""),
                                spec.unit
                            ),
                        });
                    }
                    metric_index += 1;
                    *sensor_counts.entry(*address).or_insert(0) += 1;
                }
                ColumnRole::SensorAddress { .. } | ColumnRole::SensorMetric { .. } => {}
            }
        }

        if let Some(address) = current_address {
            if metric_index != SENSOR_COLUMN_SPECS.len() {
                return Err(ParserError::InvalidHeader {
                    parser: Self::NAME,
                    row_index: 3,
                    message: format!(
                        "sensor {address} had {metric_index} columns, expected {}",
                        SENSOR_COLUMN_SPECS.len()
                    ),
                });
            }
        }

        for (address, count) in sensor_counts {
            if count != SENSOR_COLUMN_SPECS.len() {
                return Err(ParserError::InvalidHeader {
                    parser: Self::NAME,
                    row_index: 3,
                    message: format!(
                        "sensor {address} had {count} columns, expected {}",
                        SENSOR_COLUMN_SPECS.len()
                    ),
                });
            }
        }

        Ok(())
    }

    fn validate_measurements(
        columns: &StringRecord,
        characteristics: &StringRecord,
        roles: &[ColumnRole],
    ) -> Result<(), ParserError> {
        if characteristics.len() != roles.len() {
            return Err(ParserError::InvalidHeader {
                parser: Self::NAME,
                row_index: 4,
                message: format!(
                    "expected {} characteristic columns, found {}",
                    roles.len(),
                    characteristics.len()
                ),
            });
        }

        let mut current_address: Option<Sdi12Address> = None;
        let mut metric_index: usize = 0;

        for (idx, (role, value)) in roles.iter().zip(characteristics.iter()).enumerate() {
            match role {
                ColumnRole::Logger(kind) => {
                    current_address = None;
                    metric_index = 0;
                    if !Self::logger_characteristic_matches(*kind, value) {
                        return Err(ParserError::InvalidHeader {
                            parser: Self::NAME,
                            row_index: 4,
                            message: format!(
                                "unexpected characteristic '{value}' for column '{}'",
                                columns.get(idx).unwrap_or("")
                            ),
                        });
                    }
                }
                ColumnRole::ThermistorMetric { address, .. } => {
                    if current_address != Some(*address) {
                        if let Some(prev) = current_address {
                            if metric_index != SENSOR_COLUMN_SPECS.len() {
                                return Err(ParserError::InvalidHeader {
                                    parser: Self::NAME,
                                    row_index: 4,
                                    message: format!(
                                        "sensor {prev} had {metric_index} characteristics, expected {}",
                                        SENSOR_COLUMN_SPECS.len()
                                    ),
                                });
                            }
                        }
                        current_address = Some(*address);
                        metric_index = 0;
                    }
                    if metric_index >= SENSOR_COLUMN_SPECS.len() {
                        return Err(ParserError::InvalidHeader {
                            parser: Self::NAME,
                            row_index: 4,
                            message: format!(
                                "sensor {address} had more than {} characteristics",
                                SENSOR_COLUMN_SPECS.len()
                            ),
                        });
                    }
                    if !value.eq_ignore_ascii_case("Smp") {
                        return Err(ParserError::InvalidHeader {
                            parser: Self::NAME,
                            row_index: 4,
                            message: format!(
                                "unexpected characteristic '{value}' for column '{}'",
                                columns.get(idx).unwrap_or("")
                            ),
                        });
                    }
                    metric_index += 1;
                }
                ColumnRole::SensorAddress { .. } | ColumnRole::SensorMetric { .. } => {}
            }
        }

        if let Some(address) = current_address {
            if metric_index != SENSOR_COLUMN_SPECS.len() {
                return Err(ParserError::InvalidHeader {
                    parser: Self::NAME,
                    row_index: 4,
                    message: format!(
                        "sensor {address} had {metric_index} characteristics, expected {}",
                        SENSOR_COLUMN_SPECS.len()
                    ),
                });
            }
        }

        Ok(())
    }
}

impl SapflowParser for SapFlowAllParser {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn parse(&self, content: &str) -> Result<ParsedFileData, ParserError> {
        self.parse_with_builder(Self::reader_builder(), content)
    }
}
