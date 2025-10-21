use csv::StringRecord;

use crate::errors::ParserError;
use crate::model::{ParsedFileData, Sdi12Address, ThermistorDepth};
use crate::registry::SapflowParser;

use super::{
    build_logger_dataframe, derive_logger_id_from_header, make_logger_data, parse_metadata,
    parse_optional_f64, parse_optional_i64, parse_required_i64, parse_sdi12_address,
    parse_timestamp, ColumnRole, LoggerColumnKind, LoggerColumns, SensorFrameBuilder, SensorMetric,
    ThermistorMetric,
};

pub struct Cr200TableParser;

impl Default for Cr200TableParser {
    fn default() -> Self {
        Self
    }
}

impl Cr200TableParser {
    const NAME: &'static str = "CR200_TABLE";

    fn classify_columns(columns: &StringRecord) -> Result<Vec<ColumnRole>, ParserError> {
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
        if trimmed.eq_ignore_ascii_case("battv")
            || trimmed.eq_ignore_ascii_case("battv_min")
            || trimmed.eq_ignore_ascii_case("batt_volt")
        {
            return Ok(ColumnRole::Logger(LoggerColumnKind::BatteryVoltage));
        }
        if trimmed.eq_ignore_ascii_case("ptemp_c") {
            return Ok(ColumnRole::Logger(LoggerColumnKind::PanelTemperature));
        }
        if trimmed.eq_ignore_ascii_case("id") {
            return Ok(ColumnRole::Logger(LoggerColumnKind::LoggerId));
        }

        if let Some((base, address)) = Self::split_address(trimmed) {
            let lower = base.to_ascii_lowercase();
            match lower.as_str() {
                s if s.starts_with("sdi") || s.starts_with("sensoraddress") => {
                    return Ok(ColumnRole::SensorAddress { address });
                }
                "sapflwtot" => {
                    return Ok(ColumnRole::SensorMetric {
                        address,
                        metric: SensorMetric::TotalSapFlow,
                    });
                }
                "vhout" | "vhouter" => {
                    return Ok(ColumnRole::ThermistorMetric {
                        address,
                        depth: ThermistorDepth::Outer,
                        metric: ThermistorMetric::SapFluxDensity,
                    });
                }
                "vhin" | "vhinner" => {
                    return Ok(ColumnRole::ThermistorMetric {
                        address,
                        depth: ThermistorDepth::Inner,
                        metric: ThermistorMetric::SapFluxDensity,
                    });
                }
                "alphaout" => {
                    return Ok(ColumnRole::ThermistorMetric {
                        address,
                        depth: ThermistorDepth::Outer,
                        metric: ThermistorMetric::Alpha,
                    });
                }
                "alphain" => {
                    return Ok(ColumnRole::ThermistorMetric {
                        address,
                        depth: ThermistorDepth::Inner,
                        metric: ThermistorMetric::Alpha,
                    });
                }
                "betaout" => {
                    return Ok(ColumnRole::ThermistorMetric {
                        address,
                        depth: ThermistorDepth::Outer,
                        metric: ThermistorMetric::Beta,
                    });
                }
                "betain" => {
                    return Ok(ColumnRole::ThermistorMetric {
                        address,
                        depth: ThermistorDepth::Inner,
                        metric: ThermistorMetric::Beta,
                    });
                }
                "tmaxtout" => {
                    return Ok(ColumnRole::ThermistorMetric {
                        address,
                        depth: ThermistorDepth::Outer,
                        metric: ThermistorMetric::TimeToMaxDownstream,
                    });
                }
                "tmaxtin" => {
                    return Ok(ColumnRole::ThermistorMetric {
                        address,
                        depth: ThermistorDepth::Inner,
                        metric: ThermistorMetric::TimeToMaxDownstream,
                    });
                }
                other => {
                    return Err(ParserError::FormatMismatch {
                        parser: Self::NAME,
                        reason: format!("unrecognized sensor column '{other}'"),
                    });
                }
            }
        }

        Err(ParserError::FormatMismatch {
            parser: Self::NAME,
            reason: format!("unrecognized column '{trimmed}'"),
        })
    }

    fn split_address(name: &str) -> Option<(&str, Sdi12Address)> {
        if name.is_empty() {
            return None;
        }
        let mut chars = name.chars();
        let addr = chars.next_back()?;
        let address = Sdi12Address::new(addr).ok()?;
        let base_len = name.len() - addr.len_utf8();
        if base_len == 0 {
            return None;
        }
        Some((&name[..base_len], address))
    }

    fn validate_table_name(metadata: &crate::model::FileMetadata) -> Result<(), ParserError> {
        let lower = metadata.table_name.to_ascii_lowercase();
        if lower.starts_with("table") {
            Ok(())
        } else {
            Err(ParserError::FormatMismatch {
                parser: Self::NAME,
                reason: format!(
                    "table name '{}' does not match expected CR200 tables",
                    metadata.table_name
                ),
            })
        }
    }

    fn validate_units(units: &StringRecord) -> Result<(), ParserError> {
        const EXPECTED: &[&str] = &[
            "TS",
            "RN",
            "Volts",
            "",
            "",
            "UNIT_SENSOR",
            "UNIT_SENSOR",
            "UNIT_SENSOR",
            "UNIT_SENSOR",
            "UNIT_SENSOR",
            "UNIT_SENSOR",
            "UNIT_SENSOR",
            "UNIT_SENSOR",
            "UNIT_SENSOR",
        ];
        if units.len() != EXPECTED.len() {
            return Err(ParserError::InvalidHeader {
                parser: Self::NAME,
                row_index: 3,
                message: format!(
                    "expected {} unit columns, found {}",
                    EXPECTED.len(),
                    units.len()
                ),
            });
        }
        for (idx, (found, expected)) in units.iter().zip(EXPECTED.iter()).enumerate() {
            let ok = match *expected {
                "UNIT_SENSOR" => matches!(
                    found,
                    "unit"
                        | "UNIT"
                        | "literPerHour"
                        | "literPerHo"
                        | "heatVelocity"
                        | "heatVeloci"
                        | "logTRatio"
                        | "second"
                ),
                other => found == other,
            };
            if !ok {
                return Err(ParserError::InvalidHeader {
                    parser: Self::NAME,
                    row_index: 3,
                    message: format!("unexpected value '{found}' at units column {idx}"),
                });
            }
        }
        Ok(())
    }

    fn validate_measurements(characteristics: &StringRecord) -> Result<(), ParserError> {
        const EXPECTED: &[&str] = &[
            "", "", "Min", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp",
            "Smp",
        ];
        if characteristics.len() != EXPECTED.len() {
            return Err(ParserError::InvalidHeader {
                parser: Self::NAME,
                row_index: 4,
                message: format!(
                    "expected {} characteristic columns, found {}",
                    EXPECTED.len(),
                    characteristics.len()
                ),
            });
        }
        for (idx, (found, expected)) in characteristics.iter().zip(EXPECTED.iter()).enumerate() {
            if found != *expected {
                return Err(ParserError::InvalidHeader {
                    parser: Self::NAME,
                    row_index: 4,
                    message: format!("unexpected value '{found}' at measurement column {idx}"),
                });
            }
        }
        Ok(())
    }
}

impl SapflowParser for Cr200TableParser {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn parse(&self, content: &str) -> Result<ParsedFileData, ParserError> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(content.as_bytes());

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
        Self::validate_table_name(&metadata)?;

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

        Self::validate_units(&units)?;
        Self::validate_measurements(&characteristics)?;

        if units.len() != columns.len() || characteristics.len() != columns.len() {
            return Err(ParserError::FormatMismatch {
                parser: Self::NAME,
                reason: "header rows have inconsistent column counts".to_string(),
            });
        }

        let column_roles = Self::classify_columns(&columns)?;

        let mut logger_columns = LoggerColumns::new(0);
        let mut sensor_builder = SensorFrameBuilder::new();
        let mut row_count = 0usize;
        let mut previous_record: Option<i64> = None;
        let mut canonical_logger_id: Option<String> = None;
        let mut logger_id_values: Vec<Option<String>> = Vec::new();
        let mut logger_id_column_present = false;

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
                                if record_value <= prev {
                                    return Err(ParserError::DataRow {
                                        parser: Self::NAME,
                                        line_index,
                                        message: format!(
                                            "record column must be strictly increasing ({} >= {})",
                                            prev, record_value
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
                            logger_id_column_present = true;
                            let parsed =
                                parse_optional_i64(Self::NAME, value, line_index, header_name)?;

                            if let Some(value) = parsed {
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
                                logger_id_values.push(Some(value_str));
                            } else {
                                logger_id_values.push(None);
                            }
                        }
                    },
                    ColumnRole::SensorAddress { address } => {
                        let parsed =
                            parse_sdi12_address(Self::NAME, value, line_index, header_name)?;
                        if parsed != *address {
                            return Err(ParserError::DataRow {
                                parser: Self::NAME,
                                line_index,
                                message: format!(
                                    "column '{header_name}' expected SDI-12 address '{}' but found '{}'",
                                    address,
                                    value.trim()
                                ),
                            });
                        }
                    }
                    ColumnRole::SensorMetric { address, metric } => {
                        let parsed =
                            parse_optional_f64(Self::NAME, value, line_index, header_name)?;
                        sensor_builder.push_sensor_metric(*address, *metric, parsed);
                    }
                    ColumnRole::ThermistorMetric {
                        address,
                        depth,
                        metric,
                    } => {
                        let parsed =
                            parse_optional_f64(Self::NAME, value, line_index, header_name)?;
                        sensor_builder.push_thermistor_metric(*address, *depth, *metric, parsed);
                    }
                }
            }

            row_count += 1;
        }

        if row_count == 0 {
            return Err(ParserError::EmptyData { parser: Self::NAME });
        }

        if logger_id_column_present {
            if canonical_logger_id.is_none() {
                canonical_logger_id = Some(derive_logger_id_from_header(Self::NAME, &metadata)?);
            }

            let canonical = canonical_logger_id.expect("canonical logger id must exist");
            let mut any_observed = false;
            for entry in logger_id_values.iter_mut() {
                if let Some(value) = entry {
                    any_observed = true;
                    if value != &canonical {
                        return Err(ParserError::Validation {
                            parser: Self::NAME,
                            message: format!(
                                "logger id column contained inconsistent values ({} != {})",
                                value, canonical
                            ),
                        });
                    }
                } else {
                    *entry = Some(canonical.clone());
                }
            }

            if !any_observed {
                for entry in logger_id_values.iter_mut() {
                    *entry = Some(canonical.clone());
                }
            }

            logger_columns.logger_id = Some(logger_id_values);
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
}
