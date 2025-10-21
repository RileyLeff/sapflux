use csv::StringRecord;

use crate::errors::ParserError;
use crate::model::{ParsedFileData, Sdi12Address, ThermistorDepth};
use crate::registry::SapflowParser;

use super::{
    build_logger_dataframe, derive_logger_id_from_header, make_logger_data, parse_metadata,
    parse_optional_f64, parse_required_i64, ColumnRole, LoggerColumnKind, LoggerColumns,
    SensorFrameBuilder, SensorMetric, ThermistorMetric,
};

pub struct Cr300HxParser;

impl Default for Cr300HxParser {
    fn default() -> Self {
        Self
    }
}

impl Cr300HxParser {
    const NAME: &'static str = "CR300_HX";

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
                s if s.starts_with("sdi") => {
                    return Ok(ColumnRole::SensorAddress { address });
                }
                "sapflwtot" => {
                    return Ok(ColumnRole::SensorMetric {
                        address,
                        metric: SensorMetric::TotalSapFlow,
                    });
                }
                "vhouter" | "vhout" => {
                    return Ok(ColumnRole::ThermistorMetric {
                        address,
                        depth: ThermistorDepth::Outer,
                        metric: ThermistorMetric::SapFluxDensity,
                    });
                }
                "vhinner" | "vhin" => {
                    return Ok(ColumnRole::ThermistorMetric {
                        address,
                        depth: ThermistorDepth::Inner,
                        metric: ThermistorMetric::SapFluxDensity,
                    });
                }
                "alphaout" | "alphout" => {
                    return Ok(ColumnRole::ThermistorMetric {
                        address,
                        depth: ThermistorDepth::Outer,
                        metric: ThermistorMetric::Alpha,
                    });
                }
                "alphain" | "alphinn" => {
                    return Ok(ColumnRole::ThermistorMetric {
                        address,
                        depth: ThermistorDepth::Inner,
                        metric: ThermistorMetric::Alpha,
                    });
                }
                "betaout" | "betout" => {
                    return Ok(ColumnRole::ThermistorMetric {
                        address,
                        depth: ThermistorDepth::Outer,
                        metric: ThermistorMetric::Beta,
                    });
                }
                "betain" | "betinn" => {
                    return Ok(ColumnRole::ThermistorMetric {
                        address,
                        depth: ThermistorDepth::Inner,
                        metric: ThermistorMetric::Beta,
                    });
                }
                "tmaxtout" | "tmxtout" => {
                    return Ok(ColumnRole::ThermistorMetric {
                        address,
                        depth: ThermistorDepth::Outer,
                        metric: ThermistorMetric::TimeToMaxDownstream,
                    });
                }
                "tmaxtin" | "tmxtin" => {
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

    fn parse_timestamp_lenient(value: &str, line_index: usize) -> Result<i64, ParserError> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(ParserError::DataRow {
                parser: Self::NAME,
                line_index,
                message: "timestamp column missing".to_string(),
            });
        }

        if !trimmed.contains('/') {
            return Err(ParserError::FormatMismatch {
                parser: Self::NAME,
                reason: "timestamp format does not match HX expectations".to_string(),
            });
        }

        static FORMATS: &[&str] = &["%m/%d/%y %H:%M", "%m/%d/%Y %H:%M"];

        for fmt in FORMATS {
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(trimmed, fmt) {
                return Ok(dt.and_utc().timestamp_micros());
            }
        }

        Err(ParserError::DataRow {
            parser: Self::NAME,
            line_index,
            message: format!("invalid timestamp '{trimmed}'"),
        })
    }

    fn validate_units(units: &StringRecord) -> Result<(), ParserError> {
        const EXPECTED: &[&str] = &[
            "TS",
            "RN",
            "Volts",
            "",
            "",
            "literPerHour",
            "heatVelocity",
            "heatVelocity",
            "logTRatio",
            "logTRatio",
            "logTRatio",
            "logTRatio",
            "second",
            "second",
        ];
        if units.len() != EXPECTED.len() {
            return Err(ParserError::FormatMismatch {
                parser: Self::NAME,
                reason: format!(
                    "expected {} unit columns, found {}",
                    EXPECTED.len(),
                    units.len()
                ),
            });
        }
        for (idx, (found, expected)) in units.iter().zip(EXPECTED.iter()).enumerate() {
            if !found.eq_ignore_ascii_case(expected) {
                return Err(ParserError::FormatMismatch {
                    parser: Self::NAME,
                    reason: format!("unexpected value '{found}' at units column {idx}"),
                });
            }
        }
        Ok(())
    }

    fn validate_characteristics(characteristics: &StringRecord) -> Result<(), ParserError> {
        const EXPECTED: &[&str] = &[
            "", "", "Min", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp",
            "Smp",
        ];
        if characteristics.len() != EXPECTED.len() {
            return Err(ParserError::FormatMismatch {
                parser: Self::NAME,
                reason: format!(
                    "expected {} characteristic columns, found {}",
                    EXPECTED.len(),
                    characteristics.len()
                ),
            });
        }
        for (idx, (found, expected)) in characteristics.iter().zip(EXPECTED.iter()).enumerate() {
            if !found.eq_ignore_ascii_case(expected) {
                return Err(ParserError::FormatMismatch {
                    parser: Self::NAME,
                    reason: format!("unexpected value '{found}' at measurement column {idx}"),
                });
            }
        }
        Ok(())
    }

    fn parse_logger_id(value: &str, line_index: usize) -> Result<Option<String>, ParserError> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        trimmed
            .parse::<i64>()
            .map(|value| Some(value.to_string()))
            .map_err(|err| ParserError::DataRow {
                parser: Self::NAME,
                line_index,
                message: format!("failed to parse logger id: {err}"),
            })
    }

    fn update_canonical_logger_id(
        line_index: usize,
        canonical: &mut Option<String>,
        observed_non_default: &mut Option<String>,
        candidate: &str,
    ) -> Result<(), ParserError> {
        if candidate == "1" {
            if canonical.is_none() {
                *canonical = Some("1".to_string());
            }
            return Ok(());
        }

        if let Some(ref existing) = observed_non_default {
            if existing != candidate {
                return Err(ParserError::Validation {
                    parser: Self::NAME,
                    message: format!(
                        "logger id column contained conflicting values '{existing}' and '{candidate}' (line {line_index})"
                    ),
                });
            }
        } else {
            *observed_non_default = Some(candidate.to_string());
        }

        *canonical = Some(candidate.to_string());
        Ok(())
    }
}

impl SapflowParser for Cr300HxParser {
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

        if !metadata
            .table_name
            .to_ascii_lowercase()
            .starts_with("table")
        {
            return Err(ParserError::FormatMismatch {
                parser: Self::NAME,
                reason: format!(
                    "table name '{}' does not match expected HX table",
                    metadata.table_name
                ),
            });
        }

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
        Self::validate_characteristics(&characteristics)?;

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
        let mut observed_non_default_id: Option<String> = None;
        let mut logger_id_column_present = false;
        let mut logger_id_values: Vec<Option<String>> = Vec::new();

        for (row_idx, record) in records.enumerate() {
            let record = record.map_err(|err| ParserError::Csv {
                parser: Self::NAME,
                source: err,
            })?;

            if record.len() < column_roles.len() {
                continue;
            }

            let line_index = row_idx + 5;

            let mut row_valid = true;
            let mut row_timestamp: Option<i64> = None;
            let mut row_record: Option<i64> = None;
            let mut row_battery: Option<Option<f64>> = None;
            let mut row_panel: Option<Option<f64>> = None;
            let mut row_logger_id: Option<String> = None;
            let mut pending_thermistor_metrics: Vec<(
                Sdi12Address,
                ThermistorDepth,
                ThermistorMetric,
                Option<f64>,
            )> = Vec::new();

            for (idx, role) in column_roles.iter().enumerate() {
                let header_name = columns.get(idx).unwrap_or("");
                let value = record.get(idx).unwrap_or("");
                match role {
                    ColumnRole::Logger(kind) => match kind {
                        LoggerColumnKind::Timestamp => {
                            let micros = Self::parse_timestamp_lenient(value, line_index)?;
                            row_timestamp = Some(micros);
                        }
                        LoggerColumnKind::Record => {
                            let record_value =
                                parse_required_i64(Self::NAME, value, line_index, header_name)?;

                            if let Some(prev) = previous_record {
                                if record_value != prev + 1 {
                                    row_valid = false;
                                    break;
                                }
                            }
                            previous_record = Some(record_value);
                            row_record = Some(record_value);
                        }
                        LoggerColumnKind::BatteryVoltage => {
                            let parsed =
                                parse_optional_f64(Self::NAME, value, line_index, header_name)?;
                            row_battery = Some(parsed);
                        }
                        LoggerColumnKind::PanelTemperature => {
                            let parsed =
                                parse_optional_f64(Self::NAME, value, line_index, header_name)?;
                            row_panel = Some(parsed);
                        }
                        LoggerColumnKind::LoggerId => {
                            logger_id_column_present = true;
                            let parsed = Self::parse_logger_id(value, line_index)?;
                            row_logger_id = parsed;
                        }
                    },
                    ColumnRole::ThermistorMetric {
                        address,
                        depth,
                        metric,
                    } => {
                        let parsed =
                            parse_optional_f64(Self::NAME, value, line_index, header_name)?;
                        pending_thermistor_metrics.push((*address, *depth, *metric, parsed));
                    }
                    ColumnRole::SensorAddress { .. } | ColumnRole::SensorMetric { .. } => {}
                }
            }

            if !row_valid {
                continue;
            }

            let timestamp = match row_timestamp {
                Some(value) => value,
                None => continue,
            };
            let record_value = match row_record {
                Some(value) => value,
                None => continue,
            };

            logger_columns.timestamp.push(timestamp);
            logger_columns.record.push(record_value);

            if let Some(value_str) = row_logger_id.as_deref() {
                Self::update_canonical_logger_id(
                    line_index,
                    &mut canonical_logger_id,
                    &mut observed_non_default_id,
                    value_str,
                )?;
            }
            logger_id_values.push(row_logger_id.clone());

            let battery_value = row_battery.unwrap_or(None);
            logger_columns.battery_mut().push(battery_value);

            let panel_value = row_panel.unwrap_or(None);
            logger_columns.panel_mut().push(panel_value);

            for (address, depth, metric, parsed) in pending_thermistor_metrics {
                sensor_builder.push_thermistor_metric(address, depth, metric, parsed);
            }

            row_count += 1;
        }

        if row_count == 0 {
            return Err(ParserError::EmptyData { parser: Self::NAME });
        }

        if logger_id_column_present {
            let canonical = match canonical_logger_id {
                Some(ref value) if value != "1" => value.clone(),
                Some(ref value) => value.clone(),
                None => derive_logger_id_from_header(Self::NAME, &metadata)?,
            };

            let filled: Vec<Option<String>> = logger_id_values
                .into_iter()
                .map(|_| Some(canonical.clone()))
                .collect();
            logger_columns.logger_id = Some(filled);
        } else if logger_columns.logger_id.is_none() {
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
