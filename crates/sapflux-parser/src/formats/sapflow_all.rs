use csv::StringRecord;

use crate::errors::ParserError;
use crate::model::{ParsedFileData, Sdi12Address, ThermistorDepth};
use crate::registry::SapflowParser;

use super::{
    ColumnRole, LoggerColumnKind, LoggerColumns, SensorFrameBuilder, ThermistorMetric,
    build_logger_dataframe, make_logger_data, parse_metadata, parse_optional_f64,
    parse_optional_i64, parse_required_i64, parse_timestamp,
};

pub struct SapFlowAllParser;

impl Default for SapFlowAllParser {
    fn default() -> Self {
        Self
    }
}

impl SapFlowAllParser {
    const NAME: &'static str = "SAPFLOW_ALL";

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
            let (depth, metric) = match lower.as_str() {
                "alpout" => (ThermistorDepth::Outer, ThermistorMetric::Alpha),
                "alpinn" => (ThermistorDepth::Inner, ThermistorMetric::Alpha),
                "betout" => (ThermistorDepth::Outer, ThermistorMetric::Beta),
                "betinn" => (ThermistorDepth::Inner, ThermistorMetric::Beta),
                "tmxtout" => (
                    ThermistorDepth::Outer,
                    ThermistorMetric::TimeToMaxDownstream,
                ),
                "tmxtinn" => (
                    ThermistorDepth::Inner,
                    ThermistorMetric::TimeToMaxDownstream,
                ),
                "tpdsout" => (
                    ThermistorDepth::Outer,
                    ThermistorMetric::PrePulseTempDownstream,
                ),
                "tpdsinn" => (
                    ThermistorDepth::Inner,
                    ThermistorMetric::PrePulseTempDownstream,
                ),
                "dtdsout" => (
                    ThermistorDepth::Outer,
                    ThermistorMetric::MaxTempRiseDownstream,
                ),
                "dtdsinn" => (
                    ThermistorDepth::Inner,
                    ThermistorMetric::MaxTempRiseDownstream,
                ),
                "tsdsout" => (
                    ThermistorDepth::Outer,
                    ThermistorMetric::PostPulseTempDownstream,
                ),
                "tsdsinn" => (
                    ThermistorDepth::Inner,
                    ThermistorMetric::PostPulseTempDownstream,
                ),
                "tpusout" => (
                    ThermistorDepth::Outer,
                    ThermistorMetric::PrePulseTempUpstream,
                ),
                "tpusinn" => (
                    ThermistorDepth::Inner,
                    ThermistorMetric::PrePulseTempUpstream,
                ),
                "dtusout" => (
                    ThermistorDepth::Outer,
                    ThermistorMetric::MaxTempRiseUpstream,
                ),
                "dtusinn" => (
                    ThermistorDepth::Inner,
                    ThermistorMetric::MaxTempRiseUpstream,
                ),
                "tsusout" => (
                    ThermistorDepth::Outer,
                    ThermistorMetric::PostPulseTempUpstream,
                ),
                "tsusinn" => (
                    ThermistorDepth::Inner,
                    ThermistorMetric::PostPulseTempUpstream,
                ),
                "tmxtuso" => (ThermistorDepth::Outer, ThermistorMetric::TimeToMaxUpstream),
                "tmxtusi" => (ThermistorDepth::Inner, ThermistorMetric::TimeToMaxUpstream),
                other => {
                    return Err(ParserError::FormatMismatch {
                        parser: Self::NAME,
                        reason: format!("unrecognized sensor column suffix '{other}'"),
                    });
                }
            };

            return Ok(ColumnRole::ThermistorMetric {
                address,
                depth,
                metric,
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
        let mut parts = name.splitn(2, '_');
        let prefix = parts.next()?; // e.g. S0
        let suffix = parts.next()?; // e.g. AlpOut
        if prefix.len() != 2 {
            return None;
        }
        let addr_char = prefix.chars().nth(1)?;
        let address = Sdi12Address::new(addr_char).ok()?;
        Some((address, suffix))
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

        Self::validate_units(&columns, &units)?;
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
                            logger_columns.logger_id_mut().push(parsed);
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
                    ColumnRole::SensorAddress { .. } | ColumnRole::SensorMetric { .. } => {
                        return Err(ParserError::FormatMismatch {
                            parser: Self::NAME,
                            reason: "unexpected sensor column kind in SapFlowAll parser"
                                .to_string(),
                        });
                    }
                }
            }

            row_count += 1;
        }

        if row_count == 0 {
            return Err(ParserError::EmptyData { parser: Self::NAME });
        }

        let logger_df = build_logger_dataframe(Self::NAME, logger_columns)?;
        let sensors = sensor_builder.build(Self::NAME, row_count)?;
        let logger = make_logger_data(logger_df, sensors);

        Ok(ParsedFileData {
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
    fn validate_units(columns: &StringRecord, units: &StringRecord) -> Result<(), ParserError> {
        const EXPECTED_UNITS: &[&str] = &[
            "TS", "RN", "", "", "ratio", "ratio", "ratio", "ratio", "sec", "sec", "degC", "degC",
            "degC", "degC", "degC", "degC", "degC", "degC", "degC", "degC", "degC", "degC", "sec",
            "sec", "ratio", "ratio", "ratio", "ratio", "sec", "sec", "degC", "degC", "degC",
            "degC", "degC", "degC", "degC", "degC", "degC", "degC", "degC", "degC", "sec", "sec",
        ];
        if units.len() != EXPECTED_UNITS.len() {
            return Err(ParserError::InvalidHeader {
                parser: Self::NAME,
                row_index: 3,
                message: format!(
                    "expected {} unit columns, found {}",
                    EXPECTED_UNITS.len(),
                    units.len()
                ),
            });
        }
        for (idx, (found, expected)) in units.iter().zip(EXPECTED_UNITS.iter()).enumerate() {
            if found != *expected {
                return Err(ParserError::InvalidHeader {
                    parser: Self::NAME,
                    row_index: 3,
                    message: format!(
                        "unexpected units '{found}' for column '{}' at position {idx}",
                        columns.get(idx).unwrap_or("")
                    ),
                });
            }
        }
        Ok(())
    }

    fn validate_measurements(characteristics: &StringRecord) -> Result<(), ParserError> {
        const EXPECTED: &[&str] = &[
            "", "", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp",
            "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp",
            "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp",
            "Smp", "Smp", "Smp", "Smp", "Smp", "Smp", "Smp",
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

impl SapflowParser for SapFlowAllParser {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn parse(&self, content: &str) -> Result<ParsedFileData, ParserError> {
        self.parse_with_builder(Self::reader_builder(), content)
    }
}
