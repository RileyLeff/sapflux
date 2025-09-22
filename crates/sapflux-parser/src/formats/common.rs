use std::collections::HashMap;

use chrono::NaiveDateTime;
use polars::prelude::*;

use crate::errors::ParserError;
use crate::model::{
    FileMetadata, LoggerData, Sdi12Address, SensorData, ThermistorDepth, ThermistorPairData,
};

#[derive(Debug, Clone, Copy)]
pub enum LoggerColumnKind {
    Timestamp,
    Record,
    BatteryVoltage,
    PanelTemperature,
    LoggerId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SensorMetric {
    TotalSapFlow,
}

impl SensorMetric {
    pub fn canonical_name(&self) -> &'static str {
        match self {
            SensorMetric::TotalSapFlow => "total_sap_flow_lph",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ThermistorMetric {
    SapFluxDensity,
    Alpha,
    Beta,
    TimeToMaxDownstream,
    PrePulseTempDownstream,
    MaxTempRiseDownstream,
    PostPulseTempDownstream,
    TimeToMaxUpstream,
    PrePulseTempUpstream,
    MaxTempRiseUpstream,
    PostPulseTempUpstream,
}

impl ThermistorMetric {
    pub fn canonical_name(&self) -> &'static str {
        match self {
            ThermistorMetric::SapFluxDensity => "sap_flux_density_cmh",
            ThermistorMetric::Alpha => "alpha",
            ThermistorMetric::Beta => "beta",
            ThermistorMetric::TimeToMaxDownstream => "time_to_max_temp_downstream_s",
            ThermistorMetric::PrePulseTempDownstream => "pre_pulse_temp_downstream_c",
            ThermistorMetric::MaxTempRiseDownstream => "max_temp_rise_downstream_c",
            ThermistorMetric::PostPulseTempDownstream => "post_pulse_temp_downstream_c",
            ThermistorMetric::TimeToMaxUpstream => "time_to_max_temp_upstream_s",
            ThermistorMetric::PrePulseTempUpstream => "pre_pulse_temp_upstream_c",
            ThermistorMetric::MaxTempRiseUpstream => "max_temp_rise_upstream_c",
            ThermistorMetric::PostPulseTempUpstream => "post_pulse_temp_upstream_c",
        }
    }
}

#[derive(Debug, Clone)]
pub struct LoggerColumns {
    pub timestamp: Vec<i64>,
    pub record: Vec<i64>,
    pub battery_voltage: Option<Vec<Option<f64>>>,
    pub panel_temperature: Option<Vec<Option<f64>>>,
    pub logger_id: Option<Vec<Option<String>>>,
}

impl LoggerColumns {
    pub fn new(capacity: usize) -> Self {
        Self {
            timestamp: Vec::with_capacity(capacity),
            record: Vec::with_capacity(capacity),
            battery_voltage: None,
            panel_temperature: None,
            logger_id: None,
        }
    }

    pub(crate) fn battery_mut(&mut self) -> &mut Vec<Option<f64>> {
        if self.battery_voltage.is_none() {
            self.battery_voltage = Some(Vec::with_capacity(self.timestamp.capacity()));
        }
        self.battery_voltage.as_mut().unwrap()
    }

    pub(crate) fn panel_mut(&mut self) -> &mut Vec<Option<f64>> {
        if self.panel_temperature.is_none() {
            self.panel_temperature = Some(Vec::with_capacity(self.timestamp.capacity()));
        }
        self.panel_temperature.as_mut().unwrap()
    }

    pub(crate) fn logger_id_mut(&mut self) -> &mut Vec<Option<String>> {
        if self.logger_id.is_none() {
            self.logger_id = Some(Vec::with_capacity(self.timestamp.capacity()));
        }
        self.logger_id.as_mut().unwrap()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ColumnRole {
    Logger(LoggerColumnKind),
    SensorAddress {
        address: Sdi12Address,
    },
    SensorMetric {
        address: Sdi12Address,
        metric: SensorMetric,
    },
    ThermistorMetric {
        address: Sdi12Address,
        depth: ThermistorDepth,
        metric: ThermistorMetric,
    },
}

#[derive(Default)]
struct SensorAccumulator {
    sensor_metric_order: Vec<SensorMetric>,
    sensor_metric_values: HashMap<SensorMetric, Vec<Option<f64>>>,
    depth_order: Vec<ThermistorDepth>,
    pair_metric_order: HashMap<ThermistorDepth, Vec<ThermistorMetric>>,
    pair_metric_values: HashMap<(ThermistorDepth, ThermistorMetric), Vec<Option<f64>>>,
}

impl SensorAccumulator {
    fn ensure_metric(&mut self, metric: SensorMetric) -> &mut Vec<Option<f64>> {
        if !self.sensor_metric_order.contains(&metric) {
            self.sensor_metric_order.push(metric);
        }
        self.sensor_metric_values
            .entry(metric)
            .or_insert_with(Vec::new)
    }

    fn ensure_pair_metric(
        &mut self,
        depth: ThermistorDepth,
        metric: ThermistorMetric,
    ) -> &mut Vec<Option<f64>> {
        if !self.depth_order.contains(&depth) {
            self.depth_order.push(depth);
        }
        let order = self.pair_metric_order.entry(depth).or_insert_with(Vec::new);
        if !order.contains(&metric) {
            order.push(metric);
        }
        self.pair_metric_values
            .entry((depth, metric))
            .or_insert_with(Vec::new)
    }
}

pub(crate) struct SensorFrameBuilder {
    order: Vec<Sdi12Address>,
    sensors: HashMap<Sdi12Address, SensorAccumulator>,
}

impl SensorFrameBuilder {
    pub fn new() -> Self {
        Self {
            order: Vec::new(),
            sensors: HashMap::new(),
        }
    }

    fn ensure_sensor(&mut self, address: Sdi12Address) -> &mut SensorAccumulator {
        if !self.order.iter().any(|addr| *addr == address) {
            self.order.push(address);
        }
        self.sensors
            .entry(address)
            .or_insert_with(SensorAccumulator::default)
    }

    pub fn push_sensor_metric(
        &mut self,
        address: Sdi12Address,
        metric: SensorMetric,
        value: Option<f64>,
    ) {
        let acc = self.ensure_sensor(address);
        acc.ensure_metric(metric).push(value);
    }

    pub fn push_thermistor_metric(
        &mut self,
        address: Sdi12Address,
        depth: ThermistorDepth,
        metric: ThermistorMetric,
        value: Option<f64>,
    ) {
        let acc = self.ensure_sensor(address);
        acc.ensure_pair_metric(depth, metric).push(value);
    }

    pub fn build(
        self,
        parser: &'static str,
        row_count: usize,
    ) -> Result<Vec<SensorData>, ParserError> {
        let mut sensors = Vec::with_capacity(self.order.len());
        for address in self.order {
            let acc = self
                .sensors
                .get(&address)
                .expect("sensor accumulator missing");

            let sensor_df = if acc.sensor_metric_order.is_empty() {
                None
            } else {
                let mut columns = Vec::with_capacity(acc.sensor_metric_order.len());
                for metric in &acc.sensor_metric_order {
                    let data = acc
                        .sensor_metric_values
                        .get(metric)
                        .expect("missing sensor metric vector");
                    if data.len() != row_count {
                        return Err(ParserError::Validation {
                            parser,
                            message: format!(
                                "sensor {address} metric {} had {} rows, expected {row_count}",
                                metric.canonical_name(),
                                data.len()
                            ),
                        });
                    }
                columns.push(Series::new(metric.canonical_name().into(), data.clone()).into());
                }
                Some(
                    DataFrame::new(columns).map_err(|err| ParserError::Validation {
                        parser,
                        message: format!(
                            "failed to construct sensor dataframe for {address}: {err}"
                        ),
                    })?,
                )
            };

            let mut thermistor_pairs = Vec::new();
            for depth in &acc.depth_order {
                let metrics = acc
                    .pair_metric_order
                    .get(depth)
                    .expect("missing depth metric order");
                let mut columns = Vec::with_capacity(metrics.len());
                for metric in metrics {
                    let key = (*depth, *metric);
                    let data = acc
                        .pair_metric_values
                        .get(&key)
                        .expect("missing thermistor metric vector");
                    if data.len() != row_count {
                        return Err(ParserError::Validation {
                            parser,
                            message: format!(
                                "sensor {address} {depth} metric {} had {} rows, expected {row_count}",
                                metric.canonical_name(),
                                data.len()
                            ),
                        });
                    }
                    columns.push(Series::new(metric.canonical_name().into(), data.clone()).into());
                }
                let df = DataFrame::new(columns).map_err(|err| ParserError::Validation {
                    parser,
                    message: format!(
                        "failed to construct thermistor dataframe for sensor {address} depth {depth}: {err}"
                    ),
                })?;
                thermistor_pairs.push(ThermistorPairData { depth: *depth, df });
            }

            sensors.push(SensorData {
                sdi12_address: address,
                sensor_df,
                thermistor_pairs,
            });
        }

        Ok(sensors)
    }
}

pub(crate) fn build_logger_dataframe(
    parser: &'static str,
    mut columns: LoggerColumns,
) -> Result<DataFrame, ParserError> {
    if columns.timestamp.len() != columns.record.len() {
        return Err(ParserError::Validation {
            parser,
            message: format!(
                "timestamp column had {} rows but record column had {} rows",
                columns.timestamp.len(),
                columns.record.len()
            ),
        });
    }

    let ts_series = Series::new("timestamp".into(), columns.timestamp);
    let ts_series = ts_series
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
        .map_err(|err| ParserError::Validation {
            parser,
            message: format!("failed to cast timestamp column: {err}"),
        })?;

    let mut cols: Vec<Column> = Vec::new();
    cols.push(ts_series.into());
    cols.push(Series::new("record".into(), columns.record).into());

    if let Some(values) = columns.battery_voltage.take() {
        cols.push(Series::new("battery_voltage_v".into(), values).into());
    }

    if let Some(values) = columns.panel_temperature.take() {
        cols.push(Series::new("panel_temperature_c".into(), values).into());
    }

    if let Some(values) = columns.logger_id.take() {
        let utf8: Vec<Option<&str>> = values.iter().map(|v| v.as_deref()).collect();
        cols.push(Series::new("logger_id".into(), utf8).into());
    }

    DataFrame::new(cols).map_err(|err| ParserError::Validation {
        parser,
        message: format!("failed to build logger dataframe: {err}"),
    })
}

pub(crate) fn parse_metadata(
    parser: &'static str,
    header: &csv::StringRecord,
) -> Result<FileMetadata, ParserError> {
    if header.len() < 8 {
        return Err(ParserError::FormatMismatch {
            parser,
            reason: format!("expected at least 8 header fields, found {}", header.len()),
        });
    }

    let file_format = header.get(0).unwrap_or_default();
    if !file_format.eq_ignore_ascii_case("toa5") {
        return Err(ParserError::FormatMismatch {
            parser,
            reason: format!("unsupported file format '{file_format}'"),
        });
    }

    let logger_name = header.get(1).unwrap_or_default().to_string();
    let logger_type = header.get(2).unwrap_or_default().to_string();
    let serial_number = clean_optional(header.get(3));
    let os_version = clean_optional(header.get(4));
    let program_name = header.get(5).unwrap_or_default().to_string();
    let signature = clean_optional(header.get(6));
    let table_name = header.get(7).unwrap_or_default().to_string();

    Ok(FileMetadata::new(
        file_format.to_string(),
        logger_name,
        logger_type,
        serial_number,
        os_version,
        program_name,
        signature,
        table_name,
    ))
}

fn clean_optional(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
}

pub(crate) fn parse_timestamp(
    parser: &'static str,
    value: &str,
    line_index: usize,
) -> Result<i64, ParserError> {
    static FORMATS: &[&str] = &["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"];
    let trimmed = value.trim();
    for fmt in FORMATS {
        if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, fmt) {
            let micros = dt.and_utc().timestamp_micros();
            return Ok(micros);
        }
    }
    Err(ParserError::DataRow {
        parser,
        line_index,
        message: format!("invalid timestamp '{trimmed}'"),
    })
}

pub(crate) fn parse_required_i64(
    parser: &'static str,
    value: &str,
    line_index: usize,
    column: &str,
) -> Result<i64, ParserError> {
    value
        .trim()
        .parse::<i64>()
        .map_err(|err| ParserError::DataRow {
            parser,
            line_index,
            message: format!("failed to parse column '{column}' as integer: {err}"),
        })
}

pub(crate) fn parse_optional_i64(
    parser: &'static str,
    value: &str,
    line_index: usize,
    column: &str,
) -> Result<Option<i64>, ParserError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("nan") {
        return Ok(None);
    }
    trimmed
        .parse::<i64>()
        .map(Some)
        .map_err(|err| ParserError::DataRow {
            parser,
            line_index,
            message: format!("failed to parse column '{column}' as integer: {err}"),
        })
}

pub(crate) fn parse_optional_f64(
    parser: &'static str,
    value: &str,
    line_index: usize,
    column: &str,
) -> Result<Option<f64>, ParserError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("nan") {
        return Ok(None);
    }

    match trimmed.parse::<f64>() {
        Ok(parsed) => {
            if (parsed + 99.0).abs() < f64::EPSILON {
                Ok(None)
            } else {
                Ok(Some(parsed))
            }
        }
        Err(err) => Err(ParserError::DataRow {
            parser,
            line_index,
            message: format!("failed to parse column '{column}' as float: {err}"),
        }),
    }
}

pub(crate) fn parse_sdi12_address(
    parser: &'static str,
    value: &str,
    line_index: usize,
    column: &str,
) -> Result<Sdi12Address, ParserError> {
    Sdi12Address::try_from(value).map_err(|err| ParserError::DataRow {
        parser,
        line_index,
        message: format!("{column}: {err}"),
    })
}

pub(crate) fn make_logger_data(df: DataFrame, sensors: Vec<SensorData>) -> LoggerData {
    LoggerData { df, sensors }
}

pub(crate) fn derive_logger_id_from_header(
    parser: &'static str,
    metadata: &FileMetadata,
) -> Result<String, ParserError> {
    let raw = metadata.logger_name.trim();
    let digits: String = raw
        .chars()
        .rev()
        .take_while(|c| c.is_ascii_digit())
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();

    if digits.is_empty() {
        return Err(ParserError::Validation {
            parser,
            message: format!("unable to derive logger_id from header '{raw}'"),
        });
    }

    Ok(digits)
}
