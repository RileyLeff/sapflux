use super::common::ThermistorMetric;

pub const LOGGER_COLUMNS: [&str; 5] = [
    "timestamp",
    "record",
    "battery_voltage_v",
    "panel_temperature_c",
    "logger_id",
];

pub fn required_thermistor_metrics() -> &'static [ThermistorMetric] {
    use ThermistorMetric::*;
    &[
        Alpha,
        Beta,
        TimeToMaxDownstream,
        TimeToMaxUpstream,
        PrePulseTempDownstream,
        MaxTempRiseDownstream,
        PostPulseTempDownstream,
        PrePulseTempUpstream,
        MaxTempRiseUpstream,
        PostPulseTempUpstream,
    ]
}
