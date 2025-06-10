use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SensorType {
    ImplexxOld,
    ImplexxNew,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorSpecs {
    pub sensor_id: SensorType,
    pub downstream_probe_distance_cm: f64,
    pub upstream_probe_distance_cm: f64,
    pub thermistor_depth_1_mm: f64, // "outer"
    pub thermistor_depth_2_mm: f64, // "inner"
}

impl SensorSpecs {
    pub fn from_sensor_type(sensor_type: SensorType) -> Self {
        match sensor_type {
            SensorType::ImplexxOld => Self {
                sensor_id: SensorType::ImplexxOld,
                downstream_probe_distance_cm: 0.6,
                upstream_probe_distance_cm: 0.6,
                thermistor_depth_1_mm: 10.0,
                thermistor_depth_2_mm: 20.0,
            },
            SensorType::ImplexxNew => Self {
                sensor_id: SensorType::ImplexxNew,
                downstream_probe_distance_cm: 0.8,
                upstream_probe_distance_cm: 0.8,
                thermistor_depth_1_mm: 10.0,
                thermistor_depth_2_mm: 20.0,
            },
        }
    }
}