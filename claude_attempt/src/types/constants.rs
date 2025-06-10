use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalculationParameters {
    pub wound_diameter_cm: f64,
    pub sapwood_green_weight_kg: f64, 
    pub sapwood_dry_weight_kg: f64,
    pub thermal_diffusivity_cm2_per_s: f64,
    pub heat_pulse_duration_s: f64,
    pub wound_correction_b: f64,
    pub time_since_pulse_s: f64,
    pub seconds_per_hour: f64,
    pub wood_density_kg_per_m3: f64,
    pub wood_specific_heat_j_per_kg_per_c: f64,
    pub gravimetric_water_content_kg_per_kg: f64,
    pub water_specific_heat_j_per_kg_per_c: f64,
    pub water_density_kg_per_m3: f64,
}

impl Default for CalculationParameters {
    fn default() -> Self {
        Self {
            wound_diameter_cm: 0.2,
            sapwood_green_weight_kg: 0.001,
            sapwood_dry_weight_kg: 0.005,
            thermal_diffusivity_cm2_per_s: 0.002409611,
            heat_pulse_duration_s: 3.0,
            wound_correction_b: 1.8905,
            time_since_pulse_s: 60.0,
            seconds_per_hour: 3600.0,
            wood_density_kg_per_m3: 500.0,
            wood_specific_heat_j_per_kg_per_c: 1000.0,
            gravimetric_water_content_kg_per_kg: 1.0,
            water_specific_heat_j_per_kg_per_c: 4182.0,
            water_density_kg_per_m3: 1000.0,
        }
    }
}

pub const FORBIDDEN_FILENAME_WORDS: &[&str] = &[
    "public", "Public", "status", "Status", "DataTableInfo"
];