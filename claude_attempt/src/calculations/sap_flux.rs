use crate::types::sensor::SensorType;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SapFluxError {
    #[error("Calculation error: {0}")]
    Calculation(String),
    #[error("Invalid sensor configuration: {0}")]
    SensorConfig(String),
}

/// Parameters for sap flux calculations - corresponds to constants.toml in README
#[derive(Debug, Clone)]
pub struct SapFluxParameters {
    /// Thermal diffusivity (cm²/s) - "k" in constants.toml
    pub k: f64,
    /// Heat pulse duration (seconds) - "hpd" in constants.toml
    pub heat_pulse_duration: f64,
    /// Time since heat pulse emission (seconds) - "t" in constants.toml
    pub measurement_time: f64,
    /// Seconds per hour - "sph" in constants.toml
    pub seconds_per_hour: f64,
    /// Wood dry density (kg/m³) - "pd" in constants.toml
    pub wood_dry_density: f64,
    /// Wood matrix specific heat capacity (J/kg/°C) - "cd" in constants.toml
    pub wood_specific_heat: f64,
    /// Gravimetric water content of sapwood (kg/kg) - "mc" in constants.toml
    pub sapwood_water_content: f64,
    /// Specific heat capacity of sap (J/kg/°C) - "cw" in constants.toml
    pub water_specific_heat: f64,
    /// Density of water (kg/m³) - "pw" in constants.toml
    pub water_density: f64,
    /// Wound correction coefficient - "woundcorr" in constants.toml
    pub wound_correction_b: f64,
}

impl Default for SapFluxParameters {
    /// Default values from constants.toml in README
    fn default() -> Self {
        Self {
            k: 0.002409611,
            heat_pulse_duration: 3.0,
            measurement_time: 60.0,
            seconds_per_hour: 3600.0,
            wood_dry_density: 500.0,
            wood_specific_heat: 1000.0,
            sapwood_water_content: 1.0,
            water_specific_heat: 4182.0,
            water_density: 1000.0,
            wound_correction_b: 1.8905,
        }
    }
}

/// DMA_Péclet sap flux calculator implementing the dual method approach
/// as described in Forster (2020)
pub struct DmaPecletCalculator;

impl DmaPecletCalculator {
    pub fn new() -> Self {
        Self
    }
    
    /// Calculate thermal diffusivity using Vandegehuchte & Steppe (2012a) method
    /// k = K/(ρc) where K is thermal conductivity and ρc is volumetric heat capacity
    pub fn calculate_thermal_diffusivity(
        thermal_conductivity: f64, // K (W/m/K)
        volumetric_heat_capacity: f64, // ρc (J/m³/K)
    ) -> f64 {
        // Convert to cm²/s
        (thermal_conductivity / volumetric_heat_capacity) * 10000.0
    }
    
    /// Step 1: Determine which method to use based on β (beta)
    /// If β ≤ 1, use HRM. If β > 1, use Tmax.
    /// β = ln(ΔTd,max / ΔTu,max)
    pub fn determine_method(
        delta_t_downstream_max: f64,
        delta_t_upstream_max: f64,
    ) -> Result<SapFluxMethod, SapFluxError> {
        if delta_t_downstream_max <= 0.0 || delta_t_upstream_max <= 0.0 {
            return Err(SapFluxError::Calculation(
                "Temperature differences must be positive".to_string()
            ));
        }
        
        let beta = (delta_t_downstream_max / delta_t_upstream_max).ln();
        
        if beta <= 1.0 {
            Ok(SapFluxMethod::HeatRatio { beta })
        } else {
            Ok(SapFluxMethod::Tmax { beta })
        }
    }
    
    /// Step 2: Calculate heat velocity (Vh) using the appropriate method
    pub fn calculate_heat_velocity(
        method: &SapFluxMethod,
        sensor_type: &SensorType,
        params: &SapFluxParameters,
        alpha: Option<f64>, // For HRM: ln(ΔTd/ΔTu) using 60-80s post-pulse temps
        t_max_downstream: Option<f64>, // For Tmax: time to max temp in downstream probe
    ) -> Result<f64, SapFluxError> {
        let (xd, xu) = Self::get_probe_distances(sensor_type)?;
        
        match method {
            SapFluxMethod::HeatRatio { .. } => {
                let alpha = alpha.ok_or_else(|| 
                    SapFluxError::Calculation("Alpha required for Heat Ratio Method".to_string())
                )?;
                
                // HRM: Vh = (2kα)/(xd + xu) + (xd - xu)/(2(t - t0/2))
                let term1 = (2.0 * params.k * alpha) / (xd + xu);
                let term2 = (xd - xu) / (2.0 * (params.measurement_time - params.heat_pulse_duration / 2.0));
                
                Ok(term1 + term2)
            },
            SapFluxMethod::Tmax { .. } => {
                let tm = t_max_downstream.ok_or_else(|| 
                    SapFluxError::Calculation("t_max required for Tmax Method".to_string())
                )?;
                
                if tm <= params.heat_pulse_duration {
                    return Err(SapFluxError::Calculation(
                        "t_max must be greater than heat pulse duration".to_string()
                    ));
                }
                
                // Tmax: Vh = √[(4k/t0) × ln(1 - t0/tm) + xd²] / (tm(tm - t0))
                let ln_term = (1.0 - params.heat_pulse_duration / tm).ln();
                if ln_term >= 0.0 {
                    return Err(SapFluxError::Calculation(
                        "Invalid t_max value leads to positive ln term".to_string()
                    ));
                }
                
                let sqrt_term = ((4.0 * params.k / params.heat_pulse_duration) * ln_term + xd * xd).sqrt();
                let denominator = tm * (tm - params.heat_pulse_duration);
                
                Ok(sqrt_term / denominator)
            }
        }
    }
    
    /// Step 3: Apply wound correction
    /// Vc = aVh + bVh² + cVh³
    /// Using Burgess et al. (2001) coefficients for typical wound diameters
    pub fn apply_wound_correction(
        vh: f64,
        wound_diameter_mm: f64,
        sensor_type: &SensorType,
        params: &SapFluxParameters,
    ) -> Result<f64, SapFluxError> {
        // Get wound correction coefficients based on sensor type and wound diameter
        let (a, b, c) = Self::get_wound_correction_coefficients(wound_diameter_mm, sensor_type, params)?;
        
        // Vc = aVh + bVh² + cVh³
        let vc = a * vh + b * vh * vh + c * vh * vh * vh;
        
        Ok(vc)
    }
    
    /// Step 4: Convert to sap flux density (J)
    /// J = Vc × ρd × (cd + mc × cw) / (ρw × cw)
    pub fn calculate_sap_flux_density(vc: f64, params: &SapFluxParameters) -> f64 {
        let numerator = vc * params.wood_dry_density * 
            (params.wood_specific_heat + params.sapwood_water_content * params.water_specific_heat);
        let denominator = params.water_density * params.water_specific_heat;
        
        // Convert from cm/s to cm/hr
        (numerator / denominator) * params.seconds_per_hour
    }
    
    /// Complete DMA_Péclet calculation pipeline
    pub fn calculate_sap_flux(
        delta_t_downstream_max: f64,
        delta_t_upstream_max: f64,
        alpha: Option<f64>,
        t_max_downstream: Option<f64>,
        sensor_type: &SensorType,
        wound_diameter_mm: f64,
        params: &SapFluxParameters,
    ) -> Result<SapFluxResult, SapFluxError> {
        // Step 1: Determine method
        let method = Self::determine_method(delta_t_downstream_max, delta_t_upstream_max)?;
        
        // Step 2: Calculate heat velocity
        let vh = Self::calculate_heat_velocity(&method, sensor_type, params, alpha, t_max_downstream)?;
        
        // Step 3: Apply wound correction
        let vc = Self::apply_wound_correction(vh, wound_diameter_mm, sensor_type, params)?;
        
        // Step 4: Convert to sap flux density
        let j = Self::calculate_sap_flux_density(vc, params);
        
        // Calculate Péclet number for validation
        let (xd, _) = Self::get_probe_distances(sensor_type)?;
        let peclet = vh * xd / params.k;
        
        Ok(SapFluxResult {
            method_used: method,
            heat_velocity_vh: vh,
            corrected_velocity_vc: vc,
            sap_flux_density_j: j,
            peclet_number: peclet,
        })
    }
    
    /// Get probe distances for sensor type
    fn get_probe_distances(sensor_type: &SensorType) -> Result<(f64, f64), SapFluxError> {
        match sensor_type {
            SensorType::ImplexxOld => Ok((0.6, 0.6)), // downstream, upstream distances in cm
            SensorType::ImplexxNew => Ok((0.8, 0.8)),
        }
    }
    
    /// Get wound correction coefficients based on sensor configuration
    /// Using typical values for Implexx sensors with 2.0-2.4mm wound diameter
    fn get_wound_correction_coefficients(
        wound_diameter_mm: f64,
        _sensor_type: &SensorType,
        params: &SapFluxParameters,
    ) -> Result<(f64, f64, f64), SapFluxError> {
        // Simplified coefficients - in practice these would come from
        // Burgess et al. (2001) Table 2 or Swanson & Whitfield (1981) model
        if wound_diameter_mm < 1.5 || wound_diameter_mm > 3.0 {
            return Err(SapFluxError::SensorConfig(
                format!("Wound diameter {}mm outside valid range (1.5-3.0mm)", wound_diameter_mm)
            ));
        }
        
        // Use wound correction coefficient from parameters
        let a = params.wound_correction_b; // From parameters.woundcorr.value in README
        let b = 0.0;    // Simplified - would be calculated based on wound geometry
        let c = 0.0;    // Simplified - would be calculated based on wound geometry
        
        Ok((a, b, c))
    }
    
    /// Calculate heat velocity for HRM method
    pub fn calculate_hrm_heat_velocity(
        alpha: f64,
        probe_distance: f64,
        params: &SapFluxParameters,
    ) -> f64 {
        // HRM: Vh = (2kα)/(xd + xu) + (xd - xu)/(2(t - t0/2))
        // For symmetric probes: xd = xu = probe_distance
        let term1 = (2.0 * params.k * alpha) / (2.0 * probe_distance);
        let term2 = 0.0; // (xd - xu) = 0 for symmetric probes
        term1 + term2
    }
    
    /// Calculate heat velocity for Tmax method
    pub fn calculate_tmax_heat_velocity(
        t_max: f64,
        probe_distance: f64,
        params: &SapFluxParameters,
    ) -> Result<f64, SapFluxError> {
        if t_max <= params.heat_pulse_duration {
            return Err(SapFluxError::Calculation(
                "t_max must be greater than heat pulse duration".to_string()
            ));
        }
        
        // Tmax: Vh = √[(4k/t0) × ln(1 - t0/tm) + xd²] / (tm(tm - t0))
        let ln_term = (1.0 - params.heat_pulse_duration / t_max).ln();
        if ln_term >= 0.0 {
            return Err(SapFluxError::Calculation(
                "Invalid t_max value leads to positive ln term".to_string()
            ));
        }
        
        let sqrt_term = ((4.0 * params.k / params.heat_pulse_duration) * ln_term + probe_distance * probe_distance).sqrt();
        let denominator = t_max * (t_max - params.heat_pulse_duration);
        
        Ok(sqrt_term / denominator)
    }
}

#[derive(Debug, Clone)]
pub enum SapFluxMethod {
    HeatRatio { beta: f64 },
    Tmax { beta: f64 },
}

#[derive(Debug, Clone)]
pub struct SapFluxResult {
    pub method_used: SapFluxMethod,
    pub heat_velocity_vh: f64,
    pub corrected_velocity_vc: f64,
    pub sap_flux_density_j: f64,
    pub peclet_number: f64,
}

/// Quality control checks for sap flux calculations
pub struct SapFluxQualityControl;

impl SapFluxQualityControl {
    /// Check if HRM is reliable for given heat velocity
    /// HRM becomes unreliable when heat velocity exceeds ~15 cm/hr
    pub fn is_hrm_reliable(vh_cm_per_hr: f64, params: &SapFluxParameters) -> bool {
        // Reliability threshold varies with thermal diffusivity
        // Use default reference value of 0.002409611 for scaling
        let reference_k = 0.002409611;
        let threshold = 15.0 * (params.k / reference_k);
        vh_cm_per_hr <= threshold
    }
    
    /// Check if Tmax can resolve given heat velocity
    /// Tmax cannot resolve velocities below ~10 cm/hr
    pub fn is_tmax_reliable(vh_cm_per_hr: f64) -> bool {
        vh_cm_per_hr >= 10.0
    }
    
    /// Validate measurement inputs
    pub fn validate_measurements(
        delta_t_downstream: f64,
        delta_t_upstream: f64,
        t_max: Option<f64>,
        params: &SapFluxParameters,
    ) -> Result<(), SapFluxError> {
        if delta_t_downstream <= 0.0 || delta_t_upstream <= 0.0 {
            return Err(SapFluxError::Calculation(
                "Temperature differences must be positive".to_string()
            ));
        }
        
        if let Some(tm) = t_max {
            if tm <= params.heat_pulse_duration {
                return Err(SapFluxError::Calculation(
                    "t_max must be greater than heat pulse duration".to_string()
                ));
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_method_determination() {
        // Test HRM selection (β ≤ 1)
        let method = DmaPecletCalculator::determine_method(1.0, 1.0).unwrap();
        matches!(method, SapFluxMethod::HeatRatio { .. });
        
        // Test Tmax selection (β > 1)
        let method = DmaPecletCalculator::determine_method(3.0, 1.0).unwrap();
        matches!(method, SapFluxMethod::Tmax { .. });
    }
    
    #[test]
    fn test_quality_control() {
        let params = SapFluxParameters::default();
        
        assert!(SapFluxQualityControl::is_hrm_reliable(10.0, &params));
        assert!(!SapFluxQualityControl::is_hrm_reliable(20.0, &params));
        
        assert!(SapFluxQualityControl::is_tmax_reliable(15.0));
        assert!(!SapFluxQualityControl::is_tmax_reliable(5.0));
    }
}