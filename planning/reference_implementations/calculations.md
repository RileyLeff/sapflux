### Rust Reference Implementation: `dma_peclet_v1` Calculator

```rust
// filename: src/processing/calculator.rs

use polars::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CalculationError {
    #[error("Polars operation failed: {0}")]
    Polars(#[from] PolarsError),
}

// ===================================================================
// 1. The Generic "Calculator" Trait
// ===================================================================

/// A trait for any component that performs scientific calculations.
/// This allows the system to support different, versioned calculation methods in the future.
pub trait Calculator: Send + Sync {
    /// Returns the unique, hardcoded identifier for this calculator.
    fn code_identifier(&self) -> &'static str;

    /// Executes the calculation logic.
    ///
    /// Takes a DataFrame that is assumed to be fully enriched with all necessary
    /// measurement and parameter columns. Returns a new DataFrame with the
    /// calculation output columns added.
    fn calculate(&self, df: &DataFrame) -> Result<DataFrame, CalculationError>;
}


// ===================================================================
// 2. The Specific `dma_peclet_v1` Implementation
// ===================================================================

/// Implements the Dual Method Approach with Péclet transition (DMA_Péclet)
/// as described in Forster (2020).
pub struct DmaPecletCalculatorV1;

impl Calculator for DmaPecletCalculatorV1 {
    fn code_identifier(&self) -> &'static str {
        "dma_peclet_v1"
    }

    /// Orchestrates the DMA-Péclet calculation by chaining together a series of
    /// component-like data transformation functions.
    fn calculate(&self, df: &DataFrame) -> Result<DataFrame, CalculationError> {
        let calculated_df = df.clone().lazy()
            .pipe(calculate_all_vh)
            .pipe(apply_all_wound_corrections)
            .pipe(convert_all_to_j)
            .pipe(apply_dma_switch)
            .collect()?;

        Ok(calculated_df)
    }
}

// --- Calculation Component Functions ---

/// Component A: Calculates raw heat velocities (Vh) for both HRM and Tmax methods in parallel.
fn calculate_all_vh(lf: LazyFrame) -> LazyFrame {
    // Expression for Vh using the Heat Ratio Method (HRM), in cm/hr.
    // Based on Forster (2020), modified Eq. (10). Used when conduction dominates.
    let vh_hrm_expr = (
        (lit(2.0) * col("parameter_thermal_diffusivity_k_cm2_s") * col("alpha"))
        /
        (col("parameter_probe_distance_downstream_cm") + col("parameter_probe_distance_upstream_cm"))
    ) * lit(3600.0);

    // Expression for Vh using the Tmax method, in cm/hr.
    // Based on Kluitenberg & Ham (2004). Used when convection dominates.
    let vh_tmax_expr = (
        (
            (
                (lit(4.0) * col("parameter_thermal_diffusivity_k_cm2_s") / col("parameter_heat_pulse_duration_s"))
                *
                (lit(1.0) - col("parameter_heat_pulse_duration_s") / col("tm_seconds")).log()
            )
            +
            col("parameter_probe_distance_downstream_cm").pow(lit(2.0))
        ).sqrt()
        /
        (col("tm_seconds") * (col("tm_seconds") - col("parameter_heat_pulse_duration_s")))
    ) * lit(3600.0);

    lf.with_column(vh_hrm_expr.alias("vh_hrm_cm_hr"))
      .with_column(vh_tmax_expr.alias("vh_tmax_cm_hr"))
}

/// Component B: Applies polynomial wound correction to both Vh values to get Vc.
fn apply_all_wound_corrections(lf: LazyFrame) -> LazyFrame {
    // Helper function to create the polynomial expression
    let apply_correction = |vh_col_name: &str| {
        let vh = col(vh_col_name);
        let a = col("parameter_wound_correction_a");
        let b = col("parameter_wound_correction_b");
        let c = col("parameter_wound_correction_c");
        // Vc = a*Vh + b*Vh^2 + c*Vh^3 (Forster (2020), Eq. 15)
        (a * vh.clone()) + (b * vh.clone().pow(lit(2.0))) + (c * vh.pow(lit(3.0)))
    };

    lf.with_column(apply_correction("vh_hrm_cm_hr").alias("vc_hrm_cm_hr"))
      .with_column(apply_correction("vh_tmax_cm_hr").alias("vc_tmax_cm_hr"))
}

/// Component C: Converts both corrected heat velocities (Vc) to sap flux densities (J).
fn convert_all_to_j(lf: LazyFrame) -> LazyFrame {
    // Helper function to create the conversion expression
    let convert_to_j = |vc_col_name: &str| {
        let vc_cm_hr = col(vc_col_name);
        let vc_m_s = vc_cm_hr / lit(360000.0); // Convert cm/hr to m/s

        // J = Vc * ρd * (cd + mc * cw) / (ρw * cw) (Forster (2020), Eq. 16)
        let numerator = vc_m_s
            * col("parameter_wood_density_kg_m3")
            * (col("parameter_wood_specific_heat_j_kg_c") + col("parameter_water_content_g_g") * col("parameter_water_specific_heat_j_kg_c"));
        let denominator = col("parameter_water_density_kg_m3") * col("parameter_water_specific_heat_j_kg_c");
        let j_m_s = numerator / denominator;

        // Convert final result from m/s back to conventional cm/hr units
        j_m_s * lit(360000.0)
    };

    lf.with_column(convert_to_j("vc_hrm_cm_hr").alias("j_hrm_cm_hr"))
      .with_column(convert_to_j("vc_tmax_cm_hr").alias("j_tmax_cm_hr"))
}

/// Component D: Applies the DMA-Péclet switch to provide a final, recommended J value
/// and a provenance column indicating the method used.
fn apply_dma_switch(lf: LazyFrame) -> LazyFrame {
    let beta = col("beta");
    let is_hrm = beta.clone().lt_eq(lit(1.0));

    lf.with_column(
        // Add the provenance column
        when(is_hrm.clone())
            .then(lit("HRM"))
            .otherwise(lit("Tmax"))
            .alias("calculation_method_used")
    ).with_column(
        // Create the final recommended column by choosing between the two parallel calculations
        when(is_hrm)
            .then(col("j_hrm_cm_hr"))
            .otherwise(col("j_tmax_cm_hr"))
            .alias("sap_flux_density_j_dma_cm_hr")
    )
}```