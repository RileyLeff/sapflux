use polars::prelude::*;
use sapflux_core::calculator::apply_dma_peclet;

fn df_with_params() -> DataFrame {
    df!(
        "alpha" => &[0.24f64, 0.12f64],
        "beta" => &[0.8f64, 1.4f64],
        "time_to_max_temp_downstream_s" => &[80.0f64, 90.0f64],
        "parameter_heat_pulse_duration_s" => &[3.0f64, 3.0f64],
        "parameter_thermal_diffusivity_k_cm2_s" => &[0.002409611f64, 0.002409611f64],
        "parameter_probe_distance_downstream_cm" => &[0.6f64, 0.6f64],
        "parameter_probe_distance_upstream_cm" => &[0.6f64, 0.6f64],
        "parameter_wound_correction_a" => &[1.8905f64, 1.8905f64],
        "parameter_wound_correction_b" => &[0.0f64, 0.0f64],
        "parameter_wound_correction_c" => &[0.0f64, 0.0f64],
        "parameter_wood_density_kg_m3" => &[500.0f64, 500.0f64],
        "parameter_wood_specific_heat_j_kg_c" => &[1000.0f64, 1000.0f64],
        "parameter_water_content_g_g" => &[1.0f64, 1.0f64],
        "parameter_water_specific_heat_j_kg_c" => &[4182.0f64, 4182.0f64],
        "parameter_water_density_kg_m3" => &[1000.0f64, 1000.0f64],
    )
    .unwrap()
}

fn expected_vh_hrm(alpha: f64) -> f64 {
    let k = 0.002409611;
    let probe = 0.6 + 0.6;
    (2.0 * k * alpha / probe) * 3600.0
}

fn expected_vh_tmax(tm: f64) -> f64 {
    let k = 0.002409611;
    let heat = 3.0;
    let probe_down: f64 = 0.6;
    let inner = (4.0 * k / heat) * ((1.0 - heat / tm).ln()) + probe_down.powi(2);
    (inner.sqrt() / (tm * (tm - heat))) * 3600.0
}

#[test]
fn calculator_populates_columns() {
    let df = apply_dma_peclet(&df_with_params()).expect("calculator succeeded");

    let vh_hrm = df.column("vh_hrm_cm_hr").unwrap().f64().unwrap();
    assert!((vh_hrm.get(0).unwrap() - expected_vh_hrm(0.24)).abs() < 1e-9);

    let vh_tmax = df.column("vh_tmax_cm_hr").unwrap().f64().unwrap();
    assert!((vh_tmax.get(1).unwrap() - expected_vh_tmax(90.0)).abs() < 1e-9);

    let method = df.column("calculation_method_used").unwrap().str().unwrap();
    assert_eq!(method.get(0), Some("HRM"));
    assert_eq!(method.get(1), Some("Tmax"));

    let sap_flux = df
        .column("sap_flux_density_j_dma_cm_hr")
        .unwrap()
        .f64()
        .unwrap();
    let j_hrm = df.column("j_hrm_cm_hr").unwrap().f64().unwrap();
    assert!((sap_flux.get(0).unwrap() - j_hrm.get(0).unwrap()).abs() < 1e-9);
}

#[test]
fn tmax_branch_requires_tm_exceeding_heat_pulse() -> PolarsResult<()> {
    let mut base = df_with_params();
    let df = base.with_column(Series::new(
        "time_to_max_temp_downstream_s".into(),
        vec![2.0f64, 90.0f64],
    ))?;

    let result = apply_dma_peclet(&df)?;
    let vh_tmax = result.column("vh_tmax_cm_hr")?.f64()?;

    assert!(vh_tmax.get(0).is_none());
    assert!(vh_tmax.get(1).is_some());

    Ok(())
}

#[test]
fn tmax_branch_returns_none_when_inside_negative() -> PolarsResult<()> {
    let mut base = df_with_params();
    let df_step = base.with_column(Series::new(
        "time_to_max_temp_downstream_s".into(),
        vec![3.01f64, 90.0f64],
    ))?;
    let df = df_step.with_column(Series::new(
        "parameter_probe_distance_downstream_cm".into(),
        vec![0.0f64, 0.6f64],
    ))?;

    let result = apply_dma_peclet(&df)?;
    let vh_tmax = result.column("vh_tmax_cm_hr")?.f64()?;
    let j_tmax = result.column("j_tmax_cm_hr")?.f64()?;

    assert!(vh_tmax.get(0).is_none());
    assert!(j_tmax.get(0).is_none());
    assert!(vh_tmax.get(1).is_some());

    Ok(())
}
