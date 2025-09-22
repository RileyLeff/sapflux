use polars::prelude::*;

const SECONDS_PER_HOUR: f64 = 3600.0;
const CM_TO_M: f64 = 0.01;

pub fn apply_dma_peclet(df: &DataFrame) -> Result<DataFrame, PolarsError> {
    let len = df.height();

    let alpha = df.column("alpha")?.f64()?;
    let beta = df.column("beta")?.f64()?;
    let tm = df.column("time_to_max_temp_downstream_s")?.f64()?;
    let heat_pulse = df.column("parameter_heat_pulse_duration_s")?.f64()?;
    let k = df.column("parameter_thermal_diffusivity_k_cm2_s")?.f64()?;
    let probe_down = df.column("parameter_probe_distance_downstream_cm")?.f64()?;
    let probe_up = df.column("parameter_probe_distance_upstream_cm")?.f64()?;
    let wound_a = df.column("parameter_wound_correction_a")?.f64()?;
    let wound_b = df.column("parameter_wound_correction_b")?.f64()?;
    let wound_c = df.column("parameter_wound_correction_c")?.f64()?;
    let wood_density = df.column("parameter_wood_density_kg_m3")?.f64()?;
    let wood_specific_heat = df.column("parameter_wood_specific_heat_j_kg_c")?.f64()?;
    let water_content = df.column("parameter_water_content_g_g")?.f64()?;
    let water_specific_heat = df.column("parameter_water_specific_heat_j_kg_c")?.f64()?;
    let water_density = df.column("parameter_water_density_kg_m3")?.f64()?;

    let mut vh_hrm = Vec::with_capacity(len);
    let mut vh_tmax = Vec::with_capacity(len);
    let mut vc_hrm = Vec::with_capacity(len);
    let mut vc_tmax = Vec::with_capacity(len);
    let mut j_hrm = Vec::with_capacity(len);
    let mut j_tmax = Vec::with_capacity(len);
    let mut method: Vec<Option<&'static str>> = Vec::with_capacity(len);
    let mut sap_flux = Vec::with_capacity(len);

    for idx in 0..len {
        let params = (
            alpha.get(idx),
            beta.get(idx),
            tm.get(idx),
            heat_pulse.get(idx),
            k.get(idx),
            probe_down.get(idx),
            probe_up.get(idx),
            wound_a.get(idx),
            wound_b.get(idx),
            wound_c.get(idx),
            wood_density.get(idx),
            wood_specific_heat.get(idx),
            water_content.get(idx),
            water_specific_heat.get(idx),
            water_density.get(idx),
        );

        let (
            vh_hrm_val,
            vc_hrm_val,
            j_hrm_val,
            vh_tmax_val,
            vc_tmax_val,
            j_tmax_val,
            method_val,
            sap_flux_val,
        ) = compute_row(params);

        vh_hrm.push(vh_hrm_val);
        vc_hrm.push(vc_hrm_val);
        j_hrm.push(j_hrm_val);
        vh_tmax.push(vh_tmax_val);
        vc_tmax.push(vc_tmax_val);
        j_tmax.push(j_tmax_val);
        method.push(method_val);
        sap_flux.push(sap_flux_val);
    }

    let method_vec: Vec<Option<&str>> = method.into_iter().collect();

    let mut output = df.clone();
    output.hstack_mut(&mut [
        Series::new("vh_hrm_cm_hr".into(), vh_hrm).into(),
        Series::new("vh_tmax_cm_hr".into(), vh_tmax).into(),
        Series::new("vc_hrm_cm_hr".into(), vc_hrm).into(),
        Series::new("vc_tmax_cm_hr".into(), vc_tmax).into(),
        Series::new("j_hrm_cm_hr".into(), j_hrm).into(),
        Series::new("j_tmax_cm_hr".into(), j_tmax).into(),
        Series::new("calculation_method_used".into(), method_vec).into(),
        Series::new("sap_flux_density_j_dma_cm_hr".into(), sap_flux).into(),
    ])?;

    Ok(output)
}

#[allow(clippy::type_complexity)]
fn compute_row(
    params: (
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
    ),
) -> (
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<&'static str>,
    Option<f64>,
) {
    let (
        alpha,
        beta,
        tm,
        heat_pulse,
        k,
        probe_down,
        probe_up,
        wound_a,
        wound_b,
        wound_c,
        wood_density,
        wood_specific_heat,
        water_content,
        water_specific_heat,
        water_density,
    ) = params;

    if let (
        Some(alpha),
        Some(beta),
        Some(heat_pulse),
        Some(k),
        Some(probe_down),
        Some(probe_up),
        Some(wound_a),
        Some(wound_b),
        Some(wound_c),
        Some(wood_density),
        Some(wood_specific_heat),
        Some(water_content),
        Some(water_specific_heat),
        Some(water_density),
    ) = (
        alpha,
        beta,
        heat_pulse,
        k,
        probe_down,
        probe_up,
        wound_a,
        wound_b,
        wound_c,
        wood_density,
        wood_specific_heat,
        water_content,
        water_specific_heat,
        water_density,
    ) {
        let vh_hrm = ((2.0 * k * alpha) / (probe_down + probe_up)) * SECONDS_PER_HOUR;
        let vc_hrm = apply_wound_correction(vh_hrm, wound_a, wound_b, wound_c);
        let j_hrm = convert_to_j(
            vc_hrm,
            wood_density,
            wood_specific_heat,
            water_content,
            water_specific_heat,
            water_density,
        );

        let (vh_tmax, vc_tmax, j_tmax) = match (tm, Some(heat_pulse)) {
            (Some(tm), Some(heat_pulse)) if tm > heat_pulse && tm > 0.0 => {
                let log_term = (1.0 - heat_pulse / tm).ln();
                if !log_term.is_finite() {
                    (None, None, None)
                } else {
                    let inside = ((4.0 * k / heat_pulse) * log_term) + probe_down.powi(2);
                    if inside <= 0.0 {
                        (None, None, None)
                    } else {
                        let vh_tmax = inside.sqrt() / (tm * (tm - heat_pulse)) * SECONDS_PER_HOUR;
                        let vc_tmax = apply_wound_correction(vh_tmax, wound_a, wound_b, wound_c);
                        let j_tmax = convert_to_j(
                            vc_tmax,
                            wood_density,
                            wood_specific_heat,
                            water_content,
                            water_specific_heat,
                            water_density,
                        );
                        (Some(vh_tmax), Some(vc_tmax), Some(j_tmax))
                    }
                }
            }
            _ => (None, None, None),
        };

        let (method, sap_flux) = if beta <= 1.0 {
            (Some("HRM"), Some(j_hrm))
        } else {
            (Some("Tmax"), j_tmax)
        };

        (
            Some(vh_hrm),
            Some(vc_hrm),
            Some(j_hrm),
            vh_tmax,
            vc_tmax,
            j_tmax,
            method,
            sap_flux,
        )
    } else {
        (None, None, None, None, None, None, None, None)
    }
}

fn apply_wound_correction(vh: f64, a: f64, b: f64, c: f64) -> f64 {
    a * vh + b * vh.powi(2) + c * vh.powi(3)
}

fn convert_to_j(
    vc_cm_hr: f64,
    wood_density: f64,
    wood_specific_heat: f64,
    water_content: f64,
    water_specific_heat: f64,
    water_density: f64,
) -> f64 {
    let vc_m_s = vc_cm_hr * CM_TO_M / SECONDS_PER_HOUR;
    let numerator =
        vc_m_s * wood_density * (wood_specific_heat + water_content * water_specific_heat);
    let denominator = water_density * water_specific_heat;
    let j_m_s = numerator / denominator;
    j_m_s * SECONDS_PER_HOUR / CM_TO_M
}
