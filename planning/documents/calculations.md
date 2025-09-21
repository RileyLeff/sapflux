# Calculations Architecture

This document describes the architecture for performing sap flux calculations within the pipeline. The system is designed to be flexible, versionable, and highly configurable, allowing for context-specific parameter overrides at multiple levels of the metadata hierarchy.

## Component Architecture

The calculation system consists of two main parts, separating the *logic* from the *data*:

1.  **The Processing Pipeline (The Code)**: This is a compiled-in Rust component (e.g., `"dma_peclet_v1"`) that defines the sequence of mathematical steps and equations to be executed. Its logic is static for a given application version. Different, versioned pipelines can be added to the application over time.
2.  **The Parameter System (The Data)**: This is a dynamic, database-driven system that provides the specific input parameters (e.g., `wound_diameter_cm`, `wood_density_kg_m3`) for the calculations. This system allows for hierarchical overrides, enabling fine-grained control over the pipeline's behavior.

## Canonical Pipeline: `dma_peclet_v1`

The default processing pipeline implements the Dual Method Approach with Péclet transition (DMA_Péclet) as described in Forster (2020). This method combines the strengths of two heat pulse approaches to resolve the entire measurement range of sap flux velocities observed in plants.

### Theoretical Foundation

The Péclet number (`Pe`) describes the ratio of convection to conduction:
```
Pe = Vh × x / k
```
Where:
*   `Vh` = heat velocity (cm/hr)
*   `x` = distance between heater and thermistor probes (cm)
*   `k` = thermal diffusivity (cm²/s)

### Calculation Steps

#### Step 1: Determine Which Method to Use

Based on thermal equilibrium theory, we use:
*   **Heat Ratio Method (HRM)** when Pe ≤ 1 (conduction-dominated, slow flows)
*   **Tmax Method** when Pe > 1 (convection-dominated, fast flows)

Since `Vh` is not known initially, its proxy, β (`beta`), is used for the decision:
*   `β = ln(ΔTd,max / ΔTu,max)`
*   If β ≤ 1, use HRM. If β > 1, use Tmax.

#### Step 2: Calculate Heat Velocity (Vh)

**For HRM (when β ≤ 1):**
```
Vh = (2 * k * α) / (xd + xu) + (xd - xu) / (2 * (t - t0 / 2))
```
Where:
*   `α` (`alpha`) = `ln(ΔTd/ΔTu)` using temperatures 60-80 seconds post-pulse
*   `k` = thermal diffusivity (cm²/s)
*   `xd` = downstream probe distance (cm)
*   `xu` = upstream probe distance (cm)
*   `t` = time since heat pulse emission (seconds)
*   `t0` = heat pulse duration (seconds)

**For Tmax (when β > 1):**
```
Vh = sqrt( (4 * k / t0) * ln(1 - t0 / tm) + xd^2 ) / (tm * (tm - t0))
```
Where:
*   `tm` = time to maximum temperature in downstream probe (seconds)

#### Step 3: Apply Wound Correction

The insertion of probes disrupts sap flow, requiring correction to find the corrected velocity, `Vc`.
```
Vc = a * Vh + b * Vh^2 + c * Vh^3
```
Wound correction coefficients (`a`, `b`, `c`) are typically derived from models like Burgess et al. (2001) or Swanson & Whitfield (1981) and depend on wound diameter and probe spacing.

#### Step 4: Convert to Sap Flux Density (J)

```
J = Vc * pd * (cd + mc * cw) / (pw * cw)
```
Where:
*   `pd` = sapwood dry density (kg/m³)
*   `cd` = specific heat capacity of dry wood matrix (J/kg/°C)
*   `mc` = gravimetric water content of sapwood (kg/kg)
*   `cw` = specific heat capacity of sap (water) (J/kg/°C)
*   `pw` = density of sap (water) (kg/m³)

The final result `J` is in cm³/cm²/hr, which is equivalent to cm/hr.

## The Parameter System

The parameter system provides the values used in the calculation steps. It consists of a dictionary of available parameters, a table of override rules, and a resolution engine that implements a "cascade" to determine the final value for any given context.

### The Parameter Cascade

To find the value for a parameter for a given deployment, the system follows a strict order of precedence: **the most specific rule always wins.**

1.  **Deployment-level** override
2.  **Stem-level** override
3.  **Plant-level** override
4.  **Plot-level** override
5.  **Zone-level** override
6.  **Species-level** override
7.  **Site-level** override
8.  **Global Default** (from the per-run transaction config)

### Database Tables

*   **`parameters`**: A dictionary defining all available parameters (e.g., `wound_diameter_cm`).
*   **`parameter_overrides`**: A flexible table storing all override rules, linking a `parameter_id` to one or more metadata entities (e.g., a specific `species_id`).

### Global Default Parameters

This is the base layer of the cascade, defined in the per-run Transaction Manifest. The following table lists the canonical parameters, their descriptions, and the system's default values.

| Parameter Name                  | Description                                                                    | Units        | Default Value |
| ------------------------------- | ------------------------------------------------------------------------------ | ------------ | ------------- |
| `wound_diameter_cm`             | Diameter of the drilled hole for the sensor probe.                             | cm           | 0.2           |
| `sapwood_green_weight_kg`       | Wet weight of a sapwood core sample.                                           | kg           | 0.001         |
| `sapwood_dry_weight_kg`         | Dry weight of a sapwood core sample.                                           | kg           | 0.005         |
| `thermal_diffusivity_k_cm2_s`   | Thermal diffusivity of the sapwood-water matrix.                               | cm²/s        | 0.00241       |
| `probe_distance_downstream_cm`  | Distance from the heater to the downstream thermistor probe.                   | cm           | 0.6           |
| `probe_distance_upstream_cm`    | Distance from the heater to the upstream thermistor probe.                     | cm           | 0.6           |
| `heat_pulse_duration_s`         | Duration of the heat pulse.                                                    | seconds      | 3.0           |
| `wound_correction_b_coeff`      | An empirical coefficient for wound correction models.                          | dimensionless| 1.8905        |
| `time_since_pulse_s`            | Time since heat pulse emission, used in the HRM equation.                      | seconds      | 60.0          |
| `seconds_per_hour`              | Conversion factor.                                                             | s/hr         | 3600          |
| `wood_density_kg_m3`            | Basic density of dry sapwood.                                                  | kg/m³        | 500           |
| `wood_specific_heat_j_kg_c`     | Specific heat capacity of the dry wood matrix at 20°C.                         | J/kg/°C      | 1200          |
| `water_content_g_g`             | Gravimetric water content of sapwood (green weight - dry weight) / dry weight. | kg/kg        | 1.0           |
| `water_specific_heat_j_kg_c`    | Specific heat capacity of sap (water) at 20°C.                                 | J/kg/°C      | 4182          |
| `water_density_kg_m3`           | Density of sap (water).                                                        | kg/m³        | 1000          |

## Implementation Details

*   **Thermal Diffusivity (k)**: If not provided as a fixed override, `k` is calculated dynamically using the Vandegehuchte & Steppe (2012a) method: `k = K / (ρc)`, where `K` is thermal conductivity and `ρc` is volumetric heat capacity.
*   **Missing Values**: Any `-99` or `NAN` values from the logger data are converted to `null` before calculations begin.
*   **Quality Control**: The DMA_Péclet approach automatically handles the transition between HRM and Tmax methods, which have different effective ranges. This avoids known issues where HRM is unreliable for high flow or Tmax is unreliable for low flow.