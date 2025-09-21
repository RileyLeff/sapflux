# Comprehensive Calculation Outputs

**Principle:** The `calculator` component will not discard intermediate calculations or results from non-selected methods. Instead, it will compute results for all applicable methods in parallel and include them in the output, providing a final, switched result as a convenience column.

**Explanation:** This approach empowers end-users to perform their own advanced quality control, custom gap-filling, or alternative analyses. It provides the fundamental building blocks of the calculation, not just the final assembly, making the dataset far more valuable for scientific discovery.

**Example:** For each measurement, the output DataFrame will contain a rich set of calculation results:
*   **`calculation_method_used`**: The method chosen by the default DMA switch (e.g., `"HRM"`).
*   **HRM-Specific Columns**: `vh_hrm_cm_hr`, `vc_hrm_cm_hr`, `j_hrm_cm_hr`. These are calculated for every row where `alpha` is available.
*   **Tmax-Specific Columns**: `vh_tmax_cm_hr`, `vc_tmax_cm_hr`, `j_tmax_cm_hr`. These are calculated for every row where `tm_seconds` is valid.
*   **Final DMA Result**: **`sap_flux_density_j_dma_cm_hr`**. This column contains the recommended value, selecting from either `j_hrm_cm_hr` or `j_tmax_cm_hr` based on the `beta` value.