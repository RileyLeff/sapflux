# Parameter Provenance in Final Outputs

**Principle:** To ensure full auditability and prevent misinterpretation, the final output DataFrame will include columns for both the *value* of each parameter used in a calculation and the *source* of that value.

**Explanation:** This design makes it explicitly clear where every number in a calculation came from. It answers both "what value was used?" and "why was that value chosen?". This is the responsibility of the `parameter_resolver` component, which implements the parameter cascade.

**Example:** The final output file will contain columns like the following:

| timestamp_utc | plant_code | parameter_wound_diameter_cm | parameter_source_wound_diameter_cm |
| :--- | :--- | :--- | :--- |
| ... | PL-01 | 0.2 | `"global_default"` |
| ... | PL-02 | 0.24 | `"stem_override"` |
| ... | PL-03 | 0.18 | `"species_override"` |
