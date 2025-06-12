// crates/sapflux-core/src/processing/mod.rs

// --- Declare all modules within the `processing` directory ---
mod correction;
mod legacy_format;
mod multi_sensor_format;
mod schema;
mod types;
mod unification;

// --- Publicly export the main pipeline functions ---
// This creates a clean public API for the `processing` module.
pub use correction::apply_dst_correction_and_map_deployments;
pub use unification::get_parsed_and_unified_lazyframe;