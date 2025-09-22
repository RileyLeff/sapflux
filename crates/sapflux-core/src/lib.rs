//! Core domain types, database access, and shared utilities for the Sapflux pipeline.

pub mod calculator;
pub mod data_formats;
pub mod db;
pub mod flatten;
pub mod ingestion;
pub mod metadata_enricher;
#[cfg(feature = "runtime")]
pub mod metadata_manifest;
#[cfg(feature = "runtime")]
pub mod object_gc;
pub mod object_store;
#[cfg(feature = "runtime")]
pub mod outputs;
pub mod parameter_resolver;
pub mod parsers;
pub mod pipelines;
pub mod quality_filters;
pub mod seed;
pub mod timestamp_fixer;
#[cfg(feature = "runtime")]
pub mod transactions;

pub mod prelude {
    pub use anyhow::{anyhow, Context, Result};
}
