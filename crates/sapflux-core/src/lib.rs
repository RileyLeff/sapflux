//! Core domain types, database access, and shared utilities for the Sapflux pipeline.

pub mod data_formats;
pub mod db;
pub mod flatten;
pub mod ingestion;
pub mod metadata_enricher;
pub mod parsers;
pub mod pipelines;
pub mod parameter_resolver;
pub mod timestamp_fixer;
pub mod seed;

pub mod prelude {
    pub use anyhow::{anyhow, Context, Result};
}
