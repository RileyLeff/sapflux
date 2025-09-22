//! Core domain types, database access, and shared utilities for the Sapflux pipeline.

pub mod data_formats;
pub mod db;
pub mod flatten;
pub mod parsers;
pub mod pipelines;
pub mod timestamp_fixer;
pub mod seed;

pub mod prelude {
    pub use anyhow::{anyhow, Context, Result};
}
