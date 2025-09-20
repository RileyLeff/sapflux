//! Processing pipeline: parse -> timestamp fix -> metadata enrich -> calculate -> publish.

mod pipeline;

pub use pipeline::{PipelineConfig, ProcessingError, ProcessingPipeline, WorkflowResult};
