use anyhow::{anyhow, Result};
use once_cell::sync::Lazy;
use polars::prelude::DataFrame;

use crate::{
    flatten::flatten_parsed_files,
    parsers::ParsedData,
    timestamp_fixer::{self, DeploymentMetadata, SiteMetadata},
};
use sapflux_parser::ParsedFileData;

#[derive(Debug, Default)]
pub struct ExecutionContext {
    pub deployments: Vec<DeploymentMetadata>,
    pub sites: Vec<SiteMetadata>,
}

pub trait ProcessingPipeline: Send + Sync {
    fn code_identifier(&self) -> &'static str;
    fn version(&self) -> &'static str;
    fn input_data_format(&self) -> &'static str;
    fn run_batch(
        &self,
        _context: &ExecutionContext,
        _parsed_batch: &[&dyn ParsedData],
    ) -> Result<DataFrame>;
}

#[derive(Debug, Clone)]
pub struct ProcessingPipelineDescriptor {
    pub code: &'static str,
    pub version: &'static str,
    pub input_data_format: &'static str,
    pub include_in_pipeline: bool,
    pub description: &'static str,
}

static PIPELINES: Lazy<Vec<ProcessingPipelineDescriptor>> = Lazy::new(|| {
    vec![ProcessingPipelineDescriptor {
        code: "standard_v1_dst_fix",
        version: "0.1.0",
        input_data_format: "sapflow_toa5_hierarchical_v1",
        include_in_pipeline: true,
        description: "Timestamp fix + metadata enrichment + DMA Peclet calculation",
    }]
});

pub fn all_pipeline_descriptors() -> &'static [ProcessingPipelineDescriptor] {
    PIPELINES.as_slice()
}

static PIPELINE_IMPLEMENTATIONS: Lazy<Vec<&'static dyn ProcessingPipeline>> =
    Lazy::new(|| vec![&StandardPipelineStub as &dyn ProcessingPipeline]);

pub fn all_pipelines() -> &'static [&'static dyn ProcessingPipeline] {
    PIPELINE_IMPLEMENTATIONS.as_slice()
}

struct StandardPipelineStub;

impl ProcessingPipeline for StandardPipelineStub {
    fn code_identifier(&self) -> &'static str {
        "standard_v1_dst_fix"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn input_data_format(&self) -> &'static str {
        "sapflow_toa5_hierarchical_v1"
    }

    fn run_batch(
        &self,
        context: &ExecutionContext,
        _parsed_batch: &[&dyn ParsedData],
    ) -> Result<DataFrame> {
        if context.deployments.is_empty() || context.sites.is_empty() {
            return Err(anyhow!(
                "standard_v1_dst_fix requires deployment/site metadata"
            ));
        }

        let mut typed_files: Vec<&ParsedFileData> = Vec::with_capacity(_parsed_batch.len());
        for parsed in _parsed_batch {
            let file = parsed
                .downcast_ref::<ParsedFileData>()
                .ok_or_else(|| anyhow!("standard_v1_dst_fix requires sapflow_toa5_hierarchical_v1"))?;
            typed_files.push(file);
        }

        let flattened = flatten_parsed_files(&typed_files)?;
        let corrected = timestamp_fixer::correct_timestamps(
            &flattened,
            &context.sites,
            &context.deployments,
        )?;

        Ok(corrected)
    }
}
