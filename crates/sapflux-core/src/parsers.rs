use std::any::Any;

use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use sapflux_parser::{parse_sapflow_file, ParsedFileData as ExternalParsedFileData};

pub trait ParsedData: Any + Send + Sync {
    fn data_format_name(&self) -> &'static str;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl dyn ParsedData {
    pub fn downcast_ref<T: ParsedData + 'static>(&self) -> Option<&T> {
        self.as_any().downcast_ref::<T>()
    }

    pub fn downcast_mut<T: ParsedData + 'static>(&mut self) -> Option<&mut T> {
        self.as_any_mut().downcast_mut::<T>()
    }
}

impl ParsedData for ExternalParsedFileData {
    fn data_format_name(&self) -> &'static str {
        "sapflow_toa5_hierarchical_v1"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub trait SapflowParser: Send + Sync {
    fn code_identifier(&self) -> &'static str;
    fn version(&self) -> &'static str;
    fn output_data_format(&self) -> &'static str;
    fn parse(&self, content: &str) -> Result<Box<dyn ParsedData>>;
}

#[derive(Debug, Clone)]
pub struct ParserDescriptor {
    pub code: &'static str,
    pub version: &'static str,
    pub output_format: &'static str,
    pub include_in_pipeline: bool,
    pub description: &'static str,
}

static PARSERS: Lazy<Vec<ParserDescriptor>> = Lazy::new(|| {
    vec![
        ParserDescriptor {
            code: "sapflow_all_v1",
            version: "0.1.0",
            output_format: "sapflow_toa5_hierarchical_v1",
            include_in_pipeline: true,
            description: "Parses SapFlowAll multi-sensor TOA5 exports",
        },
        ParserDescriptor {
            code: "cr300_table_v1",
            version: "0.1.0",
            output_format: "sapflow_toa5_hierarchical_v1",
            include_in_pipeline: true,
            description: "Parses CR300 Table-based TOA5 exports",
        },
    ]
});

pub fn all_parser_descriptors() -> &'static [ParserDescriptor] {
    PARSERS.as_slice()
}

static PARSER_IMPLEMENTATIONS: Lazy<Vec<&'static dyn SapflowParser>> = Lazy::new(|| {
    vec![
        &SapFlowAllParserV1 as &dyn SapflowParser,
        &Cr300TableParserV1 as &dyn SapflowParser,
    ]
});

pub fn all_parsers() -> &'static [&'static dyn SapflowParser] {
    PARSER_IMPLEMENTATIONS.as_slice()
}

struct SapFlowAllParserV1;

impl SapflowParser for SapFlowAllParserV1 {
    fn code_identifier(&self) -> &'static str {
        "sapflow_all_v1"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn output_data_format(&self) -> &'static str {
        "sapflow_toa5_hierarchical_v1"
    }

    fn parse(&self, content: &str) -> Result<Box<dyn ParsedData>> {
        let parsed = parse_sapflow_file(content).context("sapflow_all_v1 parser failed")?;
        Ok(Box::new(parsed))
    }
}

struct Cr300TableParserV1;

impl SapflowParser for Cr300TableParserV1 {
    fn code_identifier(&self) -> &'static str {
        "cr300_table_v1"
    }

    fn version(&self) -> &'static str {
        "0.1.0"
    }

    fn output_data_format(&self) -> &'static str {
        "sapflow_toa5_hierarchical_v1"
    }

    fn parse(&self, content: &str) -> Result<Box<dyn ParsedData>> {
        let parsed = parse_sapflow_file(content).context("cr300_table_v1 parser failed")?;
        Ok(Box::new(parsed))
    }
}
