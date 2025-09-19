pub mod errors;
pub mod formats;
pub mod model;
mod registry;

pub use errors::{ParserAttempt, ParserError};
pub use model::{
    ArchiveError, FileMetadata, LoggerData, ParsedFileData, Sdi12Address, SensorData,
    ThermistorDepth, ThermistorPairData,
};
pub use registry::{SapflowParser, parse_sapflow_file, parse_with_parsers};

#[cfg(test)]
mod tests;
