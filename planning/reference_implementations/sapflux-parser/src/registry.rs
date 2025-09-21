use crate::errors::{ParserAttempt, ParserError};
use crate::formats::{Cr300TableParser, SapFlowAllParser};
use crate::model::ParsedFileData;

pub trait SapflowParser {
    fn name(&self) -> &'static str;
    fn parse(&self, content: &str) -> Result<ParsedFileData, ParserError>;
}

pub fn parse_sapflow_file(content: &str) -> Result<ParsedFileData, ParserError> {
    let sap_flow_all = SapFlowAllParser::default();
    let cr300_table = Cr300TableParser::default();
    let parsers: [&dyn SapflowParser; 2] = [&sap_flow_all, &cr300_table];
    parse_with_parsers(content, &parsers)
}

pub fn parse_with_parsers(
    content: &str,
    parsers: &[&dyn SapflowParser],
) -> Result<ParsedFileData, ParserError> {
    let mut attempts = Vec::new();

    for parser in parsers {
        match parser.parse(content) {
            Ok(parsed) => return Ok(parsed),
            Err(ParserError::FormatMismatch { reason, .. }) => {
                attempts.push(ParserAttempt::new(parser.name(), reason));
            }
            Err(err) => return Err(err),
        }
    }

    Err(ParserError::NoMatchingParser { attempts })
}
