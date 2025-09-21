use std::fmt;

use thiserror::Error;

#[derive(Debug, Clone)]
pub struct ParserAttempt {
    pub parser: &'static str,
    pub message: String,
}

impl ParserAttempt {
    pub fn new(parser: &'static str, message: impl Into<String>) -> Self {
        Self {
            parser,
            message: message.into(),
        }
    }
}

impl fmt::Display for ParserAttempt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.parser, self.message)
    }
}

#[derive(Debug, Error)]
pub enum ParserError {
    #[error("{parser} format mismatch: {reason}")]
    FormatMismatch {
        parser: &'static str,
        reason: String,
    },

    #[error("{parser} header row {row_index} invalid: {message}")]
    InvalidHeader {
        parser: &'static str,
        row_index: usize,
        message: String,
    },

    #[error("{parser} CSV error: {source}")]
    Csv {
        parser: &'static str,
        #[source]
        source: csv::Error,
    },

    #[error("{parser} data row {line_index} invalid: {message}")]
    DataRow {
        parser: &'static str,
        line_index: usize,
        message: String,
    },

    #[error("{parser} validation error: {message}")]
    Validation {
        parser: &'static str,
        message: String,
    },

    #[error("{parser} file did not contain any data rows")]
    EmptyData { parser: &'static str },

    #[error("no parser recognized this file; attempts: {attempts:?}")]
    NoMatchingParser { attempts: Vec<ParserAttempt> },
}
