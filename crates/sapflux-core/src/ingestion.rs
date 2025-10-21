use std::collections::HashSet;

use anyhow::Error;
use blake3::Hasher;
use sapflux_parser::ParserError;
use serde::Serialize;

use crate::parsers::{all_parsers, ParsedData};

#[derive(Debug)]
pub struct FileInput<'a> {
    pub path: &'a str,
    pub contents: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum FileStatus {
    Duplicate,
    Parsed,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct ParserAttemptReport {
    pub parser: &'static str,
    pub message: String,
    pub line_index: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FileReport {
    pub path: String,
    pub hash: String,
    pub status: FileStatus,
    pub parser_attempts: Vec<ParserAttemptReport>,
    pub first_error_line: Option<usize>,
}

pub struct ParsedFile {
    pub hash: String,
    pub data: Box<dyn ParsedData>,
}

impl std::fmt::Debug for ParsedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParsedFile")
            .field("hash", &self.hash)
            .finish()
    }
}

#[derive(Debug)]
pub struct IngestionBatch {
    pub parsed: Vec<ParsedFile>,
    pub reports: Vec<FileReport>,
    pub new_hashes: Vec<String>,
}

pub fn ingest_files(inputs: &[FileInput<'_>], existing_hashes: &HashSet<String>) -> IngestionBatch {
    let mut parsed_files = Vec::new();
    let mut reports = Vec::new();
    let mut new_hashes = Vec::new();
    let mut seen_hashes = existing_hashes.clone();

    for input in inputs {
        let hash = compute_hash(input.contents);
        if seen_hashes.contains(&hash) {
            reports.push(FileReport {
                path: input.path.to_string(),
                hash,
                status: FileStatus::Duplicate,
                parser_attempts: Vec::new(),
                first_error_line: None,
            });
            continue;
        }

        let mut attempts = Vec::new();
        let mut parsed_opt = None;

        let Ok(content_str) = std::str::from_utf8(input.contents) else {
            reports.push(FileReport {
                path: input.path.to_string(),
                hash,
                status: FileStatus::Failed,
                parser_attempts: vec![ParserAttemptReport {
                    parser: "utf8",
                    message: "file contents were not valid UTF-8".to_string(),
                    line_index: None,
                }],
                first_error_line: None,
            });
            continue;
        };

        for parser in all_parsers() {
            match parser.parse(content_str) {
                Ok(mut parsed) => {
                    if let Some(pfd) = parsed.downcast_mut::<sapflux_parser::ParsedFileData>() {
                        pfd.file_hash = hash.clone();
                    }
                    parsed_opt = Some(parsed);
                    break;
                }
                Err(err) => {
                    let line_index = extract_line_index(&err);
                    attempts.push(ParserAttemptReport {
                        parser: parser.code_identifier(),
                        message: err.to_string(),
                        line_index,
                    });
                }
            }
        }

        match parsed_opt {
            Some(data) => {
                seen_hashes.insert(hash.clone());
                new_hashes.push(hash.clone());
                parsed_files.push(ParsedFile {
                    hash: hash.clone(),
                    data,
                });
                reports.push(FileReport {
                    path: input.path.to_string(),
                    hash,
                    status: FileStatus::Parsed,
                    parser_attempts: attempts,
                    first_error_line: None,
                });
            }
            None => {
                let first_error_line = attempts.iter().find_map(|a| a.line_index);
                reports.push(FileReport {
                    path: input.path.to_string(),
                    hash,
                    status: FileStatus::Failed,
                    parser_attempts: attempts,
                    first_error_line,
                });
            }
        }
    }

    IngestionBatch {
        parsed: parsed_files,
        reports,
        new_hashes,
    }
}

fn compute_hash(contents: &[u8]) -> String {
    let mut hasher = Hasher::new();
    hasher.update(contents);
    let hash = hasher.finalize();
    hash.to_hex().to_string()
}

fn extract_line_index(error: &Error) -> Option<usize> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<ParserError>()
            .and_then(|parser_err| match parser_err {
                ParserError::DataRow { line_index, .. } => Some(*line_index),
                ParserError::InvalidHeader { row_index, .. } => Some(*row_index),
                _ => None,
            })
    })
}
