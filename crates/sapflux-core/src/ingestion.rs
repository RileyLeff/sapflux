use std::collections::HashSet;

use blake3::Hasher;

use crate::parsers::{all_parsers, ParsedData};

#[derive(Debug)]
pub struct FileInput<'a> {
    pub path: &'a str,
    pub contents: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileStatus {
    Duplicate,
    Parsed,
    Failed,
}

#[derive(Debug)]
pub struct ParserAttemptReport {
    pub parser: &'static str,
    pub message: String,
}

#[derive(Debug)]
pub struct FileReport {
    pub path: String,
    pub hash: String,
    pub status: FileStatus,
    pub parser_attempts: Vec<ParserAttemptReport>,
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

    for input in inputs {
        let hash = compute_hash(input.contents);
        if existing_hashes.contains(&hash) {
            reports.push(FileReport {
                path: input.path.to_string(),
                hash,
                status: FileStatus::Duplicate,
                parser_attempts: Vec::new(),
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
                }],
            });
            continue;
        };

        for parser in all_parsers() {
            match parser.parse(content_str) {
                Ok(parsed) => {
                    parsed_opt = Some(parsed);
                    break;
                }
                Err(err) => attempts.push(ParserAttemptReport {
                    parser: parser.code_identifier(),
                    message: err.to_string(),
                }),
            }
        }

        match parsed_opt {
            Some(data) => {
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
                });
            }
            None => {
                reports.push(FileReport {
                    path: input.path.to_string(),
                    hash,
                    status: FileStatus::Failed,
                    parser_attempts: attempts,
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
