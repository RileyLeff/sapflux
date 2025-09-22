use std::collections::HashSet;

use sapflux_core::ingestion::{ingest_files, FileInput, FileStatus};

fn fixture(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../sapflux-parser/tests/data")
        .join(name);
    std::fs::read_to_string(path).expect("read fixture")
}

#[test]
fn ingestion_parses_new_file() {
    let content = fixture("CR300Series_420_SapFlowAll.dat");
    let inputs = [FileInput {
        path: "CR300Series_420_SapFlowAll.dat",
        contents: content.as_bytes(),
    }];

    let batch = ingest_files(&inputs, &HashSet::new());

    assert_eq!(batch.parsed.len(), 1);
    assert_eq!(batch.new_hashes.len(), 1);
    assert_eq!(batch.reports.len(), 1);
    assert_eq!(batch.reports[0].status, FileStatus::Parsed);
}

#[test]
fn ingestion_marks_duplicate() {
    let content = fixture("CR300Series_420_SapFlowAll.dat");
    let inputs = [FileInput {
        path: "CR300Series_420_SapFlowAll.dat",
        contents: content.as_bytes(),
    }];

    let first_batch = ingest_files(&inputs, &HashSet::new());
    let mut existing = HashSet::new();
    existing.extend(first_batch.new_hashes.iter().cloned());

    let second_batch = ingest_files(&inputs, &existing);
    assert!(second_batch.parsed.is_empty());
    assert_eq!(second_batch.reports[0].status, FileStatus::Duplicate);
}
