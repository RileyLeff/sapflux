CREATE TABLE IF NOT EXISTS processed_files (
    file_hash TEXT PRIMARY KEY,
    raw_filename TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    parser_version TEXT NOT NULL,
    artifact_manifest JSONB NOT NULL,
    warnings JSONB,
    status TEXT NOT NULL CHECK (status IN ('processing', 'ready', 'failed')),
    idempotency_key UUID
);

CREATE INDEX IF NOT EXISTS idx_processed_files_status ON processed_files (status);
CREATE INDEX IF NOT EXISTS idx_processed_files_processed_at ON processed_files (processed_at);
