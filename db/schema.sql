-- db/schema.sql

-- A simple table to define the schemas our system can recognize.
-- We're not using this yet, but it's good practice.
CREATE TABLE IF NOT EXISTS file_schemas (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT
);

-- The main table for storing our immutable, raw data.
CREATE TABLE IF NOT EXISTS raw_files (
    id BIGSERIAL PRIMARY KEY,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    file_hash TEXT NOT NULL UNIQUE,
    file_content BYTEA NOT NULL,
    -- For now we'll just store the name, we can foreign key this later.
    detected_schema_name TEXT NOT NULL
);

-- An index on the hash column will make our duplicate check extremely fast.
CREATE INDEX IF NOT EXISTS idx_raw_files_file_hash ON raw_files(file_hash);