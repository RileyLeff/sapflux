-- db/schema.sql

-- A simple table to define the schemas our system can recognize.
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
    detected_schema_name TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_files_file_hash ON raw_files(file_hash);

-- Table to store Daylight Saving Time transition rules.
CREATE TABLE IF NOT EXISTS dst_transitions (
    id SERIAL PRIMARY KEY,
    transition_action TEXT NOT NULL CHECK (transition_action IN ('start', 'end')),
    ts_local TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    UNIQUE (transition_action, ts_local)
);
CREATE INDEX IF NOT EXISTS idx_dst_transitions_ts ON dst_transitions(ts_local);

-- ====================================================================
-- METADATA TABLES
-- ====================================================================

-- Stores high-level project information.
CREATE TABLE IF NOT EXISTS projects (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT
);

-- Stores hardware specifications for different sensor models.
CREATE TABLE IF NOT EXISTS sensors (
    id SERIAL PRIMARY KEY,
    sensor_id TEXT NOT NULL UNIQUE,
    downstream_probe_distance_cm NUMERIC NOT NULL,
    upstream_probe_distance_cm NUMERIC NOT NULL,
    thermistor_depth_1_mm INTEGER NOT NULL,
    thermistor_depth_2_mm INTEGER NOT NULL
);

-- A simple key-value store for global calculation parameters.
CREATE TABLE IF NOT EXISTS parameters (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    value NUMERIC NOT NULL,
    unit TEXT,
    description TEXT
);

-- The central table linking raw data to its full context.
CREATE TABLE IF NOT EXISTS deployments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    start_time_utc TIMESTAMPTZ NOT NULL,
    end_time_utc TIMESTAMPTZ, -- NULL means the deployment is currently active.
    datalogger_id INTEGER NOT NULL,
    sdi_address TEXT NOT NULL,
    tree_id TEXT NOT NULL,
    project_id INTEGER NOT NULL REFERENCES projects(id),
    sensor_id INTEGER NOT NULL REFERENCES sensors(id),
    attributes JSONB
);

-- ====================================================================
-- INDEXES
-- ====================================================================

-- Ensures a given datalogger/SDI address combo can only be active in ONE deployment at a time.
-- This is the standard SQL way to create a unique constraint on an expression.
-- The `COALESCE` trick treats all NULL end times as a single, unique value ('infinity').
CREATE UNIQUE INDEX IF NOT EXISTS idx_deployments_unique_active_sensor
ON deployments (datalogger_id, sdi_address, (COALESCE(end_time_utc, 'infinity'::timestamptz)));

-- Indexes to speed up common queries on the deployments table.
CREATE INDEX IF NOT EXISTS idx_deployments_project_id ON deployments(project_id);
CREATE INDEX IF NOT EXISTS idx_deployments_sensor_id ON deployments(sensor_id);
CREATE INDEX IF NOT EXISTS idx_deployments_datalogger_id ON deployments(datalogger_id);
CREATE INDEX IF NOT EXISTS idx_deployments_time_range ON deployments(start_time_utc, end_time_utc);


-- ====================================================================
-- MANUAL CORRECTIONS TABLE
-- ====================================================================

-- Stores explicit, one-off corrections for specific raw data files,
-- identified by their immutable SHA-256 hash.
CREATE TABLE IF NOT EXISTS manual_fixes (
    id SERIAL PRIMARY KEY,
    file_hash TEXT NOT NULL UNIQUE,
    action TEXT NOT NULL,
    value JSONB NOT NULL,
    description TEXT
);
CREATE INDEX IF NOT EXISTS idx_manual_fixes_file_hash ON manual_fixes(file_hash);