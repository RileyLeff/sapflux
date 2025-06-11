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

-- Table to store Daylight Saving Time transition rules.
CREATE TABLE IF NOT EXISTS dst_transitions (
    id SERIAL PRIMARY KEY,
    -- 'start' for DST beginning, 'end' for DST ending.
    transition_action TEXT NOT NULL CHECK (transition_action IN ('start', 'end')),
    -- The naive, local wall-clock time of the transition.
    ts_local TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    -- It's good practice to ensure we don't have duplicate rules.
    UNIQUE (transition_action, ts_local)
);

-- An index on the timestamp will make lookups very fast.
CREATE INDEX IF NOT EXISTS idx_dst_transitions_ts ON dst_transitions(ts_local);

-- ====================================================================
-- METADATA TABLES (NEW)
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
    sensor_id TEXT NOT NULL UNIQUE, -- e.g., "implexx_new"
    downstream_probe_distance_cm NUMERIC NOT NULL,
    upstream_probe_distance_cm NUMERIC NOT NULL,
    thermistor_depth_1_mm INTEGER NOT NULL,
    thermistor_depth_2_mm INTEGER NOT NULL
);

-- A simple key-value store for global calculation parameters.
CREATE TABLE IF NOT EXISTS parameters (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE, -- e.g., "wound_diameter_cm"
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
    sdi_address TEXT NOT NULL, -- Will be validated by our SdiAddress newtype

    tree_id TEXT NOT NULL,
    
    -- Foreign key to the projects table.
    project_id INTEGER NOT NULL REFERENCES projects(id),
    
    -- Foreign key to the sensors table.
    sensor_id INTEGER NOT NULL REFERENCES sensors(id),
    
    -- Stores project-specific attributes as a flexible JSON object.
    -- Ensures the data is stored in Postgres's efficient binary JSON format.
    attributes JSONB,

    -- Ensures a given datalogger/SDI address combo can only be active in one deployment at a time.
    -- The `COALESCE` trick treats all NULL end times as a single value for uniqueness checks.
    UNIQUE (datalogger_id, sdi_address, (COALESCE(end_time_utc, 'infinity'::timestamptz)))
);

-- Indexes to speed up common queries on the deployments table.
CREATE INDEX IF NOT EXISTS idx_deployments_project_id ON deployments(project_id);
CREATE INDEX IF NOT EXISTS idx_deployments_sensor_id ON deployments(sensor_id);
CREATE INDEX IF NOT EXISTS idx_deployments_datalogger_id ON deployments(datalogger_id);
CREATE INDEX IF NOT EXISTS idx_deployments_time_range ON deployments(start_time_utc, end_time_utc);