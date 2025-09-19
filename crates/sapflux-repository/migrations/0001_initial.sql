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

CREATE TABLE IF NOT EXISTS sites (
    site_id    UUID PRIMARY KEY,
    code       TEXT UNIQUE NOT NULL,
    name       TEXT,
    timezone   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS species (
    species_id UUID PRIMARY KEY,
    code       TEXT UNIQUE NOT NULL,
    common_name TEXT,
    latin_name  TEXT
);

CREATE TABLE IF NOT EXISTS sensor_types (
    sensor_type_id UUID PRIMARY KEY,
    code           TEXT UNIQUE NOT NULL,
    description    TEXT,
    downstream_probe_distance_cm NUMERIC,
    upstream_probe_distance_cm NUMERIC
);

CREATE TABLE IF NOT EXISTS sensor_thermistor_pairs (
    thermistor_pair_id UUID PRIMARY KEY,
    sensor_type_id     UUID NOT NULL REFERENCES sensor_types(sensor_type_id) ON DELETE CASCADE,
    name               TEXT NOT NULL,
    depth_mm           NUMERIC NOT NULL,
    position_label     TEXT,
    aliases            JSONB
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_sensor_thermistor_pairs ON sensor_thermistor_pairs(sensor_type_id, name);

CREATE TABLE IF NOT EXISTS parser_config (
    config_id UUID PRIMARY KEY,
    version   TEXT NOT NULL,
    settings  JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS deployments (
    deployment_id      UUID PRIMARY KEY,
    logger_id          TEXT      NOT NULL,
    sdi_address        TEXT      NOT NULL,
    sensor_type_id     UUID      NOT NULL REFERENCES sensor_types(sensor_type_id),
    site_id            UUID      NOT NULL REFERENCES sites(site_id),
    tree_id            TEXT      NOT NULL,
    species_id         UUID      NOT NULL REFERENCES species(species_id),
    zone               TEXT,
    plot               TEXT,
    start_timestamp_utc TIMESTAMPTZ NOT NULL,
    end_timestamp_utc   TIMESTAMPTZ,
    notes              TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_deployments_logger_sdi_start
    ON deployments (logger_id, sdi_address, start_timestamp_utc);

CREATE TABLE IF NOT EXISTS dataset_versions (
    dataset_version_id   UUID PRIMARY KEY,
    version_tag          TEXT UNIQUE NOT NULL,
    semantic_tag         TEXT,
    description          TEXT,
    manifest_key         TEXT NOT NULL,
    parquet_key          TEXT NOT NULL,
    pipeline_git_sha     TEXT,
    parser_config_version TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dataset_latest (
    id              BOOLEAN PRIMARY KEY DEFAULT TRUE,
    dataset_version_id UUID NOT NULL REFERENCES dataset_versions(dataset_version_id)
);
