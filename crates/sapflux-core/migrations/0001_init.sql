-- Initial Sapflux schema
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Enumerated types
CREATE TYPE transaction_outcome AS ENUM ('PENDING', 'ACCEPTED', 'REJECTED');
CREATE TYPE run_status AS ENUM ('SUCCESS', 'FAILED');

-- Core audit tables
CREATE TABLE transactions (
    transaction_id      UUID PRIMARY KEY,
    user_id             TEXT NOT NULL,
    message             TEXT,
    attempted_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    outcome             transaction_outcome NOT NULL DEFAULT 'PENDING',
    receipt             JSONB
);

CREATE TABLE raw_files (
    file_hash               TEXT PRIMARY KEY,
    ingesting_transaction_id UUID NOT NULL REFERENCES transactions(transaction_id),
    ingest_context          JSONB,
    include_in_pipeline     BOOLEAN NOT NULL DEFAULT TRUE
);

-- Parser / pipeline inventory
CREATE TABLE data_formats (
    data_format_id      UUID PRIMARY KEY,
    code_identifier     TEXT UNIQUE NOT NULL,
    schema_definition   JSONB
);

CREATE TABLE parsers (
    parser_id           UUID PRIMARY KEY,
    code_identifier     TEXT UNIQUE NOT NULL,
    version             TEXT NOT NULL,
    output_data_format_id UUID NOT NULL REFERENCES data_formats(data_format_id),
    include_in_pipeline BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE processing_pipelines (
    pipeline_id         UUID PRIMARY KEY,
    code_identifier     TEXT UNIQUE NOT NULL,
    version             TEXT NOT NULL,
    input_data_format_id UUID NOT NULL REFERENCES data_formats(data_format_id),
    include_in_pipeline BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE runs (
    run_id                  UUID PRIMARY KEY,
    triggering_transaction_id UUID NOT NULL REFERENCES transactions(transaction_id),
    processing_pipeline_id  UUID NOT NULL REFERENCES processing_pipelines(pipeline_id),
    started_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at             TIMESTAMPTZ,
    status                  run_status NOT NULL,
    git_commit_hash         TEXT NOT NULL,
    run_log                 JSONB
);

CREATE TABLE outputs (
    output_id                       UUID PRIMARY KEY,
    run_id                          UUID UNIQUE NOT NULL REFERENCES runs(run_id),
    object_store_path               TEXT NOT NULL,
    reproducibility_cartridge_path  TEXT NOT NULL,
    row_count                       INTEGER,
    is_latest                       BOOLEAN NOT NULL DEFAULT FALSE
);

-- Project hierarchy
CREATE TABLE projects (
    project_id      UUID PRIMARY KEY,
    code            TEXT UNIQUE NOT NULL,
    name            TEXT,
    description     TEXT
);

CREATE TABLE sites (
    site_id     UUID PRIMARY KEY,
    code        TEXT UNIQUE NOT NULL,
    name        TEXT,
    timezone    TEXT NOT NULL,
    boundary    GEOMETRY(Polygon, 4326),
    icon_path   TEXT
);

CREATE TABLE zones (
    zone_id     UUID PRIMARY KEY,
    site_id     UUID NOT NULL REFERENCES sites(site_id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    boundary    GEOMETRY(Polygon, 4326),
    CONSTRAINT uq_zone_site_name UNIQUE (site_id, name)
);

CREATE TABLE plots (
    plot_id     UUID PRIMARY KEY,
    zone_id     UUID NOT NULL REFERENCES zones(zone_id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    boundary    GEOMETRY(Polygon, 4326),
    CONSTRAINT uq_plot_zone_name UNIQUE (zone_id, name)
);

-- Biological hierarchy
CREATE TABLE species (
    species_id  UUID PRIMARY KEY,
    code        TEXT UNIQUE NOT NULL,
    common_name JSONB,
    latin_name  JSONB,
    icon_path   TEXT
);

CREATE TABLE plants (
    plant_id    UUID PRIMARY KEY,
    plot_id     UUID NOT NULL REFERENCES plots(plot_id) ON DELETE CASCADE,
    species_id  UUID NOT NULL REFERENCES species(species_id),
    code        TEXT NOT NULL,
    location    GEOMETRY(Point, 4326),
    CONSTRAINT uq_plant_plot_code UNIQUE (plot_id, code)
);

CREATE TABLE stems (
    stem_id     UUID PRIMARY KEY,
    plant_id    UUID NOT NULL REFERENCES plants(plant_id) ON DELETE CASCADE,
    code        TEXT NOT NULL,
    dbh_cm      NUMERIC,
    CONSTRAINT uq_stem_plant_code UNIQUE (plant_id, code)
);

-- Instrumentation
CREATE TABLE datalogger_types (
    datalogger_type_id UUID PRIMARY KEY,
    code               TEXT UNIQUE NOT NULL,
    name               TEXT,
    icon_path          TEXT
);

CREATE TABLE dataloggers (
    datalogger_id      UUID PRIMARY KEY,
    datalogger_type_id UUID NOT NULL REFERENCES datalogger_types(datalogger_type_id),
    code               TEXT UNIQUE NOT NULL
);

CREATE TABLE datalogger_aliases (
    datalogger_alias_id UUID PRIMARY KEY,
    datalogger_id       UUID NOT NULL REFERENCES dataloggers(datalogger_id) ON DELETE CASCADE,
    alias               TEXT NOT NULL,
    active_during       TSTZRANGE NOT NULL,
    CONSTRAINT uq_datalogger_alias UNIQUE (datalogger_id, alias, active_during)
);

CREATE INDEX idx_datalogger_alias_range ON datalogger_aliases USING GIST (alias, active_during);

ALTER TABLE datalogger_aliases
    ADD CONSTRAINT uq_alias_no_overlap EXCLUDE USING gist (
        alias WITH =,
        active_during WITH &&
    );

ALTER TABLE datalogger_aliases
    ADD CONSTRAINT uq_alias_no_adjacency EXCLUDE USING gist (
        alias WITH =,
        active_during WITH -|-
    );

CREATE TABLE sensor_types (
    sensor_type_id UUID PRIMARY KEY,
    code           TEXT UNIQUE NOT NULL,
    description    TEXT
);

CREATE TABLE sensor_thermistor_pairs (
    thermistor_pair_id UUID PRIMARY KEY,
    sensor_type_id     UUID NOT NULL REFERENCES sensor_types(sensor_type_id) ON DELETE CASCADE,
    name               TEXT NOT NULL,
    depth_mm           NUMERIC NOT NULL,
    CONSTRAINT uq_sensor_thermistor_pair_name UNIQUE (sensor_type_id, name)
);

-- Deployments
CREATE TABLE deployments (
    deployment_id       UUID PRIMARY KEY,
    project_id          UUID NOT NULL REFERENCES projects(project_id),
    stem_id             UUID NOT NULL REFERENCES stems(stem_id),
    datalogger_id       UUID NOT NULL REFERENCES dataloggers(datalogger_id),
    sensor_type_id      UUID NOT NULL REFERENCES sensor_types(sensor_type_id),
    sdi_address         TEXT NOT NULL,
    start_timestamp_utc TIMESTAMPTZ NOT NULL,
    end_timestamp_utc   TIMESTAMPTZ,
    installation_metadata JSONB,
    include_in_pipeline BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT uq_deployment_sensor_time UNIQUE (datalogger_id, sdi_address, start_timestamp_utc)
);

ALTER TABLE deployments
    ADD COLUMN active_during TSTZRANGE GENERATED ALWAYS AS (
        tstzrange(start_timestamp_utc, end_timestamp_utc, '[)')
    ) STORED;

ALTER TABLE deployments
    ADD CONSTRAINT uq_deployment_no_overlap EXCLUDE USING gist (
        datalogger_id WITH =,
        sdi_address WITH =,
        active_during WITH &&
    );

ALTER TABLE deployments
    ADD CONSTRAINT uq_deployment_no_adjacency EXCLUDE USING gist (
        datalogger_id WITH =,
        sdi_address WITH =,
        active_during WITH -|-
    );

-- Parameters
CREATE TABLE parameters (
    parameter_id    UUID PRIMARY KEY,
    code            TEXT UNIQUE NOT NULL,
    description     TEXT,
    unit            TEXT
);

CREATE TABLE parameter_overrides (
    override_id     UUID PRIMARY KEY,
    parameter_id    UUID NOT NULL REFERENCES parameters(parameter_id),
    value           JSONB NOT NULL,
    site_id         UUID REFERENCES sites(site_id) ON DELETE CASCADE,
    species_id      UUID REFERENCES species(species_id) ON DELETE CASCADE,
    zone_id         UUID REFERENCES zones(zone_id) ON DELETE CASCADE,
    plot_id         UUID REFERENCES plots(plot_id) ON DELETE CASCADE,
    plant_id        UUID REFERENCES plants(plant_id) ON DELETE CASCADE,
    stem_id         UUID REFERENCES stems(stem_id) ON DELETE CASCADE,
    deployment_id   UUID REFERENCES deployments(deployment_id) ON DELETE CASCADE,
    effective_transaction_id UUID NOT NULL REFERENCES transactions(transaction_id),
    CONSTRAINT uq_parameter_override_context UNIQUE (
        parameter_id, site_id, species_id, zone_id, plot_id, plant_id, stem_id, deployment_id
    )
);
