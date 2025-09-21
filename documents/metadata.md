# Sapflux Pipeline Metadata

This document provides a comprehensive overview of the metadata tables used in the Sapflux data pipeline. This schema is designed to provide a robust, normalized, and extensible foundation for storing the contextual information necessary to process and analyze sap flux data.

The schema is designed for a PostgreSQL database (version 17 or newer) and requires the **PostGIS** extension for geospatial data support.

Important Riley Note: We need to add a "project" association with the deployments. And probably a "project" table. E.g. to distinguish between which data belongs to monitoring, stemflow, etc. This is a project table and an update to the deployments table there should be a 

## Database Setup

Before creating the tables, the PostGIS extension must be enabled. This is a one-time operation per database.

```sql
-- Connect to your 'sapflux' database and run this command
CREATE EXTENSION IF NOT EXISTS postgis;
```

---

## Schema Overview

The metadata is organized into a logical hierarchy that mirrors the real-world setup of the field sites and experiments:

1.  **Geographic Hierarchy:** `Sites` -> `Zones` -> `Plots`
2.  **Biological Hierarchy:** `Species` -> `Plants` -> `Stems`
3.  **Instrumentation:** `Datalogger Types` -> `Dataloggers` and `Sensor Types` -> `Thermistor Pairs`
4.  **Linking Table:** `Deployments` connects a specific stem to a specific sensor for a period of time, with a pipeline inclusion flag and flexible installation details.

---

### 1. Sites

This table stores information about the top-level physical locations where data is collected. It serves as the root of the geographic hierarchy.

**PostgreSQL Schema (`sites`)**
```sql
CREATE TABLE IF NOT EXISTS sites (
    -- A unique identifier for the site.
    site_id   UUID PRIMARY KEY,

    -- A short, human-readable code for the site (e.g., "BVL", "MBY").
    code      TEXT UNIQUE NOT NULL,

    -- The full name of the site (e.g., "Brownsville Preserve").
    name      TEXT,

    -- The IANA timezone name (e.g., "America/New_York") for the site.
    -- This is critical for correct timestamp conversions.
    timezone  TEXT NOT NULL,

    -- A PostGIS polygon representing the geographic boundary of the site.
    -- SRID 4326 corresponds to the WGS 84 coordinate system (standard GPS).
    boundary  GEOMETRY(Polygon, 4326)
);
```

**Rust Struct (`sapflux-repository::metadata`)**
```rust
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use geo_types::Polygon;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Site {
    pub site_id: Uuid,
    pub code: String,
    pub name: Option<String>,
    pub timezone: String,
    pub boundary: Option<Polygon>,
}
```

### 2. Zones

Zones represent distinct ecological or experimental areas *within a site*. Each zone belongs to exactly one site.

**PostgreSQL Schema (`zones`)**
```sql
CREATE TABLE IF NOT EXISTS zones (
    zone_id   UUID PRIMARY KEY,
    -- Foreign key linking this zone to its parent site.
    site_id   UUID NOT NULL REFERENCES sites(site_id) ON DELETE CASCADE,
    -- The name of the zone (e.g., "High Forest", "Reference Forest").
    name      TEXT NOT NULL,
    boundary  GEOMETRY(Polygon, 4326),

    -- A zone's name must be unique within its parent site.
    CONSTRAINT uq_zone_site_name UNIQUE (site_id, name)
);
```

**Rust Struct (`sapflux-repository::metadata`)**
```rust
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use geo_types::Polygon;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Zone {
    pub zone_id: Uuid,
    pub site_id: Uuid,
    pub name: String,
    pub boundary: Option<Polygon>,
}
```

### 3. Plots

Plots represent smaller, specific sub-areas *within a zone*. Each plot belongs to exactly one zone.

**PostgreSQL Schema (`plots`)**
```sql
CREATE TABLE IF NOT EXISTS plots (
    plot_id   UUID PRIMARY KEY,
    -- Foreign key linking this plot to its parent zone.
    zone_id   UUID NOT NULL REFERENCES zones(zone_id) ON DELETE CASCADE,
    -- The name of the plot (e.g., "Plot 1", "Control Group").
    name      TEXT NOT NULL,
    boundary  GEOMETRY(Polygon, 4326),

    -- A plot's name must be unique within its parent zone.
    CONSTRAINT uq_plot_zone_name UNIQUE (zone_id, name)
);
```

**Rust Struct (`sapflux-repository::metadata`)**
```rust
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use geo_types::Polygon;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Plot {
    pub plot_id: Uuid,
    pub zone_id: Uuid,
    pub name: String,
    pub boundary: Option<Polygon>,
}
```

### 4. Species

This table stores information about the plant species being monitored. It uses `JSONB` columns to flexibly store hierarchical name data.

**PostgreSQL Schema (`species`)**
```sql
CREATE TABLE IF NOT EXISTS species (
    species_id  UUID PRIMARY KEY,
    code        TEXT UNIQUE NOT NULL, -- e.g., "PITA", "LIST"
    common_name JSONB,
    latin_name  JSONB,
    icon_url    TEXT
);
```

**Rust Structs (`sapflux-repository::metadata`)**
```rust
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Species {
    pub species_id: Uuid,
    pub code: String,
    pub common_name: Option<HierarchicalName>,
    pub latin_name: Option<HierarchicalName>,
    pub icon_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HierarchicalName {
    pub genus: String,
    pub species: String,
    pub subspecies: Option<String>,
}
```

### 5. Plants

This table tracks the individual plants (trees, shrubs, etc.). Each plant is an *instance* of a species and is located within a specific plot.

**PostgreSQL Schema (`plants`)**
```sql
CREATE TABLE IF NOT EXISTS plants (
    plant_id   UUID PRIMARY KEY,
    plot_id    UUID NOT NULL REFERENCES plots(plot_id) ON DELETE CASCADE,
    species_id UUID NOT NULL REFERENCES species(species_id),
    code       TEXT NOT NULL, -- A human-friendly ID, unique within its plot
    location   GEOMETRY(Point, 4326), -- Precise geographic location

    CONSTRAINT uq_plant_plot_code UNIQUE (plot_id, code)
);
```

**Rust Struct (`sapflux-repository::metadata`)**
```rust
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use geo_types::Point;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Plant {
    pub plant_id: Uuid,
    pub plot_id: Uuid,
    pub species_id: Uuid,
    pub code: String,
    pub location: Option<Point>,
}```

### 6. Stems

This table tracks individual, measurable stems on a parent plant. This is essential for multi-stemmed plants where each stem might be instrumented separately.

**PostgreSQL Schema (`stems`)**
```sql
CREATE TABLE IF NOT EXISTS stems (
    stem_id   UUID PRIMARY KEY,
    -- Foreign key linking this stem to its parent plant.
    plant_id  UUID NOT NULL REFERENCES plants(plant_id) ON DELETE CASCADE,

    -- A human-friendly code or rank for the stem (e.g., "1", "2", "main").
    code      TEXT NOT NULL,

    -- Optional physical characteristics of the stem.
    dbh_cm    NUMERIC, -- Diameter at Breast Height

    -- A stem's code must be unique for its parent plant.
    CONSTRAINT uq_stem_plant_code UNIQUE (plant_id, code)
);
```

**Rust Struct (`sapflux-repository::metadata`)**
```rust
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Stem {
    pub stem_id: Uuid,
    pub plant_id: Uuid,
    pub code: String,
    pub dbh_cm: Option<f64>,
}
```

### 7. Datalogger Types

This table defines the *models* or *types* of dataloggers used in the field.

**PostgreSQL Schema (`datalogger_types`)**
```sql
CREATE TABLE IF NOT EXISTS datalogger_types (
    datalogger_type_id UUID PRIMARY KEY,
    code               TEXT UNIQUE NOT NULL, -- e.g., "CR300", "CR200X"
    name               TEXT,                 -- e.g., "Campbell Scientific CR300"
    icon_url           TEXT
);
```

**Rust Struct (`sapflux-repository::metadata`)**
```rust
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataloggerType {
    pub datalogger_type_id: Uuid,
    pub code: String,
    pub name: Option<String>,
    pub icon_url: Option<String>,
}
```

### 8. Dataloggers

This table tracks the specific, individual datalogger *units*. Each unit is an instance of a `datalogger_type`.

**PostgreSQL Schema (`dataloggers`)**
```sql
CREATE TABLE IF NOT EXISTS dataloggers (
    datalogger_id      UUID PRIMARY KEY,
    datalogger_type_id UUID NOT NULL REFERENCES datalogger_types(datalogger_type_id),
    code               TEXT UNIQUE NOT NULL, -- The ID from the data files, e.g., "420"
    aliases            TEXT[] -- Array for known alternate IDs
);
```

**Rust Struct (`sapflux-repository::metadata`)**
```rust
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Datalogger {
    pub datalogger_id: Uuid,
    pub datalogger_type_id: Uuid,
    pub code: String,
    pub aliases: Option<Vec<String>>,
}
```

### 9. Sensor Types and Thermistor Pairs

These tables define the physical specifications of sensor hardware. `sensor_types` stores information about a sensor model, and `sensor_thermistor_pairs` stores details of each measurement point on that model.

**PostgreSQL Schema (`sensor_types`, `sensor_thermistor_pairs`)**
```sql
CREATE TABLE IF NOT EXISTS sensor_types (
    sensor_type_id               UUID PRIMARY KEY,
    code                         TEXT UNIQUE NOT NULL, -- e.g., "implexx_new"
    description                  TEXT,
    downstream_probe_distance_cm NUMERIC,
    upstream_probe_distance_cm   NUMERIC
);

CREATE TABLE IF NOT EXISTS sensor_thermistor_pairs (
    thermistor_pair_id UUID PRIMARY KEY,
    sensor_type_id     UUID NOT NULL REFERENCES sensor_types(sensor_type_id) ON DELETE CASCADE,
    name               TEXT NOT NULL, -- e.g., "outer", "inner"
    depth_mm           NUMERIC NOT NULL,
    position_label     TEXT,          -- e.g., "P1", "P2"
    aliases            JSONB          -- For other possible names, e.g., ["out", "o"]
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_sensor_thermistor_pairs
ON sensor_thermistor_pairs(sensor_type_id, name);
```

**Rust Structs (`sapflux-repository::metadata`)**
```rust
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SensorType {
    pub sensor_type_id: Uuid,
    pub code: String,
    pub description: Option<String>,
    pub downstream_probe_distance_cm: Option<f64>,
    pub upstream_probe_distance_cm: Option<f64>,
    pub thermistor_pairs: Vec<ThermistorPair>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ThermistorPair {
    pub thermistor_pair_id: Uuid,
    pub name: String,
    pub depth_mm: f64,
    pub position_label: Option<String>,
    pub aliases: Vec<String>,
}
```

### 10. Deployments

This is the central "event" table that links all other metadata together. A deployment represents the installation of a specific sensor (on a specific datalogger) onto a specific stem for a defined period.

**PostgreSQL Schema (`deployments`)**
```sql
CREATE TABLE IF NOT EXISTS deployments (
    deployment_id       UUID PRIMARY KEY,
    -- Foreign key to the specific stem being instrumented.
    stem_id             UUID NOT NULL REFERENCES stems(stem_id),
    datalogger_id       UUID NOT NULL REFERENCES dataloggers(datalogger_id),
    sensor_type_id      UUID NOT NULL REFERENCES sensor_types(sensor_type_id),
    sdi_address         TEXT NOT NULL, -- SDI-12 address, e.g., "0", "1"
    start_timestamp_utc TIMESTAMPTZ NOT NULL,
    end_timestamp_utc   TIMESTAMPTZ, -- NULL if currently active in the field
    notes               TEXT,
    -- Flexible JSONB field for any installation details (e.g., height, direction).
    installation_metadata JSONB,

    -- A flag to control whether data from this deployment is included
    -- in processed datasets. Setting to false effectively "archives" it.
    include_in_pipeline BOOLEAN NOT NULL DEFAULT TRUE
);

-- Constraint to prevent deploying the same sensor (logger + address)
-- more than once at the same time.
CREATE UNIQUE INDEX IF NOT EXISTS uq_deployments_sensor_start
ON deployments (datalogger_id, sdi_address, start_timestamp_utc);
```

**Rust Struct (`sapflux-repository::metadata`)**
```rust
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value; // Using serde_json::Value for maximum flexibility
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Deployment {
    pub deployment_id: Uuid,
    pub stem_id: Uuid,
    pub datalogger_id: Uuid,
    pub sensor_type_id: Uuid,
    pub sdi_address: String,
    pub start_timestamp_utc: DateTime<Utc>,
    pub end_timestamp_utc: Option<DateTime<Utc>>,
    pub notes: Option<String>,
    pub installation_metadata: Option<Value>,
    pub include_in_pipeline: bool,
}
```