#![cfg(feature = "runtime")]

use std::env;

use anyhow::Result;
use chrono::{TimeZone, Utc};
use sapflux_core::{db, metadata_manifest, seed};
use sqlx::Row;
use uuid::Uuid;

fn manifest_fixture() -> &'static str {
    r#"
[[projects.add]]
code = "TEST"
name = "Test Project"
description = "Integration manifest"

[[sites.add]]
code = "TEST_SITE"
name = "Test Site"
timezone = "America/New_York"
icon_path = "/icons/test.png"
boundary = { type = "Polygon", coordinates = [[[ -105.0, 39.0 ], [ -105.0, 39.1 ], [ -104.9, 39.1 ], [ -104.9, 39.0 ], [ -105.0, 39.0 ]]] }

[[zones.add]]
site_code = "TEST_SITE"
name = "Zone A"
boundary = { type = "Polygon", coordinates = [[[ -105.0, 39.0 ], [ -105.0, 39.05 ], [ -104.95, 39.05 ], [ -104.95, 39.0 ], [ -105.0, 39.0 ]]] }

[[plots.add]]
site_code = "TEST_SITE"
zone_name = "Zone A"
name = "Plot 1"
boundary = { type = "Polygon", coordinates = [[[ -105.0, 39.0 ], [ -105.0, 39.02 ], [ -104.98, 39.02 ], [ -104.98, 39.0 ], [ -105.0, 39.0 ]]] }

[[species.add]]
code = "SPEC"
common_name = { en = "Sample" }
latin_name = { binomial = "Specia example" }
icon_path = "/icons/spec.png"

[[plants.add]]
site_code = "TEST_SITE"
zone_name = "Zone A"
plot_name = "Plot 1"
species_code = "SPEC"
code = "PLANT"
location = { type = "Point", coordinates = [ -104.99, 39.01 ] }

[[stems.add]]
plant_code = "PLANT"
code = "STEM1"
dbh_cm = 12.34

[[datalogger_types.add]]
code = "CR300"
name = "CR300"
icon_path = "/icons/logger.png"

[[dataloggers.add]]
datalogger_type_code = "CR300"
code = "LOGGER42"

[[datalogger_aliases.add]]
datalogger_code = "LOGGER42"
alias = "ALIAS42"
start_timestamp_utc = "2024-01-01T00:00:00Z"
end_timestamp_utc = "2024-12-31T00:00:00Z"

[[sensor_types.add]]
code = "sapflux_probe"
description = "Sapflux thermal sensor"

[[sensor_thermistor_pairs.add]]
sensor_type_code = "sapflux_probe"
name = "inner"
depth_mm = 10.0

[[sensor_thermistor_pairs.add]]
sensor_type_code = "sapflux_probe"
name = "outer"
depth_mm = 5.0

[[deployments]]
project_code = "TEST"
plant_code = "PLANT"
stem_code = "STEM1"
datalogger_code = "LOGGER42"
sensor_type_code = "sapflux_probe"
sdi_address = "0"
start_timestamp_utc = "2025-01-01T00:00:00Z"
end_timestamp_utc = "2025-12-31T00:00:00Z"
include_in_pipeline = true
installation_metadata = { mounting = "shroud" }

[[parameter_overrides]]
parameter_code = "parameter_heat_pulse_duration_s"
value = 3.0
site_code = "TEST_SITE"
"#
}

#[tokio::test]
async fn metadata_manifest_adds_all_entities() -> Result<()> {
    let database_url = match env::var("SAPFLUX_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!(
                "Skipping metadata manifest integration test because SAPFLUX_TEST_DATABASE_URL is not set"
            );
            return Ok(());
        }
    };

    let pool = db::connect(&database_url).await?;
    db::run_migrations(&pool).await?;
    seed::run(&pool).await?;

    sqlx::query(
        "TRUNCATE TABLE parameter_overrides, deployments, sensor_thermistor_pairs, sensor_types, datalogger_aliases, dataloggers, datalogger_types, stems, plants, plots, zones, sites, projects, species CASCADE",
    )
    .execute(&pool)
    .await?;

    let manifest = metadata_manifest::parse_manifest(manifest_fixture())?;
    let (resolved, summary) = metadata_manifest::preflight_manifest(&pool, &manifest).await?;

    assert_eq!(summary.projects_added, 1);
    assert_eq!(summary.sites_added, 1);
    assert_eq!(summary.zones_added, 1);
    assert_eq!(summary.plots_added, 1);
    assert_eq!(summary.species_added, 1);
    assert_eq!(summary.plants_added, 1);
    assert_eq!(summary.stems_added, 1);
    assert_eq!(summary.datalogger_types_added, 1);
    assert_eq!(summary.dataloggers_added, 1);
    assert_eq!(summary.datalogger_aliases_added, 1);
    assert_eq!(summary.sensor_types_added, 1);
    assert_eq!(summary.sensor_thermistor_pairs_added, 2);
    assert_eq!(summary.deployments_added, 1);
    assert_eq!(summary.parameter_overrides_upserted, 1);

    let transaction_id = Uuid::new_v4();
    metadata_manifest::apply_manifest(&pool, &resolved, transaction_id).await?;

    let project_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM projects WHERE code = $1")
        .bind("TEST")
        .fetch_one(&pool)
        .await?;
    assert_eq!(project_count, 1);

    let site_geojson: Option<String> =
        sqlx::query_scalar("SELECT ST_AsGeoJSON(boundary) FROM sites WHERE code = $1")
            .bind("TEST_SITE")
            .fetch_one(&pool)
            .await?;
    let boundary_json = site_geojson.expect("site boundary geojson");
    assert!(boundary_json.contains("Polygon"));

    let alias_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM datalogger_aliases WHERE alias = $1")
            .bind("ALIAS42")
            .fetch_one(&pool)
            .await?;
    assert_eq!(alias_count, 1);

    let deployment_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM deployments WHERE sdi_address = $1")
            .bind("0")
            .fetch_one(&pool)
            .await?;
    assert_eq!(deployment_count, 1);

    let override_row = sqlx::query(
        r#"
            SELECT value, effective_transaction_id
            FROM parameter_overrides
            WHERE parameter_id = (
                SELECT parameter_id FROM parameters WHERE code = $1
            )
              AND site_id = (
                SELECT site_id FROM sites WHERE code = $2
            )
        "#,
    )
    .bind("parameter_heat_pulse_duration_s")
    .bind("TEST_SITE")
    .fetch_one(&pool)
    .await?;

    let effective_id: Uuid = override_row.try_get("effective_transaction_id")?;
    assert_eq!(effective_id, transaction_id);

    Ok(())
}

#[tokio::test]
async fn plants_allow_same_code_in_different_plots() -> Result<()> {
    let database_url = match env::var("SAPFLUX_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("Skipping plant scoping test because SAPFLUX_TEST_DATABASE_URL is not set");
            return Ok(());
        }
    };

    let pool = db::connect(&database_url).await?;
    db::run_migrations(&pool).await?;
    seed::run(&pool).await?;

    sqlx::query(
        "TRUNCATE TABLE parameter_overrides, deployments, sensor_thermistor_pairs, sensor_types, datalogger_aliases, dataloggers, datalogger_types, stems, plants, plots, zones, sites, projects, species CASCADE",
    )
    .execute(&pool)
    .await?;

    let manifest = metadata_manifest::parse_manifest(
        r#"
[[projects.add]]
code = "PLOT_TEST"

[[sites.add]]
code = "SITE"
name = "Site"
timezone = "UTC"

[[zones.add]]
site_code = "SITE"
name = "ZONE"

[[plots.add]]
site_code = "SITE"
zone_name = "ZONE"
name = "PlotA"

[[plots.add]]
site_code = "SITE"
zone_name = "ZONE"
name = "PlotB"

[[species.add]]
code = "SPEC"

[[plants.add]]
site_code = "SITE"
zone_name = "ZONE"
plot_name = "PlotA"
species_code = "SPEC"
code = "PLANT"

[[plants.add]]
site_code = "SITE"
zone_name = "ZONE"
plot_name = "PlotB"
species_code = "SPEC"
code = "PLANT"
"#,
    )?;

    let (_resolved, summary) = metadata_manifest::preflight_manifest(&pool, &manifest).await?;

    assert_eq!(summary.plants_added, 2);

    Ok(())
}

#[tokio::test]
async fn plants_duplicate_in_same_plot_are_rejected() -> Result<()> {
    let database_url = match env::var("SAPFLUX_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("Skipping plant duplicate test because SAPFLUX_TEST_DATABASE_URL is not set");
            return Ok(());
        }
    };

    let pool = db::connect(&database_url).await?;
    db::run_migrations(&pool).await?;
    seed::run(&pool).await?;

    sqlx::query(
        "TRUNCATE TABLE parameter_overrides, deployments, sensor_thermistor_pairs, sensor_types, datalogger_aliases, dataloggers, datalogger_types, stems, plants, plots, zones, sites, projects, species CASCADE",
    )
    .execute(&pool)
    .await?;

    let manifest = metadata_manifest::parse_manifest(
        r#"
[[projects.add]]
code = "PLOT_TEST"

[[sites.add]]
code = "SITE"
name = "Site"
timezone = "UTC"

[[zones.add]]
site_code = "SITE"
name = "ZONE"

[[plots.add]]
site_code = "SITE"
zone_name = "ZONE"
name = "PlotA"

[[species.add]]
code = "SPEC"

[[plants.add]]
site_code = "SITE"
zone_name = "ZONE"
plot_name = "PlotA"
species_code = "SPEC"
code = "PLANT"

[[plants.add]]
site_code = "SITE"
zone_name = "ZONE"
plot_name = "PlotA"
species_code = "SPEC"
code = "PLANT"
"#,
    )?;

    let err = metadata_manifest::preflight_manifest(&pool, &manifest)
        .await
        .expect_err("expected duplicate plant error");

    assert!(err.to_string().contains("already exists"));

    Ok(())
}

#[tokio::test]
async fn stems_allow_same_code_in_different_plants() -> Result<()> {
    let database_url = match env::var("SAPFLUX_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("Skipping stem scoping test because SAPFLUX_TEST_DATABASE_URL is not set");
            return Ok(());
        }
    };

    let pool = db::connect(&database_url).await?;
    db::run_migrations(&pool).await?;
    seed::run(&pool).await?;

    sqlx::query(
        "TRUNCATE TABLE parameter_overrides, deployments, sensor_thermistor_pairs, sensor_types, datalogger_aliases, dataloggers, datalogger_types, stems, plants, plots, zones, sites, projects, species CASCADE",
    )
    .execute(&pool)
    .await?;

    let manifest = metadata_manifest::parse_manifest(
        r#"
[[projects.add]]
code = "STEM_TEST"

[[sites.add]]
code = "SITE"
name = "Site"
timezone = "UTC"

[[zones.add]]
site_code = "SITE"
name = "ZONE"

[[plots.add]]
site_code = "SITE"
zone_name = "ZONE"
name = "PlotA"

[[plots.add]]
site_code = "SITE"
zone_name = "ZONE"
name = "PlotB"

[[species.add]]
code = "SPEC"

[[plants.add]]
site_code = "SITE"
zone_name = "ZONE"
plot_name = "PlotA"
species_code = "SPEC"
code = "PLANT_A"

[[plants.add]]
site_code = "SITE"
zone_name = "ZONE"
plot_name = "PlotB"
species_code = "SPEC"
code = "PLANT_B"

[[stems.add]]
plant_code = "PLANT_A"
code = "STEM"

[[stems.add]]
plant_code = "PLANT_B"
code = "STEM"
"#,
    )?;

    let (_resolved, summary) = metadata_manifest::preflight_manifest(&pool, &manifest).await?;

    assert_eq!(summary.stems_added, 2);

    Ok(())
}

#[tokio::test]
async fn stems_duplicate_in_same_plant_are_rejected() -> Result<()> {
    let database_url = match env::var("SAPFLUX_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("Skipping stem duplicate test because SAPFLUX_TEST_DATABASE_URL is not set");
            return Ok(());
        }
    };

    let pool = db::connect(&database_url).await?;
    db::run_migrations(&pool).await?;
    seed::run(&pool).await?;

    sqlx::query(
        "TRUNCATE TABLE parameter_overrides, deployments, sensor_thermistor_pairs, sensor_types, datalogger_aliases, dataloggers, datalogger_types, stems, plants, plots, zones, sites, projects, species CASCADE",
    )
    .execute(&pool)
    .await?;

    let manifest = metadata_manifest::parse_manifest(
        r#"
[[projects.add]]
code = "STEM_TEST"

[[sites.add]]
code = "SITE"
name = "Site"
timezone = "UTC"

[[zones.add]]
site_code = "SITE"
name = "ZONE"

[[plots.add]]
site_code = "SITE"
zone_name = "ZONE"
name = "PlotA"

[[species.add]]
code = "SPEC"

[[plants.add]]
site_code = "SITE"
zone_name = "ZONE"
plot_name = "PlotA"
species_code = "SPEC"
code = "PLANT"

[[stems.add]]
plant_code = "PLANT"
code = "STEM"

[[stems.add]]
plant_code = "PLANT"
code = "STEM"
"#,
    )?;

    let err = metadata_manifest::preflight_manifest(&pool, &manifest)
        .await
        .expect_err("expected duplicate stem error");

    assert!(err.to_string().contains("already exists"));

    Ok(())
}

#[tokio::test]
async fn parameter_override_requires_plant_for_stem() -> Result<()> {
    let database_url = match env::var("SAPFLUX_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!(
                "Skipping parameter override stem test because SAPFLUX_TEST_DATABASE_URL is not set"
            );
            return Ok(());
        }
    };

    let pool = db::connect(&database_url).await?;
    db::run_migrations(&pool).await?;
    seed::run(&pool).await?;

    sqlx::query(
        "TRUNCATE TABLE parameter_overrides, deployments, sensor_thermistor_pairs, sensor_types, datalogger_aliases, dataloggers, datalogger_types, stems, plants, plots, zones, sites, projects, species CASCADE",
    )
    .execute(&pool)
    .await?;

    let manifest = metadata_manifest::parse_manifest(
        r#"
[[projects.add]]
code = "PARAM_TEST"

[[sites.add]]
code = "SITE"
name = "Site"
timezone = "UTC"

[[zones.add]]
site_code = "SITE"
name = "ZONE"

[[plots.add]]
site_code = "SITE"
zone_name = "ZONE"
name = "PlotA"

[[species.add]]
code = "SPEC"

[[plants.add]]
site_code = "SITE"
zone_name = "ZONE"
plot_name = "PlotA"
species_code = "SPEC"
code = "PLANT"

[[stems.add]]
plant_code = "PLANT"
code = "STEM"

[[parameter_overrides]]
parameter_code = "parameter_heat_pulse_duration_s"
value = 3.0
stem_code = "STEM"
"#,
    )?;

    let err = metadata_manifest::preflight_manifest(&pool, &manifest)
        .await
        .expect_err("expected stem override error");

    assert!(err.to_string().contains("requires plant_code"));

    Ok(())
}

#[tokio::test]
async fn metadata_manifest_rejects_alias_overlap() -> Result<()> {
    let database_url = match env::var("SAPFLUX_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!(
                "Skipping metadata manifest overlap test because SAPFLUX_TEST_DATABASE_URL is not set"
            );
            return Ok(());
        }
    };

    let pool = db::connect(&database_url).await?;
    db::run_migrations(&pool).await?;

    sqlx::query("TRUNCATE TABLE datalogger_aliases, dataloggers, datalogger_types CASCADE")
        .execute(&pool)
        .await?;

    let datalogger_type_id = Uuid::new_v4();
    sqlx::query("INSERT INTO datalogger_types (datalogger_type_id, code) VALUES ($1, $2)")
        .bind(datalogger_type_id)
        .bind("CR300")
        .execute(&pool)
        .await?;

    let datalogger_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO dataloggers (datalogger_id, datalogger_type_id, code) VALUES ($1, $2, $3)",
    )
    .bind(datalogger_id)
    .bind(datalogger_type_id)
    .bind("LOGGER42")
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
            INSERT INTO datalogger_aliases (
                datalogger_alias_id,
                datalogger_id,
                alias,
                active_during
            )
            VALUES ($1, $2, $3, tstzrange($4, $5, '[)'))
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(datalogger_id)
    .bind("ALIAS42")
    .bind(Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).single().unwrap())
    .bind(Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).single().unwrap())
    .execute(&pool)
    .await?;

    let manifest_toml = r#"
[[datalogger_aliases.add]]
datalogger_code = "LOGGER42"
alias = "ALIAS42"
start_timestamp_utc = "2024-05-01T00:00:00Z"
end_timestamp_utc = "2024-10-01T00:00:00Z"
"#;

    let manifest = metadata_manifest::parse_manifest(manifest_toml)?;
    let error = metadata_manifest::preflight_manifest(&pool, &manifest)
        .await
        .expect_err("expected alias overlap to fail");
    let message = format!("{error:#}");
    assert!(message.contains("alias 'ALIAS42' overlaps"));

    Ok(())
}
