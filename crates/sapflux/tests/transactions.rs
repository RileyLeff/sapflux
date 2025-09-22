use std::env;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sapflux_core::{
    db,
    object_store::ObjectStore,
    seed,
    transactions::{execute_transaction, PipelineStatus, TransactionFile, TransactionRequest},
};
use serde_json::json;
use tokio::runtime::Runtime;
use uuid::Uuid;

fn fixture(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../sapflux-parser/tests/data")
        .join(name);
    std::fs::read_to_string(path).expect("read fixture")
}

#[test]
fn execute_transaction_roundtrip() -> Result<()> {
    let database_url = match env::var("SAPFLUX_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!(
                "Skipping transactions integration test because SAPFLUX_TEST_DATABASE_URL is not set"
            );
            return Ok(());
        }
    };

    let rt = Runtime::new()?;
    let result: Result<()> = rt.block_on(async move {
        let pool = db::connect(&database_url).await?;
        db::run_migrations(&pool).await?;
        seed::run(&pool).await?;

        sqlx::query(
            "TRUNCATE TABLE raw_files, transactions, deployments, stems, plants, plots, zones, projects, species, datalogger_aliases, dataloggers, datalogger_types, sensor_types, sites CASCADE",
        )
        .execute(&pool)
        .await?;

        let project_id = Uuid::new_v4();
        let site_id = Uuid::new_v4();
        let zone_id = Uuid::new_v4();
        let plot_id = Uuid::new_v4();
        let species_id = Uuid::new_v4();
        let plant_id = Uuid::new_v4();
        let stem_outer_id = Uuid::new_v4();
        let stem_inner_id = Uuid::new_v4();
        let datalogger_type_id = Uuid::new_v4();
        let datalogger_id = Uuid::new_v4();
        let sensor_type_id = Uuid::new_v4();

        sqlx::query("INSERT INTO projects (project_id, code, name) VALUES ($1, $2, $3)")
            .bind(project_id)
            .bind("TEST")
            .bind("Test Project")
            .execute(&pool)
            .await?;

        sqlx::query(
            "INSERT INTO sites (site_id, code, name, timezone) VALUES ($1, $2, $3, $4)",
        )
        .bind(site_id)
        .bind("TEST_SITE")
        .bind("Integration Test Site")
        .bind("America/New_York")
        .execute(&pool)
        .await?;

        sqlx::query("INSERT INTO zones (zone_id, site_id, name) VALUES ($1, $2, $3)")
            .bind(zone_id)
            .bind(site_id)
            .bind("Zone A")
            .execute(&pool)
            .await?;

        sqlx::query("INSERT INTO plots (plot_id, zone_id, name) VALUES ($1, $2, $3)")
            .bind(plot_id)
            .bind(zone_id)
            .bind("Plot 1")
            .execute(&pool)
            .await?;

        sqlx::query("INSERT INTO species (species_id, code) VALUES ($1, $2)")
            .bind(species_id)
            .bind("SPEC")
            .execute(&pool)
            .await?;

        sqlx::query(
            "INSERT INTO plants (plant_id, plot_id, species_id, code) VALUES ($1, $2, $3, $4)",
        )
        .bind(plant_id)
        .bind(plot_id)
        .bind(species_id)
        .bind("PLANT")
        .execute(&pool)
        .await?;

        sqlx::query("INSERT INTO stems (stem_id, plant_id, code) VALUES ($1, $2, $3)")
            .bind(stem_outer_id)
            .bind(plant_id)
            .bind("STEM_OUT")
            .execute(&pool)
            .await?;

        sqlx::query("INSERT INTO stems (stem_id, plant_id, code) VALUES ($1, $2, $3)")
            .bind(stem_inner_id)
            .bind(plant_id)
            .bind("STEM_IN")
            .execute(&pool)
            .await?;

        sqlx::query(
            "INSERT INTO datalogger_types (datalogger_type_id, code, name) VALUES ($1, $2, $3)",
        )
        .bind(datalogger_type_id)
        .bind("CR300")
        .bind("CR300 Series")
        .execute(&pool)
        .await?;

        sqlx::query(
            "INSERT INTO dataloggers (datalogger_id, datalogger_type_id, code) VALUES ($1, $2, $3)",
        )
        .bind(datalogger_id)
        .bind(datalogger_type_id)
        .bind("420")
        .execute(&pool)
        .await?;

        sqlx::query(
            "INSERT INTO sensor_types (sensor_type_id, code, description) VALUES ($1, $2, $3)",
        )
        .bind(sensor_type_id)
        .bind("sapflux_probe")
        .bind("Sapflux thermal sensor")
        .execute(&pool)
        .await?;

        let start_time: DateTime<Utc> = "2025-07-28T00:00:00Z".parse().unwrap();

        for (stem_id, address) in [(stem_outer_id, "0"), (stem_inner_id, "1")] {
            sqlx::query(
                r#"INSERT INTO deployments (
                        deployment_id,
                        project_id,
                        stem_id,
                        datalogger_id,
                        sensor_type_id,
                        sdi_address,
                        start_timestamp_utc,
                        end_timestamp_utc,
                        installation_metadata,
                        include_in_pipeline
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8, TRUE)"#,
            )
            .bind(Uuid::new_v4())
            .bind(project_id)
            .bind(stem_id)
            .bind(datalogger_id)
            .bind(sensor_type_id)
            .bind(address)
            .bind(start_time)
            .bind(json!({}))
            .execute(&pool)
            .await?;
        }

        let file_bytes = fixture("CR300Series_420_SapFlowAll.dat");
        let object_store = ObjectStore::noop();

        let receipt = execute_transaction(
            &pool,
            &object_store,
            TransactionRequest {
                user_id: "tester".into(),
                message: Some("integration-dry-run".into()),
                dry_run: true,
                files: vec![TransactionFile {
                    path: "CR300Series_420_SapFlowAll.dat".into(),
                    contents: file_bytes.as_bytes().to_vec(),
                }],
            },
        )
        .await?;

        assert!(receipt.transaction_id.is_none());
        assert_eq!(receipt.ingestion_summary.parsed, 1);
        assert_eq!(receipt.ingestion_summary.failed, 0);
        assert_eq!(receipt.pipeline.status, PipelineStatus::Success);
        assert!(receipt.pipeline.row_count.unwrap_or_default() > 0);

        let committed = execute_transaction(
            &pool,
            &object_store,
            TransactionRequest {
                user_id: "tester".into(),
                message: Some("integration-commit".into()),
                dry_run: false,
                files: vec![TransactionFile {
                    path: "CR300Series_420_SapFlowAll.dat".into(),
                    contents: file_bytes.as_bytes().to_vec(),
                }],
            },
        )
        .await?;

        let transaction_id = committed
            .transaction_id
            .context("expected committed transaction id")?;
        assert_eq!(committed.pipeline.status, PipelineStatus::Success);

        let outcome: String = sqlx::query_scalar(
            "SELECT outcome::text FROM transactions WHERE transaction_id = $1",
        )
        .bind(transaction_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(outcome, "ACCEPTED");

        let raw_file_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM raw_files").fetch_one(&pool).await?;
        assert_eq!(raw_file_count, 1);

        Ok(())
    });

    result?;

    Ok(())
}
