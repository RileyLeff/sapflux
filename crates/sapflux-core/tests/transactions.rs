#![cfg(feature = "runtime")]

use std::env;

use anyhow::Result;
use sapflux_core::{
    db,
    object_store::ObjectStore,
    transactions::{execute_transaction, PipelineStatus, TransactionFile, TransactionRequest},
};
use tokio::runtime::Runtime;

fn fixture(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../sapflux-parser/tests/data")
        .join(name);
    std::fs::read_to_string(path).expect("read fixture")
}

#[test]
fn pipeline_skips_when_no_deployments() -> Result<()> {
    let database_url = match env::var("SAPFLUX_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!(
                "Skipping pipeline_skips_when_no_deployments because SAPFLUX_TEST_DATABASE_URL is not set"
            );
            return Ok(());
        }
    };

    let rt = Runtime::new()?;
    rt.block_on(async move {
        let pool = db::connect(&database_url).await?;
        db::run_migrations(&pool).await?;

        sqlx::query(
            "TRUNCATE TABLE raw_files, transactions, deployments, stems, plants, plots, zones, projects, species, datalogger_aliases, dataloggers, datalogger_types, sensor_types, parameter_overrides, sites CASCADE",
        )
        .execute(&pool)
        .await?;

        let file_bytes = fixture("CR300Series_420_SapFlowAll.dat");

        let receipt = execute_transaction(
            &pool,
            &ObjectStore::noop(),
            TransactionRequest {
                user_id: "tester".into(),
                message: None,
                dry_run: false,
                files: vec![TransactionFile {
                    path: "CR300Series_420_SapFlowAll.dat".into(),
                    contents: file_bytes.as_bytes().to_vec(),
                }],
                metadata_manifest: None,
            },
        )
        .await?;

        assert!(receipt.transaction_id.is_some());
        assert_eq!(receipt.pipeline.status, PipelineStatus::Skipped);
        assert!(receipt.pipeline.pipeline.is_none());
        assert!(receipt.artifacts.is_none());
        assert!(receipt.metadata_summary.is_none());
        assert_eq!(receipt.ingestion_summary.parsed, 1);
        assert_eq!(receipt.ingestion_summary.failed, 0);
        assert_eq!(receipt.ingestion_summary.duplicates, 0);
        let transaction_id = receipt
            .transaction_id
            .expect("transaction id should be present for non-dry-run");

        let (outcome,): (String,) = sqlx::query_as(
            "SELECT outcome::text FROM transactions WHERE transaction_id = $1",
        )
        .bind(transaction_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(outcome.as_str(), "ACCEPTED");

        let (raw_count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM raw_files WHERE ingesting_transaction_id = $1",
        )
        .bind(transaction_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(raw_count, 1);

        sqlx::query(
            "TRUNCATE TABLE raw_files, transactions CASCADE",
        )
        .execute(&pool)
        .await?;

        Ok(())
    })
}
