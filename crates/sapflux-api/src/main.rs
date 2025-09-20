mod routes;
mod state;

use std::sync::Arc;

use anyhow::Result;
use axum::{routing::post, Router};
use routes::ingest;
use sapflux_bucket::S3Config;
use state::PipelineState;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{info, Level};
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pipeline_state: Arc<Mutex<PipelineState>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL not set");
    let bucket_endpoint = std::env::var("BUCKET_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
    let bucket_name = std::env::var("BUCKET_NAME").unwrap_or_else(|_| "sapflux-parsed".to_string());
    let bucket_access_key = std::env::var("BUCKET_ACCESS_KEY").ok();
    let bucket_secret_key = std::env::var("BUCKET_SECRET_KEY").ok();

    let bucket_config = S3Config {
        bucket: bucket_name,
        region: "us-east-1".to_string(),
        endpoint: Some(bucket_endpoint),
        access_key_id: bucket_access_key,
        secret_access_key: bucket_secret_key,
        force_path_style: true,
    };

    let parser_config_id = Uuid::new_v4();
    let parser_version = "0.1.0".to_string();

    let pipeline_state = PipelineState::new(
        &database_url,
        bucket_config,
        parser_config_id,
        parser_version,
    )
    .await?;

    let app_state = Arc::new(AppState { pipeline_state });

    let router = Router::new()
        .route("/files", post(ingest))
        .with_state(app_state);

    let listener = TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, 3000)).await?;
    info!("listening on {}", listener.local_addr()?);
    axum::serve(listener, router.into_make_service())
        .await?;

    Ok(())
}
