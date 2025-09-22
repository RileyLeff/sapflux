use anyhow::{Context, Result};
use axum::{
    extract::{Json, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use chrono::{Duration as ChronoDuration, Utc};
use clap::{Parser, Subcommand};
use sapflux_core::{
    db,
    object_store::ObjectStore,
    outputs, seed,
    transactions::{
        execute_transaction, PipelineStatus, TransactionFile, TransactionReceipt,
        TransactionRequest as CoreTransactionRequest,
    },
};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tracing::info;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(author, version, about = "Sapflux CLI / API runner", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start the Axum API server
    Serve(ServeArgs),
}

#[derive(clap::Args, Debug)]
struct ServeArgs {
    /// Address to bind the API server
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: SocketAddr,
}

#[derive(Clone)]
struct AppState {
    pool: db::DbPool,
    object_store: Arc<ObjectStore>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Serve(args) => run_server(args).await,
    }
}

async fn connect_pool() -> Result<db::DbPool> {
    let database_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("SAPFLUX_DATABASE_URL"))
        .context("DATABASE_URL (or SAPFLUX_DATABASE_URL) must be set")?;
    db::connect(&database_url).await
}

async fn run_server(args: ServeArgs) -> Result<()> {
    let pool = connect_pool().await?;
    let store = ObjectStore::from_env().context("failed to configure object store")?;
    info!(?store, "configured object store");
    let state = AppState {
        pool,
        object_store: Arc::new(store),
    };

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/admin/migrate", post(run_migrations))
        .route("/admin/seed", post(run_seed))
        .route("/transactions", post(handle_transaction))
        .route("/outputs/:id/download", get(download_output))
        .with_state(state);

    let addr = args.addr;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("failed to bind listener")?;
    info!(%addr, "Starting API server");
    axum::serve(listener, app.into_make_service())
        .await
        .context("Axum server failed")
}

async fn health_check() -> &'static str {
    "ok"
}

#[derive(Serialize)]
struct AdminResponse {
    message: String,
}

async fn run_migrations(
    State(state): State<AppState>,
) -> Result<Json<AdminResponse>, (StatusCode, &'static str)> {
    db::run_migrations(&state.pool)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "migration failed"))?;
    Ok(Json(AdminResponse {
        message: "migrations applied".to_string(),
    }))
}

async fn run_seed(
    State(state): State<AppState>,
) -> Result<Json<AdminResponse>, (StatusCode, &'static str)> {
    db::run_migrations(&state.pool)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "migration failed"))?;
    seed::run(&state.pool)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "seed failed"))?;
    Ok(Json(AdminResponse {
        message: "seed complete".to_string(),
    }))
}

#[derive(Debug, Deserialize)]
struct TransactionFilePayload {
    path: String,
    contents_base64: String,
}

#[derive(Debug, Deserialize)]
struct TransactionRequest {
    pub message: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
    pub files: Vec<TransactionFilePayload>,
}

#[derive(Debug, Serialize)]
struct TransactionResponse {
    pub status: PipelineStatus,
    pub receipt: TransactionReceipt,
}

#[derive(Debug, Deserialize)]
struct OutputDownloadQuery {
    #[serde(default)]
    include_cartridge: bool,
}

#[derive(Debug, Serialize)]
struct OutputDownloadResponse {
    url: String,
    expires_at: String,
}

async fn handle_transaction(
    State(state): State<AppState>,
    Json(req): Json<TransactionRequest>,
) -> Response {
    if req.files.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "transaction requires at least one file",
        )
            .into_response();
    }

    let mut files = Vec::with_capacity(req.files.len());
    for file in req.files {
        let bytes = match BASE64_STANDARD.decode(file.contents_base64.as_bytes()) {
            Ok(bytes) => bytes,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    "file contents must be base64 encoded",
                )
                    .into_response();
            }
        };

        files.push(TransactionFile {
            path: file.path,
            contents: bytes,
        });
    }

    let core_req = CoreTransactionRequest {
        user_id: "anonymous".to_string(),
        message: req.message,
        dry_run: req.dry_run,
        files,
    };

    match execute_transaction(&state.pool, &state.object_store, core_req).await {
        Ok(receipt) => {
            let status = receipt.pipeline.status.clone();
            let body = TransactionResponse { status, receipt };
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "transaction processing failed",
        )
            .into_response(),
    }
}

async fn download_output(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(query): Query<OutputDownloadQuery>,
) -> Result<Json<OutputDownloadResponse>, (StatusCode, &'static str)> {
    let Some(paths) = outputs::fetch_output_paths(&state.pool, id)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "failed to load output"))?
    else {
        return Err((StatusCode::NOT_FOUND, "output not found"));
    };

    let key = if query.include_cartridge {
        paths.cartridge_path
    } else {
        paths.object_store_path
    };

    let expiry = Duration::from_secs(900);
    let url = state
        .object_store
        .presign_get(&key, expiry)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to presign output",
            )
        })?;

    let Some(url) = url else {
        return Err((StatusCode::NOT_FOUND, "output not available"));
    };

    let expires_at = (Utc::now() + ChronoDuration::from_std(expiry).unwrap()).to_rfc3339();

    Ok(Json(OutputDownloadResponse { url, expires_at }))
}
