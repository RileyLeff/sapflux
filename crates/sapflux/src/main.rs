use anyhow::{Context, Result};
use axum::{
    extract::{Json, State},
    http::StatusCode,
    routing::{get, post},
    Router,
};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use clap::{Parser, Subcommand};
use sapflux_core::{
    db, seed,
    transactions::{
        execute_transaction, PipelineStatus, TransactionFile, TransactionReceipt,
        TransactionRequest as CoreTransactionRequest,
    },
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tracing::info;
use tracing_subscriber::EnvFilter;

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
    let state = AppState { pool };

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/admin/migrate", post(run_migrations))
        .route("/admin/seed", post(run_seed))
        .route("/transactions", post(handle_transaction))
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

async fn handle_transaction(
    State(state): State<AppState>,
    Json(req): Json<TransactionRequest>,
) -> Result<Json<TransactionResponse>, (StatusCode, &'static str)> {
    if req.files.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "transaction requires at least one file",
        ));
    }

    let mut files = Vec::with_capacity(req.files.len());
    for file in req.files {
        let bytes = BASE64_STANDARD
            .decode(file.contents_base64.as_bytes())
            .map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    "file contents must be base64 encoded",
                )
            })?;

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

    let receipt = execute_transaction(&state.pool, core_req)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "transaction processing failed",
            )
        })?;

    let status = receipt.pipeline.status.clone();

    Ok(Json(TransactionResponse { status, receipt }))
}
