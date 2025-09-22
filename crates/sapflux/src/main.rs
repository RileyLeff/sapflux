use anyhow::{Context, Result};
use axum::{extract::State, response::IntoResponse, routing::{get, post}, Json, Router};
use clap::{Parser, Subcommand};
use serde::Serialize;
use sapflux_core::{db, seed};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};
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

async fn run_migrations(State(state): State<AppState>) -> Result<Json<AdminResponse>, (axum::http::StatusCode, &'static str)> {
    db::run_migrations(&state.pool)
        .await
        .map_err(|_| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "migration failed"))?;
    Ok(Json(AdminResponse {
        message: "migrations applied".to_string(),
    }))
}

async fn run_seed(State(state): State<AppState>) -> Result<Json<AdminResponse>, (axum::http::StatusCode, &'static str)> {
    db::run_migrations(&state.pool)
        .await
        .map_err(|_| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "migration failed"))?;
    seed::run(&state.pool)
        .await
        .map_err(|_| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "seed failed"))?;
    Ok(Json(AdminResponse {
        message: "seed complete".to_string(),
    }))
}
