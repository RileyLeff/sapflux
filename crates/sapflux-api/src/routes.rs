use std::sync::Arc;

use axum::{extract::State, Json};
use serde::Deserialize;
use crate::AppState;
use sapflux_processing::WorkflowResult;

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub raw_text: String,
    pub raw_filename: String,
}

pub async fn ingest(
    State(app_state): State<Arc<AppState>>,
    Json(payload): Json<IngestRequest>,
) -> Result<Json<WorkflowResult>, axum::http::StatusCode> {
    let file_hash = blake3::hash(payload.raw_text.as_bytes()).to_hex().to_string();

    let state = app_state.pipeline_state.lock().await;

    state
        .run(&payload.raw_text, &file_hash, &payload.raw_filename)
        .await
        .map(Json)
        .map_err(|err| {
            tracing::error!("pipeline failed: {err}");
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        })
}
