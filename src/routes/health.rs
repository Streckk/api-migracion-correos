use axum::{routing::get, Json, Router};

use crate::{routes::structs::HealthResponse, state::AppState};

async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        service: "correo-migrador-api".to_string(),
    })
}

pub fn router() -> Router<AppState> {
    Router::new().route("/health", get(health_check))
}
