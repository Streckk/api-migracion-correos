use axum::{
    extract::State,
    http::StatusCode,
    response::{ IntoResponse, Response },
    routing::post,
    Json,
    Router,
};
use serde::Deserialize;

use crate::{
    routes::structs::UserSyncResponse,
    services::{ tickets_sync, users_sync::{ self, UserSyncResult } },
    state::AppState,
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tickets/sync", post(tickets_sync::sync_tickets_by_range))
        .route("/tickets/obtener_correos_notas", post(tickets_sync::sync_notes_by_range))
        .route("/tickets/obtener_correos_respuesta", post(tickets_sync::sync_responses_by_range))
        .route("/users/sync", post(sync_users))
}

#[derive(Debug, Default, Deserialize)]
struct UserSyncRequest {
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    user_ids: Vec<String>,
}

async fn sync_users(
    State(state): State<AppState>,
    payload: Option<Json<UserSyncRequest>>
) -> Response {
    let requested_ids = payload
        .map(|Json(body)| {
            let mut ids = body.user_ids;
            if let Some(single) = body.user_id {
                if !ids.contains(&single) {
                    ids.push(single);
                }
            }
            ids
        })
        .unwrap_or_default();

    match users_sync::sync_users(&state, requested_ids).await {
        Ok(UserSyncResult::NoUsers) => {
            let body = Json(UserSyncResponse {
                status: "ok".to_string(),
                detail: "No se encontraron usuarios para migrar".to_string(),
                processed: 0,
                inserted: 0,
                skipped: 0,
                errors: 0,
                skipped_missing_reference: 0,
                skipped_duplicate: 0,
                missing_references: Vec::new(),
            });
            (StatusCode::OK, body).into_response()
        }
        Ok(UserSyncResult::Summary(summary)) => {
            let status = if summary.errors == 0 { "ok" } else { "partial" };
            let detail = format!(
                "Usuarios procesados: {} | insertados: {} | omitidos: {} | errores: {}",
                summary.processed,
                summary.inserted,
                summary.skipped,
                summary.errors
            );
            let body = Json(UserSyncResponse {
                status: status.to_string(),
                detail,
                processed: summary.processed,
                inserted: summary.inserted,
                skipped: summary.skipped,
                errors: summary.errors,
                skipped_missing_reference: summary.skipped_missing_reference,
                skipped_duplicate: summary.skipped_duplicate,
                missing_references: summary.missing_references,
            });
            (StatusCode::OK, body).into_response()
        }
        Err(err) => {
            let body = Json(UserSyncResponse {
                status: "error".to_string(),
                detail: err.to_string(),
                processed: 0,
                inserted: 0,
                skipped: 0,
                errors: 0,
                skipped_missing_reference: 0,
                skipped_duplicate: 0,
                missing_references: Vec::new(),
            });
            (StatusCode::INTERNAL_SERVER_ERROR, body).into_response()
        }
    }
}
