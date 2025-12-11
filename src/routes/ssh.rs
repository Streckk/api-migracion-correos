use axum::{
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;

use crate::{ssh::SshError, state::AppState};

#[derive(Serialize)]
struct ConnectTestResponse {
    status: String,
    detail: String,
}

#[derive(Serialize)]
struct ListResponse {
    path: String,
    entries: Vec<crate::ssh::RemoteDirEntry>,
}

#[derive(Serialize)]
struct ErrorResponse {
    status: String,
    detail: String,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/ssh/connect-test", post(connect_test))
        .route("/ssh/list", get(list_base_dir))
        .route("/ssh/list/:entry", get(list_entry_dir))
}

async fn connect_test(State(state): State<AppState>) -> impl IntoResponse {
    match state.ssh_service.test_connection().await {
        Ok(_) => (
            StatusCode::OK,
            Json(ConnectTestResponse {
                status: "ok".into(),
                detail: "Conexión SSH establecida correctamente".into(),
            }),
        )
            .into_response(),
        Err(err) => ssh_error_response(err),
    }
}

async fn list_base_dir(State(state): State<AppState>) -> Response {
    list_remote_dir_for(&state, "").await
}

async fn list_entry_dir(
    State(state): State<AppState>,
    AxumPath(entry): AxumPath<String>,
) -> Response {
    let ticket_folder = entry.trim();
    if ticket_folder.is_empty() {
        return list_remote_dir_for(&state, "").await;
    }
    list_remote_dir_for(&state, ticket_folder).await
}

async fn list_remote_dir_for(state: &AppState, request_path: &str) -> Response {
    let resolved_path = state.ssh_service.resolved_path(request_path);
    tracing::info!(target = "ssh", request = %request_path, resolved = %resolved_path, "Listando directorio remoto");
    match state.ssh_service.list_remote_dir(request_path).await {
        Ok(entries) => (
            StatusCode::OK,
            Json(ListResponse {
                path: resolved_path,
                entries,
            }),
        )
            .into_response(),
        Err(err) => ssh_error_response(err),
    }
}
fn ssh_error_response(err: SshError) -> Response {
    let status = match err {
        SshError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
        SshError::AuthenticationFailed => StatusCode::UNAUTHORIZED,
        SshError::HostKeyUnknown { .. } | SshError::HostKeyMismatch { .. } => {
            StatusCode::BAD_GATEWAY
        }
        _ => StatusCode::BAD_GATEWAY,
    };

    let body = Json(ErrorResponse {
        status: "error".into(),
        detail: err.to_string(),
    });

    (status, body).into_response()
}
