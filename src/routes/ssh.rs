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
    let ticket_folder = entry.trim().to_string();
    if ticket_folder.is_empty() {
        return list_remote_dir_for(&state, "").await;
    }
    let resolved_primary = state.ssh_service.resolved_path(ticket_folder.as_str());
    tracing::info!(
        target = "ssh",
        entry = %ticket_folder,
        resolved = %resolved_primary,
        "Solicitando listado para entrada específica"
    );
    match fetch_remote_dir_entries(&state, ticket_folder.as_str()).await {
        Ok((resolved_path, entries)) => respond_with_entries(resolved_path, entries),
        Err(primary_err) => {
            if let Some(fallback_path) =
                fallback_numeric_ticket_path(&state, ticket_folder.as_str())
            {
                tracing::debug!(
                    target = "ssh",
                    original = %ticket_folder,
                    fallback = %fallback_path,
                    error = %primary_err,
                    "Reintentando listado agregando sufijo numérico al base_path"
                );
                tracing::info!(
                    target = "ssh",
                    attempt = "fallback",
                    entry = %ticket_folder,
                    absolute_path = %fallback_path,
                    "Listando directorio con ruta calculada manualmente"
                );
                return match fetch_remote_dir_entries(&state, fallback_path.as_str()).await {
                    Ok((resolved_path, entries)) => respond_with_entries(resolved_path, entries),
                    Err(final_err) => {
                        tracing::error!(
                            target = "ssh",
                            entry = %ticket_folder,
                            resolved_primary = %resolved_primary,
                            fallback = %fallback_path,
                            error = %final_err,
                            "No se pudo listar el directorio incluso tras el fallback"
                        );
                        ssh_error_response(final_err)
                    }
                };
            }
            tracing::error!(
                target = "ssh",
                entry = %ticket_folder,
                resolved = %resolved_primary,
                error = %primary_err,
                "No se pudo listar el directorio remoto"
            );
            ssh_error_response(primary_err)
        }
    }
}

async fn list_remote_dir_for(state: &AppState, request_path: &str) -> Response {
    match fetch_remote_dir_entries(state, request_path).await {
        Ok((resolved_path, entries)) => respond_with_entries(resolved_path, entries),
        Err(err) => {
            let resolved = state.ssh_service.resolved_path(request_path);
            tracing::error!(
                target = "ssh",
                request = %request_path,
                resolved = %resolved,
                error = %err,
                "No se pudo listar el directorio remoto solicitado"
            );
            ssh_error_response(err)
        }
    }
}

async fn fetch_remote_dir_entries(
    state: &AppState,
    request_path: &str,
) -> Result<(String, Vec<crate::ssh::RemoteDirEntry>), SshError> {
    let resolved_path = state.ssh_service.resolved_path(request_path);
    tracing::info!(
        target = "ssh",
        request = %request_path,
        resolved = %resolved_path,
        "Listando directorio remoto"
    );
    let entries = state.ssh_service.list_remote_dir(request_path).await?;
    Ok((resolved_path, entries))
}

fn respond_with_entries(
    resolved_path: String,
    entries: Vec<crate::ssh::RemoteDirEntry>,
) -> Response {
    (
        StatusCode::OK,
        Json(ListResponse {
            path: resolved_path,
            entries,
        }),
    )
        .into_response()
}

fn fallback_numeric_ticket_path(state: &AppState, request_path: &str) -> Option<String> {
    if request_path.is_empty() || !request_path.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(format!(
        "{} {}",
        state.ssh_service.base_path(),
        request_path
    ))
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
