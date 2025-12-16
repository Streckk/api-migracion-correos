use axum::{
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use sea_orm::{ConnectionTrait, DbBackend, FromQueryResult, JsonValue, Statement};
use tracing::{error, info};

use crate::{
    routes::structs::{DbCheckResponse, IncomingMailResponse, MailConfigListResponse},
    state::AppState,
};

async fn db_connection_check(State(state): State<AppState>) -> impl IntoResponse {
    match state.mysql.ping().await {
        Ok(_) => {
            info!("Conexión a MySQL verificada correctamente");
            (
                StatusCode::OK,
                Json(DbCheckResponse {
                    status: "ok".to_string(),
                    detail: "Conexión exitosa a MySQL".to_string(),
                }),
            )
        }
        Err(err) => {
            error!("Fallo al verificar MySQL: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(DbCheckResponse {
                    status: "error".to_string(),
                    detail: format!("Error al verificar MySQL: {err}"),
                }),
            )
        }
    }
}

async fn list_mail_configs(State(state): State<AppState>) -> impl IntoResponse {
    let statement =
        Statement::from_string(DbBackend::MySql, "SELECT * FROM configuracion_de_correos");

    match state.mysql.query_all_raw(statement).await {
        Ok(rows) => {
            info!(
                "Consulta de configuracion_de_correos obtuvo {} filas",
                rows.len()
            );
            let response_rows = rows
                .into_iter()
                .map(|row| JsonValue::from_query_result(&row, "").unwrap_or(JsonValue::Null))
                .collect::<Vec<_>>();

            (
                StatusCode::OK,
                Json(MailConfigListResponse {
                    status: "ok".to_string(),
                    rows: response_rows,
                    detail: None,
                }),
            )
        }
        Err(err) => {
            error!("Error ejecutando SELECT en MySQL: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(MailConfigListResponse {
                    status: "error".to_string(),
                    rows: Vec::new(),
                    detail: Some(format!("Error ejecutando SELECT: {err}")),
                }),
            )
        }
    }
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/db-connection", get(db_connection_check))
        .route("/email-configs", get(list_mail_configs))
        .route("/incoming-email/:num_caso", get(get_incoming_email_by_case))
}

async fn get_incoming_email_by_case(
    State(state): State<AppState>,
    AxumPath(num_caso): AxumPath<String>,
) -> impl IntoResponse {
    let statement = Statement::from_sql_and_values(
        DbBackend::MySql,
        "SELECT * FROM correos_entrada WHERE Num_Caso = ?",
        vec![num_caso.clone().into()],
    );

    match state.mysql.query_all_raw(statement).await {
        Ok(rows) if rows.is_empty() => (
            StatusCode::NOT_FOUND,
            Json(IncomingMailResponse {
                status: "not_found".to_string(),
                rows: Vec::new(),
                detail: Some(format!(
                    "No se encontraron correos para Num_Caso {num_caso}"
                )),
            }),
        ),
        Ok(rows) => {
            let payload = rows
                .into_iter()
                .map(|row| JsonValue::from_query_result(&row, "").unwrap_or(JsonValue::Null))
                .collect::<Vec<_>>();
            (
                StatusCode::OK,
                Json(IncomingMailResponse {
                    status: "ok".to_string(),
                    rows: payload,
                    detail: None,
                }),
            )
        }
        Err(err) => {
            error!("Error consultando correos_entrada: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(IncomingMailResponse {
                    status: "error".to_string(),
                    rows: Vec::new(),
                    detail: Some(format!(
                        "No se pudo consultar correos_entrada para {num_caso}: {err}"
                    )),
                }),
            )
        }
    }
}
