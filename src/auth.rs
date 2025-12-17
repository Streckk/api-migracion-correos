use axum::{
    body::Body,
    http::{header, HeaderMap, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use tracing::{debug, warn};

use crate::state::AppState;

pub async fn require_token(req: Request<Body>, next: Next) -> Response {
    let token = match extract_token(req.headers()) {
        Some(token) => token,
        None => return unauthorized_response("Falta encabezado Authorization o X-Api-Key"),
    };

    let Some(state) = req.extensions().get::<AppState>().cloned() else {
        return unauthorized_response("No se pudo acceder al estado de la aplicación");
    };

    if state.is_token_valid(&token) {
        debug!("Token válido recibido");
        next.run(req).await
    } else {
        warn!("Intento de acceso con token inválido");
        unauthorized_response("Token inválido")
    }
}

fn extract_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|raw| {
            raw.strip_prefix("Bearer ")
                .map(|token| token.trim().to_string())
                .filter(|token| !token.is_empty())
                .or_else(|| {
                    let trimmed = raw.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                })
        })
        .or_else(|| {
            headers
                .get("x-api-key")
                .and_then(|value| value.to_str().ok())
                .map(|value| value.trim().to_string())
                .filter(|token| !token.is_empty())
        })
}

fn unauthorized_response(detail: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "status": "error",
            "detail": detail
        })),
    )
        .into_response()
}
