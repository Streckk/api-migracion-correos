use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use mongodb::bson::doc;

use crate::{
    routes::structs::MongoCheckResponse,
    state::AppState,
};

async fn mongo_connection_check(State(state): State<AppState>) -> impl IntoResponse {
    let db = state.mongo.database("admin");
    match db.run_command(doc! { "ping": 1 }).await {
        Ok(_) => (
            StatusCode::OK,
            Json(MongoCheckResponse {
                status: "ok".to_string(),
                detail: "Conexión exitosa a MongoDB".to_string(),
            }),
        ),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(MongoCheckResponse {
                status: "error".to_string(),
                detail: format!("Error al verificar MongoDB: {err}"),
            }),
        ),
    }
}

pub fn router() -> Router<AppState> {
    Router::new().route("/mongo-connection", get(mongo_connection_check))
}
