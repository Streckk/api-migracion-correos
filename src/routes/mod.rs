use axum::Router;

pub mod health;
pub mod mongo;
pub mod mysql;
pub mod storage;
pub mod structs;

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .merge(health::router())
        .merge(mysql::router())
        .merge(mongo::router())
        .merge(storage::router())
}
