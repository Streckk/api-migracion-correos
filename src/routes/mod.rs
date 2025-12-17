use axum::Router;

pub mod health;
pub mod mongo;
pub mod mysql;
pub mod ssh;
pub mod storage;
pub mod structs;
pub mod sync;

use crate::state::AppState;

pub fn public_router() -> Router<AppState> {
    Router::new().merge(health::router())
}

pub fn protected_router() -> Router<AppState> {
    Router::new()
        .merge(mysql::router())
        .merge(mongo::router())
        .merge(storage::router())
        .merge(ssh::router())
        .merge(sync::router())
}
