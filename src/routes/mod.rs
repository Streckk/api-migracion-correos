use axum::{middleware, Router};

pub mod health;
pub mod mongo;
pub mod mysql;
pub mod ssh;
pub mod storage;
pub mod structs;
pub mod sync;

use crate::{auth, state::AppState};

pub fn router() -> Router<AppState> {
    let protected_routes = Router::new()
        .merge(mysql::router())
        .merge(mongo::router())
        .merge(storage::router())
        .merge(ssh::router())
        .merge(sync::router())
        .layer(middleware::from_fn(auth::require_token));

    Router::new()
        .merge(health::router())
        .merge(protected_routes)
}
