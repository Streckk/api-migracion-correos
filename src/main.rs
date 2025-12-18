use mongodb::Client as MongoClient;
use sea_orm::Database;
use std::{
    env,
    net::{IpAddr, SocketAddr},
};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod auth;
mod entities;
mod routes;
mod ssh;
mod state;
mod storage;

use axum::{middleware, Router};
use state::AppState;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let mysql_url = env::var("MY_SQL_CONEXION").expect("Configura MY_SQL_CONEXION en el entorno");
    let mysql = Database::connect(&mysql_url)
        .await
        .expect("No se pudo establecer conexión con MySQL vía SeaORM");

    let mongo_uri = env::var("MONGO_URI").expect("Configura MONGO_URI en el entorno");
    let mongo = MongoClient::with_uri_str(&mongo_uri)
        .await
        .expect("No se pudo establecer conexión con MongoDB");

    let state = AppState::new(mysql, mongo);

    let auth_layer = middleware::from_fn_with_state(state.clone(), auth::require_token);

    let protected_routes = routes::protected_router().layer(auth_layer);
    let app = Router::new()
        .merge(routes::public_router())
        .merge(protected_routes)
        .with_state(state);

    let port = env::var("PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3000);

    let host = env::var("IP_SERVER")
        .ok()
        .and_then(|value| value.parse::<IpAddr>().ok())
        .unwrap_or_else(|| IpAddr::from([0, 0, 0, 0]));

    let addr = SocketAddr::new(host, port);

    println!("API corriendo en http://{}/health", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app.into_make_service())
        .await
        .unwrap();
}
