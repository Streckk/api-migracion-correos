use mongodb::Client as MongoClient;
use sea_orm::Database;
use std::{env, net::SocketAddr};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod entities;
mod routes;
mod ssh;
mod state;

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

    let app = routes::router().with_state(state);

    let port = env::var("PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3000);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    println!("API corriendo en http://{}/health", addr);

    axum::serve(tokio::net::TcpListener::bind(addr).await.unwrap(), app)
        .await
        .unwrap();
}
