use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{ get, post },
    Json,
    Router,
};
use mongodb::bson::{ doc, oid::ObjectId };
use sea_orm::{ ConnectionTrait, DbBackend, Statement };

use crate::{
    entities::email_config::EmailConfigDocument,
    routes::structs::{ MongoCheckResponse, MongoSetupResponse, MongoSyncResponse },
    state::AppState,
};

async fn mongo_connection_check(State(state): State<AppState>) -> impl IntoResponse {
    let db = state.mongo.database("admin");
    match db.run_command(doc! { "ping": 1 }).await {
        Ok(_) =>
            (
                StatusCode::OK,
                Json(MongoCheckResponse {
                    status: "ok".to_string(),
                    detail: "Conexión exitosa a MongoDB".to_string(),
                }),
            ),
        Err(err) =>
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(MongoCheckResponse {
                    status: "error".to_string(),
                    detail: format!("Error al verificar MongoDB: {err}"),
                }),
            ),
    }
}

async fn setup_mongo_collections(State(state): State<AppState>) -> impl IntoResponse {
    const DB_NAME: &str = "correos_exchange_queretaro";
    const COLLECTIONS: [&str; 4] = ["configuration", "msg-mime", "msg-struct", "msg-teams"];

    let db = state.mongo.database(DB_NAME);
    let mut created = Vec::new();

    for &collection in COLLECTIONS.iter() {
        if let Err(err) = db.create_collection(collection).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(MongoSetupResponse {
                    status: "error".to_string(),
                    detail: format!("No se pudo crear la colección {collection}: {err}"),
                }),
            );
        }
        created.push(collection);
    }

    (
        StatusCode::OK,
        Json(MongoSetupResponse {
            status: "ok".to_string(),
            detail: format!("Base {DB_NAME} inicializada con colecciones: {}", created.join(", ")),
        }),
    )
}

async fn sync_email_configs_from_mysql(State(state): State<AppState>) -> impl IntoResponse {
    let db_name = std::env
        ::var("MONGO_DB_NAME")
        .unwrap_or_else(|_| "correos_exchange_queretaro".into());
    let collection_name = std::env
        ::var("MONGO_CONFIG_COLLECTION")
        .unwrap_or_else(|_| "configuration".into());
    let client_id = std::env::var("API_CLIENT_ID").unwrap_or_default();
    let client_secret = std::env::var("API_CLIENT_SECRET").unwrap_or_default();
    let tenant_id = std::env::var("API_TENANT_ID").unwrap_or_default();

    let statement = Statement::from_string(
        DbBackend::MySql,
        "SELECT * FROM configuracion_de_correos"
    );

    let rows = match state.mysql.query_all_raw(statement).await {
        Ok(rows) => rows,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(MongoSyncResponse {
                    status: "error".to_string(),
                    detail: format!("No se pudo leer MySQL: {err}"),
                }),
            );
        }
    };

    let mut documents = Vec::new();

    for row in rows.into_iter().take(1) {
        let email: String = row.try_get("", "Correo_Salida").unwrap_or_default();
        let name_session: String = row.try_get("", "Nombre_Mascara").unwrap_or_default();
        let incoming_server: String = row.try_get("", "Servidor_Entrada").unwrap_or_default();
        let outbound_server: String = row.try_get("", "Servidor_Salida").unwrap_or_default();
        let password: String = row.try_get("", "Password_Correo_Entrada").unwrap_or_default();
        let port: i32 = row.try_get("", "Puerto").unwrap_or(0);

        let document = EmailConfigDocument {
            id: ObjectId::new(),
            name_session,
            email: email.clone(),
            outbound_email: email,
            incoming_server,
            outbound_server,
            imap_server: String::new(),
            password,
            port,
            port_outbound: 587,
            is_block_send_email: false,
            tls: false,
            client_id: client_id.clone(),
            tenant_id: tenant_id.clone(),
            client_secret_id: client_secret.clone(),
            access_token: String::new(),
        };

        documents.push(document);
    }

    let collection = state.mongo
        .database(&db_name)
        .collection::<EmailConfigDocument>(&collection_name);

    match collection.insert_many(documents).await {
        Ok(result) =>
            (
                StatusCode::OK,
                Json(MongoSyncResponse {
                    status: "ok".to_string(),
                    detail: format!(
                        "Se insertaron {} registros en {collection_name}",
                        result.inserted_ids.len()
                    ),
                }),
            ),
        Err(err) =>
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(MongoSyncResponse {
                    status: "error".to_string(),
                    detail: format!("Error insertando en MongoDB: {err}"),
                }),
            ),
    }
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/mongo-connection", get(mongo_connection_check))
        .route("/mongo/setup", post(setup_mongo_collections))
        .route("/mongo/sync-email-configs", post(sync_email_configs_from_mysql))
}
