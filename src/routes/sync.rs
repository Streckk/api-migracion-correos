use std::{
    collections::{HashMap, HashSet},
    env,
    sync::Arc,
};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use chrono::{Duration, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::America::Monterrey;
use futures::stream::{self, StreamExt, TryStreamExt};
use mime_guess::MimeGuess;
use mongodb::bson::{doc, oid::ObjectId, Bson, Document};
use sea_orm::{ConnectionTrait, DbBackend, Statement, Value};
use serde::Deserialize;
use tracing::{error, info, warn};

use crate::{
    entities::{
        email_config::EmailConfigDocument,
        msg_mime::{MsgMimeDocument, MsgMimeFile},
        msg_struct::{MsgContact, MsgStructDocument},
        user_migration::{
            hash_password_argon2id, normalize_campaign_name, normalize_lookup_key, split_name,
            MongoUserDocument, MysqlUser, NormalizedUser, UserSettingsDocument,
        },
    },
    routes::structs::{MessageSyncResponse, TicketBatchResponse, TicketSyncResponse, UserSyncResponse},
    ssh::config::RemoteDirEntry,
    ssh::SshError,
    state::AppState,
    storage::StorageError,
};

const DEFAULT_CASE_BATCH_SIZE: usize = 10;
const DEFAULT_CASE_PARALLELISM: usize = 2;
const DEFAULT_FILE_UPLOAD_CONCURRENCY: usize = 4;
const DEFAULT_USER_BATCH_SIZE: usize = 25;

fn case_batch_size() -> usize {
    env::var("CASE_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_CASE_BATCH_SIZE)
}

fn case_parallelism() -> usize {
    env::var("CASE_PARALLELISM")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_CASE_PARALLELISM)
}

fn file_upload_concurrency() -> usize {
    env::var("FILE_UPLOAD_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_FILE_UPLOAD_CONCURRENCY)
}

fn user_batch_size() -> usize {
    env::var("USER_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_USER_BATCH_SIZE)
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tickets/sync", post(sync_tickets_by_range))
        .route("/tickets/obtener_correos_notas", post(sync_notes_by_range))
        .route(
            "/tickets/obtener_correos_respuesta",
            post(sync_responses_by_range),
        )
        .route("/users/sync", post(sync_users))
}

#[derive(Debug, Default, Deserialize)]
struct UserSyncRequest {
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    user_ids: Vec<String>,
}

async fn sync_users(
    State(state): State<AppState>,
    payload: Option<Json<UserSyncRequest>>,
) -> Response {
    let requested_ids = payload
        .map(|Json(body)| {
            let mut ids = body.user_ids;
            if let Some(single) = body.user_id {
                if !ids.contains(&single) {
                    ids.push(single);
                }
            }
            ids
        })
        .unwrap_or_default();

    let configured_ids = if requested_ids.is_empty() {
        load_configured_user_ids()
    } else {
        Vec::new()
    };

    let ids_to_fetch = if !requested_ids.is_empty() {
        requested_ids
    } else {
        configured_ids
    };

    let mysql_users = if ids_to_fetch.is_empty() {
        match fetch_all_mysql_users(&state).await {
            Ok(users) => users,
            Err(err) => {
                let body = Json(UserSyncResponse {
                    status: "error".to_string(),
                    detail: err.to_string(),
                    processed: 0,
                    inserted: 0,
                    skipped: 0,
                    errors: 0,
                    skipped_missing_reference: 0,
                    skipped_duplicate: 0,
                    missing_references: Vec::new(),
                });
                return (StatusCode::INTERNAL_SERVER_ERROR, body).into_response();
            }
        }
    } else {
        match fetch_mysql_users_by_ids(&state, &ids_to_fetch).await {
            Ok(users) => users,
            Err(err) => {
                let body = Json(UserSyncResponse {
                    status: "error".to_string(),
                    detail: err.to_string(),
                    processed: 0,
                    inserted: 0,
                    skipped: 0,
                    errors: 0,
                    skipped_missing_reference: 0,
                    skipped_duplicate: 0,
                    missing_references: Vec::new(),
                });
                return (StatusCode::INTERNAL_SERVER_ERROR, body).into_response();
            }
        }
    };

    if mysql_users.is_empty() {
        let body = Json(UserSyncResponse {
            status: "ok".to_string(),
            detail: "No se encontraron usuarios para migrar".to_string(),
            processed: 0,
            inserted: 0,
            skipped: 0,
            errors: 0,
            skipped_missing_reference: 0,
            skipped_duplicate: 0,
            missing_references: Vec::new(),
        });
        return (StatusCode::OK, body).into_response();
    }

    let db_name =
        env::var("MONGO_DB_NAME").unwrap_or_else(|_| "correos_exchange_queretaro".to_string());
    let users_db_name = env::var("MONGO_USERS_DB").unwrap_or_else(|_| db_name.clone());
    let collection_name =
        env::var("MONGO_USERS_COLLECTION").unwrap_or_else(|_| "crm-users".to_string());
    let settings_collection_name =
        env::var("MONGO_USER_SETTINGS_COLLECTION").unwrap_or_else(|_| "crm-user-settings".to_string());

    let collection = state
        .mongo
        .database(&users_db_name)
        .collection::<MongoUserDocument>(&collection_name);
    let collection_docs = state
        .mongo
        .database(&users_db_name)
        .collection::<Document>(&collection_name);
    let settings_collection = state
        .mongo
        .database(&users_db_name)
        .collection::<UserSettingsDocument>(&settings_collection_name);

    let (mysql_users, already_present, existing_sample) =
        match filter_pending_users(&collection_docs, mysql_users).await {
            Ok(result) => result,
            Err(err) => {
                let body = Json(UserSyncResponse {
                    status: "error".to_string(),
                    detail: err.to_string(),
                    processed: 0,
                    inserted: 0,
                    skipped: 0,
                    errors: 0,
                    skipped_missing_reference: 0,
                    skipped_duplicate: 0,
                    missing_references: Vec::new(),
                });
                return (StatusCode::INTERNAL_SERVER_ERROR, body).into_response();
            }
        };

    if already_present > 0 {
        warn!(
            "Usuarios - {} usuario(s) ya existen en MongoDB y serán omitidos",
            already_present
        );
        if !existing_sample.is_empty() {
            warn!(
                "Usuarios - Ejemplo de usuarios existentes: {}",
                existing_sample.join(", ")
            );
        }
    }

    let aliases = campaign_aliases();
    let refs = match load_branch_office_refs(&state, &users_db_name).await {
        Ok(refs) => refs,
        Err(err) => {
            let body = Json(UserSyncResponse {
                status: "error".to_string(),
                detail: err.to_string(),
                processed: 0,
                inserted: 0,
                skipped: 0,
                errors: 0,
                skipped_missing_reference: 0,
                skipped_duplicate: 0,
                missing_references: Vec::new(),
            });
            return (StatusCode::INTERNAL_SERVER_ERROR, body).into_response();
        }
    };

    let mut processed = 0usize;
    let mut inserted = 0usize;
    let mut skipped = 0usize;
    let mut errors = 0usize;
    let mut skipped_missing_reference = 0usize;
    let mut skipped_duplicate = already_present;
    let mut missing_references = Vec::new();

    let batch_size = user_batch_size();
    let total_batches = (mysql_users.len() + batch_size - 1) / batch_size;
    info!(
        "Usuarios - Se procesarán {} usuario(s) en {} lote(s) de hasta {} elementos",
        mysql_users.len(),
        total_batches,
        batch_size
    );

    for (index, batch) in mysql_users.chunks(batch_size).enumerate() {
        info!(
            "Usuarios - Iniciando lote {}/{} con {} usuario(s)",
            index + 1,
            total_batches,
            batch.len()
        );

        for raw_user in batch {
            processed += 1;
            let normalized = normalize_mysql_user(raw_user, &aliases);
            let Some(reference) = resolve_user_references(&normalized, &refs) else {
                warn!(
                    "Usuarios - No se encontró referencia para campaña '{}' y departamento '{}' (usuario {})",
                    normalized.campaign_name,
                    normalized.department_name,
                    normalized.username
                );
                skipped += 1;
                skipped_missing_reference += 1;
                if missing_references.len() < 10 {
                    missing_references.push(format!(
                        "{} | campana='{}' | departamento='{}' | key='{}|{}'",
                        normalized.username,
                        normalized.campaign_name,
                        normalized.department_name,
                        normalized.campaign_key,
                        normalized.department_key
                    ));
                }
                continue;
            };

            let mut settings_doc = build_user_settings_document();
            let settings_result = match settings_collection.insert_one(&settings_doc).await {
                Ok(result) => result,
                Err(err) => {
                    errors += 1;
                    error!(
                        "Usuarios - Fallo al insertar settings de {}: {}",
                        normalized.username, err
                    );
                    continue;
                }
            };

            let Some(settings_id) = settings_result.inserted_id.as_object_id() else {
                errors += 1;
                error!(
                    "Usuarios - No se obtuvo ObjectId para settings de {}",
                    normalized.username
                );
                continue;
            };
            settings_doc.id = Some(settings_id);
            info!(
                "Usuarios - Settings insertado en {}.{} con _id {}",
                users_db_name,
                settings_collection_name,
                settings_id.to_hex()
            );

            let mongo_user = build_mongo_user_document(&normalized, &reference, settings_doc);
            match upsert_mongo_user(&collection, &mongo_user).await {
                Ok(UpsertResult::Inserted) => inserted += 1,
                Ok(UpsertResult::Existing) => {
                    skipped += 1;
                    skipped_duplicate += 1;
                }
                Err(err) => {
                    errors += 1;
                    error!(
                        "Usuarios - Fallo al insertar usuario {}: {}",
                        normalized.username, err
                    );
                }
            }
        }
    }

    let status = if errors == 0 { "ok" } else { "partial" };
    let detail = format!(
        "Usuarios procesados: {} | insertados: {} | omitidos: {} | errores: {}",
        processed, inserted, skipped, errors
    );

    let body = Json(UserSyncResponse {
        status: status.to_string(),
        detail,
        processed,
        inserted,
        skipped,
        errors,
        skipped_missing_reference,
        skipped_duplicate,
        missing_references,
    });

    (StatusCode::OK, body).into_response()
}

#[derive(Debug, Clone)]
struct BranchOfficeRef {
    branch_office_id: ObjectId,
    client_id: String,
    campaign_id: String,
    department_id: String,
}

#[derive(Debug, Default)]
struct BranchOfficeRefs {
    by_campaign_department: HashMap<String, BranchOfficeRef>,
    by_campaign: HashMap<String, BranchOfficeRef>,
}

#[derive(Debug)]
enum UpsertResult {
    Inserted,
    Existing,
}

async fn filter_pending_users(
    collection: &mongodb::Collection<Document>,
    users: Vec<MysqlUser>,
) -> Result<(Vec<MysqlUser>, usize, Vec<String>), UserSyncError> {
    if users.is_empty() {
        return Ok((Vec::new(), 0, Vec::new()));
    }

    let mut existing = HashSet::new();
    let mut user_names = users
        .iter()
        .map(|user| user.usuario.clone())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();

    user_names.sort();
    user_names.dedup();

    const BATCH: usize = 500;
    for chunk in user_names.chunks(BATCH) {
        let cursor = collection
            .find(doc! { "cu_user": { "$in": chunk } })
            .await?;
        let docs = cursor.try_collect::<Vec<Document>>().await?;
        for doc in docs {
            if let Ok(value) = doc.get_str("cu_user") {
                existing.insert(value.to_string());
            }
        }
    }

    let already_present = existing.len();
    let mut existing_sample = existing.iter().cloned().collect::<Vec<_>>();
    existing_sample.sort();
    if existing_sample.len() > 20 {
        existing_sample.truncate(20);
    }
    let filtered = users
        .into_iter()
        .filter(|user| !existing.contains(&user.usuario))
        .collect::<Vec<_>>();

    Ok((filtered, already_present, existing_sample))
}

fn load_configured_user_ids() -> Vec<String> {
    env::var("USER_SYNC_IDS")
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn users_table_name() -> String {
    env::var("MYSQL_USERS_TABLE").unwrap_or_else(|_| "usuarios".to_string())
}

fn campaign_aliases() -> HashMap<String, String> {
    HashMap::new()
}

fn branch_office_filter_id() -> Option<ObjectId> {
    env::var("MONGO_BRANCH_OFFICE_ID")
        .ok()
        .and_then(|raw| ObjectId::parse_str(raw.trim()).ok())
}

fn normalize_mysql_user(raw: &MysqlUser, aliases: &HashMap<String, String>) -> NormalizedUser {
    let (first_names, last_names) = split_name(&raw.nombre);
    let campaign_name = raw.campana.trim().to_string();
    let department_name = raw.departamento.trim().to_string();

    NormalizedUser {
        username: raw.usuario.trim().to_string(),
        password: raw.password.clone(),
        first_names,
        last_names,
        campaign_name,
        department_name,
        campaign_key: normalize_campaign_name(&raw.campana, aliases),
        department_key: normalize_lookup_key(&raw.departamento),
        password_changed_at: raw.fecha_password,
        created_at: raw.fecha_creacion,
        last_connection: raw.fecha_conexion,
        status: raw.estatus,
    }
}

fn build_mongo_user_document(
    user: &NormalizedUser,
    reference: &BranchOfficeRef,
    settings: UserSettingsDocument,
) -> MongoUserDocument {
    let mut document = MongoUserDocument::default();
    document.user = user.username.clone();
    document.password = match hash_password_argon2id(&user.password) {
        Ok(value) => value,
        Err(err) => {
            warn!(
                "Usuarios - No se pudo hashear la contraseña de {}: {}",
                user.username, err
            );
            user.password.clone()
        }
    };
    document.first_names = user.first_names.clone();
    document.last_names = user.last_names.clone();
    document.branch_office_ids = vec![reference.branch_office_id.to_hex()];
    document.client_ids = vec![reference.client_id.clone()];
    document.campaign_ids = vec![reference.campaign_id.clone()];
    document.department_id = reference.department_id.clone();
    document.status = user.status;
    document.deleted = 0;
    document.date_registration = user.created_at.map(|value| {
        mongodb::bson::DateTime::from_millis(value.and_utc().timestamp_millis())
    });
    document.date_change_password = user.password_changed_at.map(|value| {
        mongodb::bson::DateTime::from_millis(value.and_utc().timestamp_millis())
    });
    document.date_last_connection = user.last_connection.map(|value| {
        mongodb::bson::DateTime::from_millis(value.and_utc().timestamp_millis())
    });
    document.date_migration = Some(now_monterrey_bson());
    document.color = "#000000".to_string();
    document.config_id = settings.id;
    document.settings = settings;
    document
}

fn build_user_settings_document() -> UserSettingsDocument {
    let mut settings = UserSettingsDocument::default();
    settings.cc_date_registration = Some(now_monterrey_bson());
    settings
}

fn now_monterrey_bson() -> mongodb::bson::DateTime {
    let now_monterrey = Monterrey.from_utc_datetime(&chrono::Utc::now().naive_utc());
    mongodb::bson::DateTime::from_millis(now_monterrey.timestamp_millis())
}

async fn upsert_mongo_user(
    collection: &mongodb::Collection<MongoUserDocument>,
    user: &MongoUserDocument,
) -> Result<UpsertResult, UserSyncError> {
    match collection.insert_one(user).await {
        Ok(_) => Ok(UpsertResult::Inserted),
        Err(err) => {
            if is_duplicate_key_error(&err) {
                Ok(UpsertResult::Existing)
            } else {
                Err(err.into())
            }
        }
    }
}

async fn fetch_mysql_users_by_ids(
    state: &AppState,
    user_ids: &[String],
) -> Result<Vec<MysqlUser>, UserSyncError> {
    if user_ids.is_empty() {
        return Ok(Vec::new());
    }

    let table = users_table_name();
    let placeholders = std::iter::repeat("?")
        .take(user_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT Usuario, Password, Nombre, Numero_Empleado, Turno, Departamento, Campana, Supervisor, Imagen_fondo, Fecha_password, Fecha_creacion, Fecha_Conexion, Estatus, Id_Confg, Actividad, Agendado_Ticket, Eliminacion_Casos, Resolver_casos, Creacion_Casos, Correo_salida, Graficas, Publicaciones_Favoritas, Extension FROM {table} WHERE Usuario IN ({placeholders})"
    );
    let values = user_ids
        .iter()
        .map(|value| Value::String(Some(value.clone())))
        .collect::<Vec<_>>();
    let statement = Statement::from_sql_and_values(DbBackend::MySql, sql, values);
    let rows = state.mysql.query_all_raw(statement).await?;

    Ok(rows
        .into_iter()
        .filter_map(build_mysql_user_from_row)
        .collect())
}

async fn fetch_all_mysql_users(state: &AppState) -> Result<Vec<MysqlUser>, UserSyncError> {
    let table = users_table_name();
    let sql = format!(
        "SELECT Usuario, Password, Nombre, Numero_Empleado, Turno, Departamento, Campana, Supervisor, Imagen_fondo, Fecha_password, Fecha_creacion, Fecha_Conexion, Estatus, Id_Confg, Actividad, Agendado_Ticket, Eliminacion_Casos, Resolver_casos, Creacion_Casos, Correo_salida, Graficas, Publicaciones_Favoritas, Extension FROM {table}"
    );
    let statement = Statement::from_string(DbBackend::MySql, sql);
    let rows = state.mysql.query_all_raw(statement).await?;

    Ok(rows
        .into_iter()
        .filter_map(build_mysql_user_from_row)
        .collect())
}

fn build_mysql_user_from_row(row: sea_orm::QueryResult) -> Option<MysqlUser> {
    let usuario = get_any_string(&row, &["Usuario", "usuario", "user", "User", "cu_user"])?;
    let password = get_any_string(&row, &["password", "Password", "contrasena", "Contraseña"])
        .unwrap_or_default();
    let nombre = get_any_string(&row, &["nombre", "Nombre"]).unwrap_or_default();
    let numero_empleado =
        get_any_string(&row, &["Numero_Empleado", "numero_empleado"]).unwrap_or_default();
    let turno = get_any_string(&row, &["Turno", "turno"]).unwrap_or_default();
    let campana = get_any_string(&row, &["campana", "Campana", "campaña", "Campaña"])
        .unwrap_or_default();
    let departamento =
        get_any_string(&row, &["departamento", "Departamento"]).unwrap_or_default();
    let supervisor = get_any_string(&row, &["Supervisor", "supervisor"]).unwrap_or_default();
    let imagen_fondo =
        get_any_string(&row, &["Imagen_fondo", "imagen_fondo"]).unwrap_or_default();
    let fecha_password = get_naive_datetime(
        &row,
        &["Fecha_password", "fecha_password", "FechaPassword"],
    );
    let fecha_creacion = get_naive_datetime(
        &row,
        &[
            "fecha_creacion",
            "Fecha_Creacion",
            "Fecha_creacion",
            "FechaCreacion",
        ],
    );
    let fecha_conexion = get_naive_datetime(
        &row,
        &[
            "fecha_conexion",
            "Fecha_Conexion",
            "Fecha_conexion",
            "FechaConexion",
        ],
    );
    let estatus = get_i32(&row, &["estatus", "Estatus"]).unwrap_or(0);
    let id_confg = get_i32(&row, &["Id_Confg", "id_confg"]).unwrap_or(0);
    let actividad = get_i32(&row, &["Actividad", "actividad"]).unwrap_or(0);
    let agendado_ticket =
        get_i32(&row, &["Agendado_Ticket", "agendado_ticket"]);
    let eliminacion_casos =
        get_i32(&row, &["Eliminacion_Casos", "eliminacion_casos"]);
    let resolver_casos = get_i32(&row, &["Resolver_casos", "resolver_casos"]);
    let creacion_casos = get_i32(&row, &["Creacion_Casos", "creacion_casos"]);
    let correo_salida = get_i32(&row, &["Correo_salida", "correo_salida"]);
    let graficas = get_i32(&row, &["Graficas", "graficas"]);
    let publicaciones_favoritas =
        get_any_string(&row, &["Publicaciones_Favoritas", "publicaciones_favoritas"]);
    let extension = get_any_string(&row, &["Extension", "extension"]).unwrap_or_default();

    Some(MysqlUser {
        usuario,
        password,
        nombre,
        numero_empleado,
        turno,
        campana,
        departamento,
        supervisor,
        imagen_fondo,
        fecha_password,
        fecha_creacion,
        fecha_conexion,
        estatus,
        id_confg,
        actividad,
        agendado_ticket,
        eliminacion_casos,
        resolver_casos,
        creacion_casos,
        correo_salida,
        graficas,
        publicaciones_favoritas,
        extension,
    })
}

fn resolve_user_references(
    user: &NormalizedUser,
    refs: &BranchOfficeRefs,
) -> Option<BranchOfficeRef> {
    let key = format!("{}|{}", user.campaign_key, user.department_key);
    refs.by_campaign_department
        .get(&key)
        .cloned()
        .or_else(|| refs.by_campaign.get(&user.campaign_key).cloned())
}

async fn load_branch_office_refs(
    state: &AppState,
    db_name: &str,
) -> Result<BranchOfficeRefs, UserSyncError> {
    let branch_db_name =
        env::var("MONGO_BRANCH_OFFICE_DB").unwrap_or_else(|_| db_name.to_string());
    let collection_name =
        env::var("MONGO_BRANCH_OFFICE_COLLECTION").unwrap_or_else(|_| "crm-branch-office".into());
    let filter = branch_office_filter_id()
        .map(|id| doc! { "_id": id })
        .unwrap_or_else(|| doc! {});
    let collection = state
        .mongo
        .database(&branch_db_name)
        .collection::<Document>(&collection_name);
    let documents = collection
        .find(filter)
        .await?
        .try_collect::<Vec<Document>>()
        .await?;

    let mut refs = BranchOfficeRefs::default();

    for doc in documents {
        let Some(branch_office_id) = doc.get_object_id("_id").ok() else {
            continue;
        };
        let clients = get_doc_array(&doc, &["cbo_list_clients", "clients", "client", "clientes"]);
        for client in clients {
            let client_id =
                get_doc_string(&client, &["id", "id_client", "client_id", "cu_client"])
                    .unwrap_or_default();
            let campaigns = get_doc_array(
                &client,
                &["list_campaigns", "campaigns", "campanas", "campañas"],
            );
            for campaign in campaigns {
                let campaign_id = get_doc_string(
                    &campaign,
                    &["id", "id_campaign", "campaign_id", "cu_campaign"],
                )
                .unwrap_or_default();
                let campaign_name =
                    get_doc_string(&campaign, &["campaign", "campaign_name", "nombre", "name"])
                .unwrap_or_default();
                if campaign_name.is_empty() {
                    continue;
                }

        let campaign_key = normalize_lookup_key(&campaign_name);
                let departments = get_doc_array(
                    &campaign,
                    &["list_departments", "departments", "departamentos"],
                );
                if departments.is_empty() {
                    let reference = BranchOfficeRef {
                        branch_office_id,
                        client_id: client_id.clone(),
                        campaign_id: campaign_id.clone(),
                        department_id: String::new(),
                    };
                    refs.by_campaign.entry(campaign_key).or_insert(reference);
                    continue;
                }

                for department in departments {
                    let department_id = get_doc_string(
                        &department,
                        &[
                            "id",
                            "id_department",
                            "department_id",
                            "cu_department",
                            "id_departamento",
                        ],
                    )
                    .unwrap_or_default();
                    let department_name =
                        get_doc_string(&department, &["department", "department_name", "name"])
                    .unwrap_or_default();
                    if department_name.is_empty() {
                        continue;
                    }

                    let department_key = normalize_lookup_key(&department_name);
                    let reference = BranchOfficeRef {
                        branch_office_id,
                        client_id: client_id.clone(),
                        campaign_id: campaign_id.clone(),
                        department_id: department_id.clone(),
                    };
                    refs.by_campaign_department
                        .entry(format!("{campaign_key}|{department_key}"))
                        .or_insert_with(|| reference.clone());
                    refs.by_campaign
                        .entry(campaign_key.clone())
                        .or_insert(reference);
                }
            }
        }
    }

    Ok(refs)
}

fn get_doc_array(doc: &Document, keys: &[&str]) -> Vec<Document> {
    for key in keys {
        if let Some(Bson::Array(values)) = doc.get(*key) {
            return values
                .iter()
                .filter_map(|value| value.as_document().cloned())
                .collect();
        }
    }
    Vec::new()
}

fn get_doc_string(doc: &Document, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(value) = doc.get(*key) {
            if let Some(value) = bson_to_string(value) {
                let trimmed = value.trim().to_string();
                if !trimmed.is_empty() {
                    return Some(trimmed);
                }
            }
        }
    }
    None
}

fn bson_to_string(value: &Bson) -> Option<String> {
    match value {
        Bson::String(value) => Some(value.clone()),
        Bson::ObjectId(value) => Some(value.to_hex()),
        Bson::Int32(value) => Some(value.to_string()),
        Bson::Int64(value) => Some(value.to_string()),
        Bson::Double(value) => Some(value.to_string()),
        _ => None,
    }
}

fn is_duplicate_key_error(err: &mongodb::error::Error) -> bool {
    let message = err.to_string();
    message.contains("E11000") || message.contains("duplicate key")
}

fn get_i64(row: &sea_orm::QueryResult, columns: &[&str]) -> Option<i64> {
    columns.iter().find_map(|column| {
        row.try_get::<i64>("", column)
            .ok()
            .or_else(|| row.try_get::<i32>("", column).ok().map(i64::from))
            .or_else(|| {
                row.try_get::<String>("", column)
                    .ok()
                    .and_then(|value| value.trim().parse::<i64>().ok())
            })
    })
}

fn get_i32(row: &sea_orm::QueryResult, columns: &[&str]) -> Option<i32> {
    columns.iter().find_map(|column| {
        row.try_get::<i32>("", column)
            .ok()
            .or_else(|| row.try_get::<i64>("", column).ok().map(|value| value as i32))
            .or_else(|| {
                row.try_get::<String>("", column)
                    .ok()
                    .and_then(|value| value.trim().parse::<i32>().ok())
            })
    })
}

fn get_naive_datetime(
    row: &sea_orm::QueryResult,
    columns: &[&str],
) -> Option<NaiveDateTime> {
    const FORMATS: [&str; 3] = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d %I:%M:%S%p"];
    columns.iter().find_map(|column| {
        row.try_get::<NaiveDateTime>("", column)
            .ok()
            .or_else(|| {
                row.try_get::<String>("", column).ok().and_then(|raw| {
                    let trimmed = raw.trim();
                    FORMATS
                        .iter()
                        .find_map(|fmt| NaiveDateTime::parse_from_str(trimmed, fmt).ok())
                })
            })
    })
}

#[derive(Debug, Default, Deserialize)]
struct DateRangeQuery {
    #[serde(default)]
    fecha_inicio: Option<String>,
    #[serde(default)]
    fecha_fin: Option<String>,
    #[serde(default)]
    num_caso: Option<String>,
}

impl DateRangeQuery {
    fn resolved_bounds(&self) -> Option<DateBounds> {
        let start = self
            .fecha_inicio
            .as_ref()
            .and_then(|value| parse_date_only(value))?;
        let end = self
            .fecha_fin
            .as_ref()
            .and_then(|value| parse_date_only(value))
            .unwrap_or_else(current_date);
        Some(DateBounds { start, end })
    }
}

#[derive(Clone)]
struct DateBounds {
    start: NaiveDate,
    end: NaiveDate,
}

fn current_date() -> NaiveDate {
    chrono::Local::now().date_naive()
}

fn parse_date_only(value: &str) -> Option<NaiveDate> {
    let part = value
        .split(|c| c == ' ' || c == 'T')
        .next()
        .unwrap_or(value)
        .trim();
    NaiveDate::parse_from_str(part, "%Y-%m-%d").ok()
}

async fn sync_tickets_by_range(
    State(state): State<AppState>,
    Query(range): Query<DateRangeQuery>,
) -> Response {
    let Some(bounds) = range.resolved_bounds() else {
        let body = Json(TicketBatchResponse {
            status: "error".to_string(),
            detail: "Debes proporcionar al menos fecha_inicio".to_string(),
            processed: 0,
            successes: Vec::new(),
            failures: Vec::new(),
        });
        return (StatusCode::BAD_REQUEST, body).into_response();
    };

    let total_cases;
    let cases = match fetch_case_ids_by_range(&state, &bounds).await {
        Ok(ids) => ids,
        Err(err) => {
            let status = err.status_code();
            let body = Json(TicketBatchResponse {
                status: "error".to_string(),
                detail: err.to_string(),
                processed: 0,
                successes: Vec::new(),
                failures: Vec::new(),
            });
            return (status, body).into_response();
        }
    };

    total_cases = cases.len();
    if total_cases == 0 {
        let body = Json(TicketBatchResponse {
            status: "ok".to_string(),
            detail: "No se encontraron casos en el rango solicitado".to_string(),
            processed: 0,
            successes: Vec::new(),
            failures: Vec::new(),
        });
        return (StatusCode::OK, body).into_response();
    }

    let db_name = env::var("MONGO_DB_NAME").unwrap_or_else(|_| "correos_exchange_queretaro".into());
    let (cases, already_present) = match filter_pending_cases(&state, &db_name, cases).await {
        Ok(result) => result,
        Err(err) => {
            let status = err.status_code();
            let body = Json(TicketBatchResponse {
                status: "error".to_string(),
                detail: err.to_string(),
                processed: 0,
                successes: Vec::new(),
                failures: Vec::new(),
            });
            return (status, body).into_response();
        }
    };

    info!(
        "Casos totales en rango: {} | Ya en MongoDB: {} | Pendientes: {}",
        total_cases,
        already_present,
        cases.len()
    );

    if cases.is_empty() {
        let body = Json(TicketBatchResponse {
            status: "ok".to_string(),
            detail: format!(
                "Los {} casos encontrados ya estaban sincronizados en MongoDB",
                total_cases
            ),
            processed: 0,
            successes: Vec::new(),
            failures: Vec::new(),
        });
        return (StatusCode::OK, body).into_response();
    }

    let batch_size = case_batch_size();
    let total_batches = (cases.len() + batch_size - 1) / batch_size;
    info!(
        "Se procesarán {} casos en {} lote(s) de hasta {} elementos",
        cases.len(),
        total_batches,
        batch_size
    );

    let mut successes = Vec::new();
    let mut failures = Vec::new();
    let mut iter = cases.into_iter();
    let mut current_batch = 0usize;

    loop {
        let batch: Vec<MysqlEmailRecord> = iter.by_ref().take(batch_size).collect();
        if batch.is_empty() {
            break;
        }

        current_batch += 1;
        info!(
            "Iniciando lote {}/{} con {} caso(s)",
            current_batch,
            total_batches,
            batch.len()
        );

        let case_parallel = case_parallelism();
        let results = stream::iter(batch.into_iter().map(|record| {
            let state = state.clone();
            async move {
                let case_id = record.case_id.clone();
                match sync_ticket_case(&state, record).await {
                    Ok(_) => Ok(case_id),
                    Err(err) => Err((case_id, err)),
                }
            }
        }))
        .buffer_unordered(case_parallel)
        .collect::<Vec<_>>()
        .await;

        for result in results {
            match result {
                Ok(case_id) => successes.push(case_id),
                Err((case_id, err)) => {
                    error!("Fallo al sincronizar ticket {}: {}", case_id, err);
                    failures.push(format!("{}: {}", case_id, err));
                }
            }
        }

        info!("Lote {}/{} finalizado", current_batch, total_batches);
    }

    let processed = successes.len() + failures.len();
    let status = if failures.is_empty() { "ok" } else { "partial" };
    let detail = if failures.is_empty() {
        format!("Se sincronizaron {} casos", processed)
    } else {
        format!(
            "Se sincronizaron {} casos, {} fallaron",
            successes.len(),
            failures.len()
        )
    };

    let body = Json(TicketBatchResponse {
        status: status.to_string(),
        detail,
        processed,
        successes,
        failures,
    });
    (StatusCode::OK, body).into_response()
}

async fn sync_notes_by_range(
    State(state): State<AppState>,
    Query(range): Query<DateRangeQuery>,
) -> Response {
    let case_override = range
        .num_caso
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string());

    let (bounds, case_ids) = if let Some(case_id) = case_override.clone() {
        info!("Notas - Modo num_caso activo: {}", case_id);
        (None, vec![case_id])
    } else {
        let Some(bounds) = range.resolved_bounds() else {
            let body = Json(MessageSyncResponse {
                status: "error".to_string(),
                detail: "Debes proporcionar al menos fecha_inicio o num_caso".to_string(),
                inserted: 0,
                mime_ids: Vec::new(),
                msg_struct_ids: Vec::new(),
            });
            return (StatusCode::BAD_REQUEST, body).into_response();
        };

        let case_ids = match fetch_note_case_ids(&state, &bounds).await {
            Ok(ids) => ids,
            Err(err) => {
                let status = err.status_code();
                let body = Json(MessageSyncResponse {
                    status: "error".to_string(),
                    detail: err.to_string(),
                    inserted: 0,
                    mime_ids: Vec::new(),
                    msg_struct_ids: Vec::new(),
                });
                return (status, body).into_response();
            }
        };

        info!(
            "Notas - Modo rango activo: {} a {} (casos {})",
            bounds.start,
            bounds.end,
            case_ids.len()
        );
        (Some(bounds), case_ids)
    };

    info!(
        "Notas - Casos antes de filtrar en MongoDB: {}",
        case_ids.len()
    );

    if case_ids.is_empty() {
        let detail = if let Some(case_id) = case_override {
            format!("No se encontraron notas para el caso {}", case_id)
        } else {
            "No se encontraron notas en el rango solicitado".to_string()
        };
        let body = Json(MessageSyncResponse {
            status: "ok".to_string(),
            detail,
            inserted: 0,
            mime_ids: Vec::new(),
            msg_struct_ids: Vec::new(),
        });
        return (StatusCode::OK, body).into_response();
    }

    let db_name = env::var("MONGO_DB_NAME").unwrap_or_else(|_| "correos_exchange_queretaro".into());
    let (case_ids, already_present) =
        match filter_pending_case_ids_by_type(&state, &db_name, case_ids, "nota").await {
            Ok(result) => result,
            Err(err) => {
                let status = err.status_code();
                let body = Json(MessageSyncResponse {
                    status: "error".to_string(),
                    detail: err.to_string(),
                    inserted: 0,
                    mime_ids: Vec::new(),
                    msg_struct_ids: Vec::new(),
                });
                return (status, body).into_response();
            }
        };

    info!(
        "Notas - Casos totales: {} | Ya en MongoDB: {} | Pendientes: {}",
        case_ids.len() + already_present,
        already_present,
        case_ids.len()
    );

    if case_ids.is_empty() {
        let body = Json(MessageSyncResponse {
            status: "ok".to_string(),
            detail: "Todos los casos de notas ya existen en MongoDB".to_string(),
            inserted: 0,
            mime_ids: Vec::new(),
            msg_struct_ids: Vec::new(),
        });
        return (StatusCode::OK, body).into_response();
    }

    let batch_size = case_batch_size();
    let total_batches = (case_ids.len() + batch_size - 1) / batch_size;
    info!(
        "Notas - Se procesarán {} casos en {} lote(s) de hasta {} elementos",
        case_ids.len(),
        total_batches,
        batch_size
    );

    let mut total_inserted = 0;
    let mut mime_ids = Vec::new();
    let mut msg_struct_ids = Vec::new();
    let mut failures = Vec::new();
    let mut iter = case_ids.into_iter();
    let mut current_batch = 0usize;

    loop {
        let batch: Vec<String> = iter.by_ref().take(batch_size).collect();
        if batch.is_empty() {
            break;
        }

        current_batch += 1;
        info!(
            "Notas - Iniciando lote {}/{} con {} caso(s)",
            current_batch,
            total_batches,
            batch.len()
        );

        let case_parallel = case_parallelism();
        let results = stream::iter(batch.into_iter().map(|case_id| {
            let state = state.clone();
            let bounds = bounds.as_ref();
            async move {
                let result = sync_notes_for_case(&state, &case_id, bounds).await;
                (case_id, result)
            }
        }))
        .buffer_unordered(case_parallel)
        .collect::<Vec<_>>()
        .await;

        for (case_id, result) in results {
            match result {
                Ok(summary) => {
                    total_inserted += summary.inserted;
                    mime_ids.extend(summary.mime_ids);
                    msg_struct_ids.extend(summary.msg_struct_ids);
                }
                Err(err) => {
                    error!("Fallo al sincronizar notas del ticket {}: {}", case_id, err);
                    failures.push(format!("{}: {}", case_id, err));
                }
            }
        }

        info!(
            "Notas - Lote {}/{} finalizado",
            current_batch, total_batches
        );
    }

    let status = if failures.is_empty() { "ok" } else { "partial" };
    let detail = if failures.is_empty() {
        format!("Se migraron {} notas", total_inserted)
    } else {
        format!(
            "Se migraron {} notas; {} casos fallaron",
            total_inserted,
            failures.len()
        )
    };

    let body = Json(MessageSyncResponse {
        status: status.to_string(),
        detail,
        inserted: total_inserted,
        mime_ids,
        msg_struct_ids,
    });
    (StatusCode::OK, body).into_response()
}

async fn sync_responses_by_range(
    State(state): State<AppState>,
    Query(range): Query<DateRangeQuery>,
) -> Response {
    let case_override = range
        .num_caso
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string());

    let (bounds, case_ids) = if let Some(case_id) = case_override.clone() {
        info!("Respuestas - Modo num_caso activo: {}", case_id);
        (None, vec![case_id])
    } else {
        let Some(bounds) = range.resolved_bounds() else {
            let body = Json(MessageSyncResponse {
                status: "error".to_string(),
                detail: "Debes proporcionar al menos fecha_inicio o num_caso".to_string(),
                inserted: 0,
                mime_ids: Vec::new(),
                msg_struct_ids: Vec::new(),
            });
            return (StatusCode::BAD_REQUEST, body).into_response();
        };

        let case_ids = match fetch_response_case_ids(&state, &bounds).await {
            Ok(ids) => ids,
            Err(err) => {
                let status = err.status_code();
                let body = Json(MessageSyncResponse {
                    status: "error".to_string(),
                    detail: err.to_string(),
                    inserted: 0,
                    mime_ids: Vec::new(),
                    msg_struct_ids: Vec::new(),
                });
                return (status, body).into_response();
            }
        };

        info!(
            "Respuestas - Modo rango activo: {} a {} (casos {})",
            bounds.start,
            bounds.end,
            case_ids.len()
        );
        (Some(bounds), case_ids)
    };

    info!(
        "Respuestas - Casos antes de filtrar en MongoDB: {}",
        case_ids.len()
    );

    if case_ids.is_empty() {
        let detail = if let Some(case_id) = case_override {
            format!("No se encontraron respuestas para el caso {}", case_id)
        } else {
            "No se encontraron respuestas en el rango solicitado".to_string()
        };
        let body = Json(MessageSyncResponse {
            status: "ok".to_string(),
            detail,
            inserted: 0,
            mime_ids: Vec::new(),
            msg_struct_ids: Vec::new(),
        });
        return (StatusCode::OK, body).into_response();
    }

    let db_name = env::var("MONGO_DB_NAME").unwrap_or_else(|_| "correos_exchange_queretaro".into());
    let (case_ids, already_present) =
        match filter_pending_case_ids_by_type(&state, &db_name, case_ids, "respuesta").await {
            Ok(result) => result,
            Err(err) => {
                let status = err.status_code();
                let body = Json(MessageSyncResponse {
                    status: "error".to_string(),
                    detail: err.to_string(),
                    inserted: 0,
                    mime_ids: Vec::new(),
                    msg_struct_ids: Vec::new(),
                });
                return (status, body).into_response();
            }
        };

    info!(
        "Respuestas - Casos totales: {} | Ya en MongoDB: {} | Pendientes: {}",
        case_ids.len() + already_present,
        already_present,
        case_ids.len()
    );

    if case_ids.is_empty() {
        let body = Json(MessageSyncResponse {
            status: "ok".to_string(),
            detail: "Todos los casos de respuestas ya existen en MongoDB".to_string(),
            inserted: 0,
            mime_ids: Vec::new(),
            msg_struct_ids: Vec::new(),
        });
        return (StatusCode::OK, body).into_response();
    }

    let batch_size = case_batch_size();
    let total_batches = (case_ids.len() + batch_size - 1) / batch_size;
    info!(
        "Respuestas - Se procesarán {} casos en {} lote(s) de hasta {} elementos",
        case_ids.len(),
        total_batches,
        batch_size
    );

    let mut total_inserted = 0;
    let mut mime_ids = Vec::new();
    let mut msg_struct_ids = Vec::new();
    let mut failures = Vec::new();
    let mut iter = case_ids.into_iter();
    let mut current_batch = 0usize;

    loop {
        let batch: Vec<String> = iter.by_ref().take(batch_size).collect();
        if batch.is_empty() {
            break;
        }

        current_batch += 1;
        info!(
            "Respuestas - Iniciando lote {}/{} con {} caso(s)",
            current_batch,
            total_batches,
            batch.len()
        );

        let case_parallel = case_parallelism();
        let results = stream::iter(batch.into_iter().map(|case_id| {
            let state = state.clone();
            let bounds = bounds.as_ref();
            async move {
                let result = sync_responses_for_case(&state, &case_id, bounds).await;
                (case_id, result)
            }
        }))
        .buffer_unordered(case_parallel)
        .collect::<Vec<_>>()
        .await;

        for (case_id, result) in results {
            match result {
                Ok(summary) => {
                    total_inserted += summary.inserted;
                    mime_ids.extend(summary.mime_ids);
                    msg_struct_ids.extend(summary.msg_struct_ids);
                }
                Err(err) => {
                    error!(
                        "Fallo al sincronizar respuestas del ticket {}: {}",
                        case_id, err
                    );
                    failures.push(format!("{}: {}", case_id, err));
                }
            }
        }

        info!(
            "Respuestas - Lote {}/{} finalizado",
            current_batch, total_batches
        );
    }

    let status = if failures.is_empty() { "ok" } else { "partial" };
    let detail = if failures.is_empty() {
        format!("Se migraron {} respuestas", total_inserted)
    } else {
        format!(
            "Se migraron {} respuestas; {} casos fallaron",
            total_inserted,
            failures.len()
        )
    };

    let body = Json(MessageSyncResponse {
        status: status.to_string(),
        detail,
        inserted: total_inserted,
        mime_ids,
        msg_struct_ids,
    });
    (StatusCode::OK, body).into_response()
}

async fn sync_ticket_case(
    state: &AppState,
    mysql_record: MysqlEmailRecord,
) -> Result<TicketSyncResponse, TicketSyncError> {
    let case_id = mysql_record.case_id.clone();
    let db_name = env::var("MONGO_DB_NAME").unwrap_or_else(|_| "correos_exchange_queretaro".into());
    let config_collection_name =
        env::var("MONGO_CONFIG_COLLECTION").unwrap_or_else(|_| "configuration".into());

    if case_already_synced(state, &db_name, &case_id).await? {
        return Ok(TicketSyncResponse {
            status: "skipped".to_string(),
            detail: format!("El caso {case_id} ya existe en MongoDB"),
            uploaded: Vec::new(),
            mongo_id: None,
            msg_struct_id: None,
        });
    }

    let config_email = mysql_record
        .config_email
        .clone()
        .ok_or_else(|| TicketSyncError::MissingConfigurationEmail(case_id.to_string()))?;
    let configuration_id =
        resolve_configuration_id(state, &db_name, &config_collection_name, &config_email).await?;

    let sanitized_case = sanitize_segment(&case_id);
    let remote_case_path = format!("{} {}", state.ssh_service.base_path(), case_id);

    let mut tasks = Vec::new();
    gather_case_files(
        state,
        &remote_case_path,
        &format!("tickets/{}", sanitized_case),
        &mut tasks,
    )
    .await?;

    if tasks.is_empty() {
        return Err(TicketSyncError::CaseNotFound(case_id.to_string()));
    }

    let (uploaded_urls, mime_files, html_body) = process_case_file_tasks(state, tasks).await?;

    if mime_files.is_empty() {
        return Err(TicketSyncError::CaseNotFound(case_id.to_string()));
    }

    let mime_collection = state
        .mongo
        .database(&db_name)
        .collection::<MsgMimeDocument>("msg-mime");

    let html_content = html_body
        .filter(|content| !content.is_empty())
        .unwrap_or_else(|| mysql_record.subject.clone().unwrap_or_default());

    let message_type = "entrada".to_string();

    let mime_document = MsgMimeDocument {
        id: ObjectId::new(),
        configuration_id,
        message_type: message_type.clone(),
        text: mysql_record.subject.clone().unwrap_or_default(),
        file_mime: "text/html".to_string(),
        html: html_content,
        is_file_local: false,
        files: mime_files,
    };

    let inserted_id = mime_document.id;
    mime_collection.insert_one(mime_document.clone()).await?;

    let struct_collection = state
        .mongo
        .database(&db_name)
        .collection::<MsgStructDocument>("msg-struct");

    let msg_struct_doc = MsgStructDocument {
        id: ObjectId::new(),
        id_mail: mysql_record.id_mail.unwrap_or_default(),
        mime_id: inserted_id,
        configuration_id,
        date_creation: parse_mysql_datetime(mysql_record.fecha_de_registro.as_deref()),
        date_buzon: parse_mysql_datetime(mysql_record.fecha_buzon.as_deref()),
        from: mysql_record.from_email.as_ref().map(|email| MsgContact {
            name: mysql_record.from_name.clone(),
            email: Some(email.clone()),
        }),
        to: mysql_record
            .to_emails
            .iter()
            .map(|email| MsgContact {
                name: None,
                email: Some(email.clone()),
            })
            .collect(),
        cc: mysql_record
            .cc_emails
            .iter()
            .map(|email| MsgContact {
                name: None,
                email: Some(email.clone()),
            })
            .collect(),
        message_type,
        subject: mysql_record.subject.clone(),
        conversation: Some(case_id.to_string()),
        num_caso: Some(case_id.to_string()),
        fechas_estatus: parse_mysql_datetime(mysql_record.fechas_estatus.as_deref()),
        nombre_cliente: mysql_record.nombre_cliente.clone(),
        estatus: mysql_record.estatus.clone(),
        agente_asignado: mysql_record.agente_asignado.clone(),
        categoria: mysql_record.categoria.clone(),
        subcategoria: mysql_record.subcategoria.clone(),
        fecha_cerrada: parse_mysql_datetime(mysql_record.fecha_cerrada.as_deref()),
        fecha_cliente: parse_mysql_datetime(mysql_record.fecha_cliente.as_deref()),
        numero_lineas: mysql_record.numero_lineas.clone(),
        lista_caso: mysql_record.lista_caso.clone(),
    };

    struct_collection.insert_one(msg_struct_doc.clone()).await?;

    info!(
        "Ticket {} sincronizado. Archivos subidos: {}",
        case_id,
        uploaded_urls.len()
    );

    Ok(TicketSyncResponse {
        status: "ok".to_string(),
        detail: format!(
            "Sincronización completada con {} archivos",
            uploaded_urls.len()
        ),
        uploaded: uploaded_urls,
        mongo_id: Some(inserted_id.to_hex()),
        msg_struct_id: Some(msg_struct_doc.id.to_hex()),
    })
}

async fn fetch_case_ids_by_range(
    state: &AppState,
    bounds: &DateBounds,
) -> Result<Vec<MysqlEmailRecord>, TicketSyncError> {
    fetch_case_records_by_range(state, bounds).await
}

async fn filter_pending_cases(
    state: &AppState,
    db_name: &str,
    cases: Vec<MysqlEmailRecord>,
) -> Result<(Vec<MysqlEmailRecord>, usize), TicketSyncError> {
    if cases.is_empty() {
        return Ok((Vec::new(), 0));
    }

    let collection = state
        .mongo
        .database(db_name)
        .collection::<MsgStructDocument>("msg-struct");

    let ids: Vec<String> = cases.iter().map(|case| case.case_id.clone()).collect();
    let ids_bson: Vec<Bson> = ids.iter().map(|id| Bson::String(id.clone())).collect();
    let filter = doc! { "num_caso": { "$in": ids_bson } };

    let existing = collection.distinct("num_caso", filter).await?;
    let existing_set: HashSet<String> = existing.into_iter().filter_map(bson_to_case_id).collect();

    let pending: Vec<MysqlEmailRecord> = cases
        .into_iter()
        .filter(|case| !existing_set.contains(&case.case_id))
        .collect();
    let skipped = ids.len().saturating_sub(pending.len());

    Ok((pending, skipped))
}

async fn filter_pending_case_ids_by_type(
    state: &AppState,
    db_name: &str,
    case_ids: Vec<String>,
    message_type: &str,
) -> Result<(Vec<String>, usize), TicketSyncError> {
    if case_ids.is_empty() {
        return Ok((Vec::new(), 0));
    }

    let collection = state
        .mongo
        .database(db_name)
        .collection::<MsgStructDocument>("msg-struct");

    let ids_bson: Vec<Bson> = case_ids.iter().map(|id| Bson::String(id.clone())).collect();
    let filter = doc! { "num_caso": { "$in": ids_bson }, "message_type": message_type };

    let existing = collection.distinct("num_caso", filter).await?;
    let existing_set: HashSet<String> = existing.into_iter().filter_map(bson_to_case_id).collect();

    let pending: Vec<String> = case_ids
        .into_iter()
        .filter(|case_id| !existing_set.contains(case_id))
        .collect();
    let skipped = existing_set.len();

    Ok((pending, skipped))
}

async fn fetch_note_case_ids(
    state: &AppState,
    bounds: &DateBounds,
) -> Result<Vec<String>, TicketSyncError> {
    fetch_distinct_case_ids(state, "correos_notas", "Fecha_de_Registro", bounds).await
}

async fn fetch_response_case_ids(
    state: &AppState,
    bounds: &DateBounds,
) -> Result<Vec<String>, TicketSyncError> {
    fetch_distinct_case_ids(state, "correos_respuesta", "Fecha_de_Registro", bounds).await
}

async fn sync_notes_for_case(
    state: &AppState,
    case_id: &str,
    bounds: Option<&DateBounds>,
) -> Result<MessageSyncResponse, TicketSyncError> {
    let mysql_record = fetch_mysql_record(state, case_id).await?;
    let db_name = env::var("MONGO_DB_NAME").unwrap_or_else(|_| "correos_exchange_queretaro".into());
    let config_collection_name =
        env::var("MONGO_CONFIG_COLLECTION").unwrap_or_else(|_| "configuration".into());

    let config_email = mysql_record
        .config_email
        .clone()
        .ok_or_else(|| TicketSyncError::MissingConfigurationEmail(case_id.to_string()))?;
    let configuration_id =
        resolve_configuration_id(state, &db_name, &config_collection_name, &config_email).await?;

    let db = state.mongo.database(&db_name);
    let mime_collection = db.collection::<MsgMimeDocument>("msg-mime");
    let struct_collection = db.collection::<MsgStructDocument>("msg-struct");

    let notes = fetch_case_notes(state, case_id, bounds).await?;
    info!(
        "Notas caso {}: {} registro(s) encontrados en MySQL{}",
        case_id,
        notes.len(),
        bounds
            .map(|value| format!(" (rango {} -> {})", value.start, value.end))
            .unwrap_or_else(|| " (sin rango)".to_string())
    );
    if notes.is_empty() {
        return Ok(MessageSyncResponse {
            status: "ok".to_string(),
            detail: format!("No se encontraron notas para el caso {}", case_id),
            inserted: 0,
            mime_ids: Vec::new(),
            msg_struct_ids: Vec::new(),
        });
    }
    let sanitized_case = sanitize_segment(case_id);
    let base_prefix = format!("tickets/{}", sanitized_case);

    let mut inserted_mime_ids = Vec::new();
    let mut inserted_struct_ids = Vec::new();
    for note in notes {
        if struct_collection
            .find_one(doc! { "id_mail": note.id, "message_type": "nota", "num_caso": case_id })
            .await?
            .is_some()
        {
            info!(
                "Nota {} del caso {} ya existe en MongoDB, se omite",
                note.id, case_id
            );
            continue;
        }

        let folder = sanitize_segment(&format!("Nota_{}", note.id));
        let prefix = format!("{}/notas/{}", base_prefix, folder);
        let (files, html_body) = build_files_from_s3(state, &prefix).await?;
        let html_content = html_body.unwrap_or_else(|| format!("Nota {}", note.id));
        let summary = note.usuario.clone().unwrap_or_default();

        let mime_document = MsgMimeDocument {
            id: ObjectId::new(),
            configuration_id,
            message_type: "nota".to_string(),
            text: summary.clone(),
            file_mime: "text/html".to_string(),
            html: html_content,
            is_file_local: false,
            files,
        };
        let mime_id = mime_document.id;
        mime_collection.insert_one(mime_document.clone()).await?;

        let msg_struct_doc = MsgStructDocument {
            id: ObjectId::new(),
            id_mail: note.id,
            mime_id,
            configuration_id,
            date_creation: parse_mysql_datetime(note.fecha_registro.as_deref()),
            date_buzon: None,
            from: None,
            to: Vec::new(),
            cc: Vec::new(),
            message_type: "nota".to_string(),
            subject: if summary.is_empty() {
                None
            } else {
                Some(summary.clone())
            },
            conversation: Some(case_id.to_string()),
            num_caso: Some(case_id.to_string()),
            fechas_estatus: None,
            nombre_cliente: None,
            estatus: None,
            agente_asignado: None,
            categoria: None,
            subcategoria: None,
            fecha_cerrada: None,
            fecha_cliente: None,
            numero_lineas: None,
            lista_caso: None,
        };

        struct_collection.insert_one(msg_struct_doc.clone()).await?;

        inserted_mime_ids.push(mime_id.to_hex());
        inserted_struct_ids.push(msg_struct_doc.id.to_hex());
    }

    let inserted = inserted_struct_ids.len();
    info!(
        "Notas caso {}: total {} | insertadas {}",
        case_id, inserted, inserted
    );
    Ok(MessageSyncResponse {
        status: "ok".to_string(),
        detail: format!("Se migraron {} notas para el caso {}", inserted, case_id),
        inserted,
        mime_ids: inserted_mime_ids,
        msg_struct_ids: inserted_struct_ids,
    })
}

async fn sync_responses_for_case(
    state: &AppState,
    case_id: &str,
    bounds: Option<&DateBounds>,
) -> Result<MessageSyncResponse, TicketSyncError> {
    let mysql_record = fetch_mysql_record(state, case_id).await?;
    let db_name = env::var("MONGO_DB_NAME").unwrap_or_else(|_| "correos_exchange_queretaro".into());
    let config_collection_name =
        env::var("MONGO_CONFIG_COLLECTION").unwrap_or_else(|_| "configuration".into());

    let config_email = mysql_record
        .config_email
        .clone()
        .ok_or_else(|| TicketSyncError::MissingConfigurationEmail(case_id.to_string()))?;
    let configuration_id =
        resolve_configuration_id(state, &db_name, &config_collection_name, &config_email).await?;

    let responses = fetch_case_responses(state, case_id, bounds).await?;
    if responses.is_empty() {
        return Ok(MessageSyncResponse {
            status: "ok".to_string(),
            detail: format!("No se encontraron respuestas para el caso {}", case_id),
            inserted: 0,
            mime_ids: Vec::new(),
            msg_struct_ids: Vec::new(),
        });
    }

    let db = state.mongo.database(&db_name);
    let mime_collection = db.collection::<MsgMimeDocument>("msg-mime");
    let struct_collection = db.collection::<MsgStructDocument>("msg-struct");
    let sanitized_case = sanitize_segment(case_id);
    let base_prefix = format!("tickets/{}", sanitized_case);

    let mut inserted_mime_ids = Vec::new();
    let mut inserted_struct_ids = Vec::new();
    let mut skipped = 0usize;

    for response in responses {
        if struct_collection
            .find_one(doc! {
                "id_mail": response.id,
                "message_type": "respuesta",
                "num_caso": case_id
            })
            .await?
            .is_some()
        {
            info!(
                "Respuesta {} del caso {} ya existe en MongoDB, se omite",
                response.id, case_id
            );
            skipped += 1;
            continue;
        }

        let folder = sanitize_segment(&format!("Respuesta_Num_-_{}", response.id));
        let prefix = format!("{}/respuestas/{}", base_prefix, folder);
        let (files, html_body) = build_files_from_s3(state, &prefix).await?;
        let subject = response
            .asunto
            .clone()
            .unwrap_or_else(|| format!("Respuesta {}", response.id));
        let html_content = html_body.unwrap_or_else(|| subject.clone());

        let mime_document = MsgMimeDocument {
            id: ObjectId::new(),
            configuration_id,
            message_type: "respuesta".to_string(),
            text: subject.clone(),
            file_mime: "text/html".to_string(),
            html: html_content,
            is_file_local: false,
            files,
        };
        let mime_id = mime_document.id;
        mime_collection.insert_one(mime_document.clone()).await?;

        let from_contact = None;

        let msg_struct_doc = MsgStructDocument {
            id: ObjectId::new(),
            id_mail: response.id,
            mime_id,
            configuration_id,
            date_creation: parse_mysql_datetime(response.fecha_registro.as_deref()),
            date_buzon: None,
            from: from_contact,
            to: contacts_from_emails(&response.correos_to),
            cc: contacts_from_emails(&response.correos_cc),
            message_type: "respuesta".to_string(),
            subject: Some(subject.clone()),
            conversation: Some(case_id.to_string()),
            num_caso: Some(case_id.to_string()),
            fechas_estatus: None,
            nombre_cliente: None,
            estatus: None,
            agente_asignado: None,
            categoria: None,
            subcategoria: None,
            fecha_cerrada: None,
            fecha_cliente: parse_mysql_datetime(response.fecha_cliente.as_deref()),
            numero_lineas: None,
            lista_caso: None,
        };

        struct_collection.insert_one(msg_struct_doc.clone()).await?;

        inserted_mime_ids.push(mime_id.to_hex());
        inserted_struct_ids.push(msg_struct_doc.id.to_hex());
    }

    let inserted = inserted_struct_ids.len();
    info!(
        "Respuestas caso {}: total {} | insertadas {} | omitidas {}",
        case_id,
        inserted + skipped,
        inserted,
        skipped
    );
    Ok(MessageSyncResponse {
        status: "ok".to_string(),
        detail: format!(
            "Se migraron {} respuestas para el caso {}",
            inserted, case_id
        ),
        inserted,
        mime_ids: inserted_mime_ids,
        msg_struct_ids: inserted_struct_ids,
    })
}

async fn fetch_mysql_record(
    state: &AppState,
    case_id: &str,
) -> Result<MysqlEmailRecord, TicketSyncError> {
    let statement = Statement::from_sql_and_values(
        DbBackend::MySql,
        "SELECT * FROM correos_entrada WHERE Num_Caso = ? LIMIT 1",
        vec![case_id.to_string().into()],
    );

    let rows = state.mysql.query_all_raw(statement).await?;
    let Some(row) = rows.into_iter().next() else {
        return Err(TicketSyncError::CaseNotFound(case_id.to_string()));
    };

    Ok(build_mysql_record(&row, Some(case_id.to_string())))
}

async fn fetch_distinct_case_ids(
    state: &AppState,
    table: &str,
    date_column: &str,
    bounds: &DateBounds,
) -> Result<Vec<String>, TicketSyncError> {
    let mut sql = format!("SELECT DISTINCT Num_Caso FROM {} WHERE 1=1", table);
    let mut values = Vec::new();
    append_date_filters(&mut sql, &mut values, date_column, bounds);
    sql.push_str(&format!(" ORDER BY {} ASC", date_column));

    let statement = Statement::from_sql_and_values(DbBackend::MySql, sql, values);
    let rows = state.mysql.query_all_raw(statement).await?;
    let mut ids = Vec::new();
    for row in rows {
        if let Some(id) = get_string(&row, "Num_Caso") {
            ids.push(id);
        }
    }
    Ok(ids)
}

async fn fetch_case_records_by_range(
    state: &AppState,
    bounds: &DateBounds,
) -> Result<Vec<MysqlEmailRecord>, TicketSyncError> {
    let mut sql = "SELECT * FROM correos_entrada WHERE 1=1".to_string();
    let mut values = Vec::new();
    append_date_filters(&mut sql, &mut values, "Fecha_de_Registro", bounds);
    sql.push_str(" ORDER BY Fecha_de_Registro ASC");

    let statement = Statement::from_sql_and_values(DbBackend::MySql, sql, values);
    let rows = state.mysql.query_all_raw(statement).await?;
    let mut seen = HashSet::new();
    let mut records = Vec::new();
    for row in rows {
        if let Some(case_id) = get_string(&row, "Num_Caso") {
            if seen.insert(case_id.clone()) {
                records.push(build_mysql_record(&row, Some(case_id)));
            }
        }
    }
    Ok(records)
}

async fn fetch_case_notes(
    state: &AppState,
    case_id: &str,
    bounds: Option<&DateBounds>,
) -> Result<Vec<MysqlNoteRecord>, TicketSyncError> {
    let mut sql =
        "SELECT Id, Num_Caso, Usuario, Fecha_de_Registro FROM correos_notas WHERE Num_Caso = ?"
            .to_string();
    let mut values = vec![case_id.to_string().into()];
    if let Some(bounds) = bounds {
        append_date_filters(&mut sql, &mut values, "Fecha_de_Registro", bounds);
    }
    sql.push_str(" ORDER BY Fecha_de_Registro ASC");

    let statement = Statement::from_sql_and_values(DbBackend::MySql, sql, values);
    let rows = state.mysql.query_all_raw(statement).await?;

    let mut records = Vec::new();
    for row in rows {
        let id = row.try_get::<i64>("", "Id").unwrap_or(0);
        if id == 0 {
            continue;
        }
        records.push(MysqlNoteRecord {
            id,
            usuario: get_string(&row, "Usuario"),
            fecha_registro: get_string(&row, "Fecha_de_Registro"),
        });
    }

    Ok(records)
}

async fn fetch_case_responses(
    state: &AppState,
    case_id: &str,
    bounds: Option<&DateBounds>,
) -> Result<Vec<MysqlResponseRecord>, TicketSyncError> {
    let mut sql = "SELECT * FROM correos_respuesta WHERE Num_Caso = ?".to_string();
    let mut values = vec![case_id.to_string().into()];
    if let Some(bounds) = bounds {
        append_date_filters(&mut sql, &mut values, "Fecha_de_Registro", bounds);
    }
    sql.push_str(" ORDER BY Fecha_de_Registro ASC");

    let statement = Statement::from_sql_and_values(DbBackend::MySql, sql, values);
    let rows = state.mysql.query_all_raw(statement).await?;
    let mut records = Vec::new();

    for row in rows {
        let id = row.try_get::<i64>("", "Id").unwrap_or(0);
        if id == 0 {
            continue;
        }
        records.push(MysqlResponseRecord {
            id,
            correos_to: split_emails(get_string(&row, "Correos_To")),
            correos_cc: split_emails(get_string(&row, "Correos_CC")),
            asunto: get_string(&row, "Asunto"),
            fecha_registro: get_string(&row, "Fecha_de_Registro"),
            fecha_cliente: get_string(&row, "Fecha_Cliente"),
        });
    }

    Ok(records)
}

async fn case_already_synced(
    state: &AppState,
    db_name: &str,
    case_id: &str,
) -> Result<bool, TicketSyncError> {
    let collection = state
        .mongo
        .database(db_name)
        .collection::<MsgStructDocument>("msg-struct");
    Ok(collection
        .find_one(doc! { "num_caso": case_id })
        .await?
        .is_some())
}

fn append_date_filters(
    sql: &mut String,
    values: &mut Vec<Value>,
    column: &str,
    bounds: &DateBounds,
) {
    sql.push_str(&format!(" AND {} >= ?", column));
    let start_dt = bounds
        .start
        .and_hms_opt(0, 0, 0)
        .unwrap_or_else(|| bounds.start.and_hms_milli_opt(0, 0, 0, 0).unwrap());
    values.push(start_dt.to_string().into());

    let end_exclusive = bounds
        .end
        .checked_add_signed(Duration::days(1))
        .unwrap_or(bounds.end);
    let end_dt = end_exclusive
        .and_hms_opt(0, 0, 0)
        .unwrap_or_else(|| end_exclusive.and_hms_milli_opt(0, 0, 0, 0).unwrap());
    sql.push_str(&format!(" AND {} < ?", column));
    values.push(end_dt.to_string().into());
}

async fn gather_case_files(
    state: &AppState,
    remote_case_path: &str,
    s3_prefix: &str,
    tasks: &mut Vec<FileTask>,
) -> Result<(), TicketSyncError> {
    info!(
        "Listando archivos del caso en ruta remota: {}",
        remote_case_path
    );
    let entries = state
        .ssh_service
        .list_remote_dir(remote_case_path)
        .await
        .map_err(|err| {
            error!(
                "Error al listar directorio remoto {}: {}",
                remote_case_path, err
            );
            err
        })?;

    for entry in entries {
        if entry.is_dir {
            match entry.name.to_lowercase().as_str() {
                "archivos" => {
                    gather_simple_dir(
                        state,
                        &entry.path,
                        &format!("{}/attachments/archivos", s3_prefix),
                        tasks,
                    )
                    .await?;
                }
                "imagenes" => {
                    gather_simple_dir(
                        state,
                        &entry.path,
                        &format!("{}/attachments/imagenes", s3_prefix),
                        tasks,
                    )
                    .await?;
                }
                "notas" => {
                    gather_notes(state, &entry.path, s3_prefix, tasks).await?;
                }
                "respuesta" => {
                    gather_responses(state, &entry.path, s3_prefix, tasks).await?;
                }
                _ => {}
            }
        } else {
            let lowered = entry.name.to_lowercase();
            if lowered.ends_with(".eml") {
                tasks.push(FileTask::from_entry(
                    entry,
                    format!("{}/original/{}", s3_prefix, sanitize_segment(&lowered)),
                    false,
                ));
            } else if lowered == "index.html" {
                tasks.push(FileTask::from_entry(
                    entry,
                    format!("{}/original/index.html", s3_prefix),
                    true,
                ));
            }
        }
    }

    Ok(())
}

async fn gather_simple_dir(
    state: &AppState,
    remote_dir: &str,
    key_prefix: &str,
    tasks: &mut Vec<FileTask>,
) -> Result<(), TicketSyncError> {
    let entries = state
        .ssh_service
        .list_remote_dir(remote_dir)
        .await
        .map_err(|err| {
            error!("Error al listar directorio remoto {}: {}", remote_dir, err);
            err
        })?;
    for entry in entries {
        if entry.is_dir {
            continue;
        }
        let key = format!("{}/{}", key_prefix, sanitize_segment(&entry.name));
        tasks.push(FileTask::from_entry(entry, key, false));
    }
    Ok(())
}

async fn gather_notes(
    state: &AppState,
    remote_dir: &str,
    s3_prefix: &str,
    tasks: &mut Vec<FileTask>,
) -> Result<(), TicketSyncError> {
    let notes = state
        .ssh_service
        .list_remote_dir(remote_dir)
        .await
        .map_err(|err| {
            error!("Error al listar directorio remoto {}: {}", remote_dir, err);
            err
        })?;
    for note in notes {
        if !note.is_dir {
            continue;
        }
        let note_prefix = format!("{}/notas/{}", s3_prefix, sanitize_segment(&note.name));
        let contents = state
            .ssh_service
            .list_remote_dir(&note.path)
            .await
            .map_err(|err| {
                error!("Error al listar directorio remoto {}: {}", note.path, err);
                err
            })?;
        for content in contents {
            if content.is_dir {
                let dir_name = content.name.to_lowercase();
                if dir_name == "archivos" {
                    gather_simple_dir(
                        state,
                        &content.path,
                        &format!("{}/archivos", note_prefix),
                        tasks,
                    )
                    .await?;
                }
                continue;
            }
            let key = format!("{}/{}", note_prefix, sanitize_segment(&content.name));
            tasks.push(FileTask::from_entry(content, key, false));
        }
    }
    Ok(())
}

async fn gather_responses(
    state: &AppState,
    remote_dir: &str,
    s3_prefix: &str,
    tasks: &mut Vec<FileTask>,
) -> Result<(), TicketSyncError> {
    let responses = state
        .ssh_service
        .list_remote_dir(remote_dir)
        .await
        .map_err(|err| {
            error!("Error al listar directorio remoto {}: {}", remote_dir, err);
            err
        })?;
    for response in responses {
        if !response.is_dir {
            continue;
        }
        let response_prefix = format!(
            "{}/respuestas/{}",
            s3_prefix,
            sanitize_segment(&response.name)
        );
        let contents = state
            .ssh_service
            .list_remote_dir(&response.path)
            .await
            .map_err(|err| {
                error!(
                    "Error al listar directorio remoto {}: {}",
                    response.path, err
                );
                err
            })?;
        for content in contents {
            if content.is_dir {
                match content.name.to_lowercase().as_str() {
                    "archivos" => {
                        gather_simple_dir(
                            state,
                            &content.path,
                            &format!("{}/archivos", response_prefix),
                            tasks,
                        )
                        .await?;
                    }
                    "imagenes" => {
                        gather_simple_dir(
                            state,
                            &content.path,
                            &format!("{}/imagenes", response_prefix),
                            tasks,
                        )
                        .await?;
                    }
                    _ => {}
                }
                continue;
            }
            let lowered = content.name.to_lowercase();
            let (key_suffix, capture_html) = if lowered == "index.html" {
                ("index.html".to_string(), false)
            } else {
                (sanitize_segment(&content.name), false)
            };
            let key = format!("{}/{}", response_prefix, key_suffix);
            tasks.push(FileTask::from_entry(content, key, capture_html));
        }
    }
    Ok(())
}

async fn process_case_file_tasks(
    state: &AppState,
    tasks: Vec<FileTask>,
) -> Result<(Vec<String>, Vec<MsgMimeFile>, Option<String>), TicketSyncError> {
    if tasks.is_empty() {
        return Ok((Vec::new(), Vec::new(), None));
    }

    let ssh_service = Arc::clone(&state.ssh_service);
    let storage = Arc::clone(&state.storage);

    let upload_concurrency = file_upload_concurrency();

    let results = stream::iter(tasks.into_iter().map(|task| {
        let ssh = Arc::clone(&ssh_service);
        let storage = Arc::clone(&storage);

        async move {
            let bytes = ssh
                .read_remote_file(&task.remote_path)
                .await
                .map_err(|err| {
                    error!(
                        "Error al leer archivo remoto {}: {}",
                        task.remote_path, err
                    );
                    err
                })?;
            let html_body = if task.capture_main_html {
                Some(
                    String::from_utf8(bytes.clone())
                        .unwrap_or_else(|_| String::from_utf8_lossy(&bytes).into_owned()),
                )
            } else {
                None
            };

            storage
                .upload_object(&task.key, bytes, Some(&task.file_mime))
                .await?;

            let url = storage.object_url(&task.key);
            let mime_file = MsgMimeFile {
                id: ObjectId::new().to_hex(),
                file_image_html: task.file_mime.starts_with("image/")
                    || task.file_mime.contains("html"),
                file_name: task.original_name,
                file_type: task.file_mime,
                file_size: task.file_size.to_string(),
                file_url: url.clone(),
                is_file_local: false,
            };

            Ok::<_, TicketSyncError>((url, mime_file, html_body))
        }
    }))
    .buffer_unordered(upload_concurrency)
    .try_collect::<Vec<_>>()
    .await?;

    let mut uploaded_urls = Vec::with_capacity(results.len());
    let mut mime_files = Vec::with_capacity(results.len());
    let mut html_body = None;

    for (url, file, html_candidate) in results {
        if html_body.is_none() {
            html_body = html_candidate;
        }
        uploaded_urls.push(url);
        mime_files.push(file);
    }

    Ok((uploaded_urls, mime_files, html_body))
}

async fn resolve_configuration_id(
    state: &AppState,
    db_name: &str,
    collection_name: &str,
    email: &str,
) -> Result<ObjectId, TicketSyncError> {
    let collection = state
        .mongo
        .database(db_name)
        .collection::<EmailConfigDocument>(collection_name);
    let Some(document) = collection
        .find_one(doc! { "incoming_email": email })
        .await?
    else {
        return Err(TicketSyncError::ConfigurationNotFound(email.to_string()));
    };
    Ok(document.id)
}

async fn build_files_from_s3(
    state: &AppState,
    prefix: &str,
) -> Result<(Vec<MsgMimeFile>, Option<String>), TicketSyncError> {
    let normalized_prefix = prefix.trim_end_matches('/');
    let list_prefix = if normalized_prefix.is_empty() {
        normalized_prefix.to_string()
    } else {
        format!("{}/", normalized_prefix)
    };
    let mut objects = state.storage.list_objects(&list_prefix).await?;
    objects.sort_by(|a, b| a.key.cmp(&b.key));

    let mut files = Vec::new();
    let mut html_body = None;

    for object in objects {
        if object.key.ends_with('/') {
            continue;
        }
        let key = object.key;
        let key_str = key.as_str();
        let file_name = key_str.rsplit('/').next().unwrap_or(key_str).to_string();

        if html_body.is_none() && file_name.eq_ignore_ascii_case("index.html") {
            match state.storage.get_object(&key).await {
                Ok(bytes) => {
                    let text = String::from_utf8(bytes)
                        .unwrap_or_else(|err| String::from_utf8_lossy(err.as_bytes()).into_owned());
                    html_body = Some(text);
                }
                Err(err) => {
                    warn!("No se pudo leer {key} desde S3 para generar HTML: {err}");
                }
            }
        }

        let mime = MimeGuess::from_path(&file_name)
            .first_raw()
            .unwrap_or("application/octet-stream")
            .to_string();

        files.push(MsgMimeFile {
            id: ObjectId::new().to_hex(),
            file_image_html: mime.starts_with("image/") || mime.contains("html"),
            file_name,
            file_type: mime,
            file_size: object.size.max(0).to_string(),
            file_url: state.storage.object_url(&key),
            is_file_local: false,
        });
    }

    Ok((files, html_body))
}

#[derive(Clone)]
struct FileTask {
    remote_path: String,
    key: String,
    original_name: String,
    file_mime: String,
    file_size: u64,
    capture_main_html: bool,
}

impl FileTask {
    fn from_entry(entry: RemoteDirEntry, key: String, capture_main_html: bool) -> Self {
        let guess: MimeGuess = MimeGuess::from_path(&entry.name);
        let mime = guess
            .first_raw()
            .unwrap_or("application/octet-stream")
            .to_string();
        Self {
            remote_path: entry.path,
            key,
            original_name: entry.name,
            file_mime: mime,
            file_size: entry.size,
            capture_main_html,
        }
    }
}

fn sanitize_segment(value: &str) -> String {
    let sanitized = value
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string();

    if sanitized.is_empty() {
        "adjunto".to_string()
    } else {
        sanitized
    }
}

fn contacts_from_emails(emails: &[String]) -> Vec<MsgContact> {
    emails
        .iter()
        .map(|email| MsgContact {
            name: None,
            email: Some(email.clone()),
        })
        .collect()
}

fn bson_to_case_id(value: Bson) -> Option<String> {
    match value {
        Bson::String(value) => Some(value),
        Bson::Int32(value) => Some(value.to_string()),
        Bson::Int64(value) => Some(value.to_string()),
        Bson::Double(value) => {
            if (value.fract() - 0.0).abs() < f64::EPSILON {
                Some(format!("{:.0}", value))
            } else {
                Some(value.to_string())
            }
        }
        Bson::ObjectId(value) => Some(value.to_hex()),
        _ => None,
    }
}

struct MysqlEmailRecord {
    case_id: String,
    subject: Option<String>,
    from_email: Option<String>,
    from_name: Option<String>,
    to_emails: Vec<String>,
    cc_emails: Vec<String>,
    id_mail: Option<i64>,
    config_email: Option<String>,
    fecha_buzon: Option<String>,
    fechas_estatus: Option<String>,
    nombre_cliente: Option<String>,
    estatus: Option<String>,
    agente_asignado: Option<String>,
    categoria: Option<String>,
    subcategoria: Option<String>,
    fecha_cerrada: Option<String>,
    fecha_cliente: Option<String>,
    numero_lineas: Option<String>,
    lista_caso: Option<String>,
    fecha_de_registro: Option<String>,
}

struct MysqlNoteRecord {
    id: i64,
    usuario: Option<String>,
    fecha_registro: Option<String>,
}

struct MysqlResponseRecord {
    id: i64,
    correos_to: Vec<String>,
    correos_cc: Vec<String>,
    asunto: Option<String>,
    fecha_registro: Option<String>,
    fecha_cliente: Option<String>,
}

fn get_string(row: &sea_orm::QueryResult, column: &str) -> Option<String> {
    row.try_get::<String>("", column)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            row.try_get::<i64>("", column)
                .ok()
                .map(|value| value.to_string())
        })
        .or_else(|| {
            row.try_get::<NaiveDateTime>("", column)
                .ok()
                .map(|value| value.format("%Y-%m-%d %H:%M:%S").to_string())
        })
}

fn split_emails(raw: Option<String>) -> Vec<String> {
    raw.map(|value| {
        value
            .split([';', ','])
            .map(|chunk| chunk.trim())
            .filter(|chunk| !chunk.is_empty())
            .map(|chunk| chunk.to_string())
            .collect()
    })
    .unwrap_or_default()
}

fn get_any_string(row: &sea_orm::QueryResult, columns: &[&str]) -> Option<String> {
    columns.iter().find_map(|column| get_string(row, column))
}

fn build_mysql_record(
    row: &sea_orm::QueryResult,
    fallback_case_id: Option<String>,
) -> MysqlEmailRecord {
    let case_id = get_string(row, "Num_Caso")
        .or(fallback_case_id)
        .unwrap_or_default();
    let subject = get_any_string(row, &["Asunto"]);
    let from_email = get_any_string(row, &["Correo_Entrada", "Correo"]);
    let from_name = get_any_string(row, &["Remitente", "Nombre_cliente", "Nombre_Cliente"]);
    let to_emails = split_emails(get_any_string(row, &["Correo_Destino", "Correos_To"]));
    let cc_emails = split_emails(get_any_string(row, &["Correos_CC"]));
    let config_email = get_any_string(row, &["Correo_Telcel"]);
    let id_mail = row
        .try_get("", "Id_Correo")
        .ok()
        .or_else(|| row.try_get("", "IdCorreo").ok());

    let fechas_estatus =
        get_any_string(row, &["Fechas_estatus", "Fecha_Estatus", "Fechas_Estatus"]);
    let nombre_cliente = get_any_string(row, &["Nombre_cliente", "Nombre_Cliente"]);
    let estatus = get_any_string(row, &["Estatus"]);
    let agente_asignado = get_any_string(row, &["Agente_asignado", "Agente_Asignado"]);
    let categoria = get_any_string(row, &["Categoria"]);
    let subcategoria = get_any_string(row, &["Subcategoria", "Subcategoría", "SubCategoría"]);
    let fecha_cerrada = get_any_string(row, &["Fecha_cerrada", "Fecha_Cerrada"]);
    let fecha_cliente = get_any_string(row, &["Fecha_cliente", "Fecha_Cliente"]);
    let numero_lineas = get_any_string(row, &["Numero_lineas", "Numero_Lineas"]);
    let lista_caso = get_any_string(row, &["Lista_caso", "Lista_Caso"]);
    let fecha_buzon = get_any_string(
        row,
        &["Fecha_buzon", "Fecha_Buzon", "Fecha_Buzón", "Fecha_buzón"],
    );
    let fecha_de_registro = get_any_string(
        row,
        &["Fecha_de_Registro", "Fecha_de_registro", "Fecha_Registro"],
    );

    MysqlEmailRecord {
        case_id,
        subject,
        from_email,
        from_name,
        to_emails,
        cc_emails,
        id_mail,
        config_email,
        fecha_buzon,
        fechas_estatus,
        nombre_cliente,
        estatus,
        agente_asignado,
        categoria,
        subcategoria,
        fecha_cerrada,
        fecha_cliente,
        numero_lineas,
        lista_caso,
        fecha_de_registro,
    }
}

fn parse_mysql_datetime(raw: Option<&str>) -> Option<mongodb::bson::DateTime> {
    const FORMATS: [&str; 3] = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %I:%M:%S%p", "%Y-%m-%d %H:%M"];
    raw.and_then(|value| {
        let trimmed = value.trim();
        let normalized = if trimmed
            .rsplit_once(' ')
            .map(|(_, suffix)| {
                suffix.eq_ignore_ascii_case("am") || suffix.eq_ignore_ascii_case("pm")
            })
            .unwrap_or(false)
        {
            let mut parts = trimmed.rsplitn(2, ' ');
            let suffix = parts.next().unwrap().to_uppercase();
            let prefix = parts.next().unwrap_or("");
            format!("{prefix} {suffix}")
        } else if trimmed.ends_with("am")
            || trimmed.ends_with("pm")
            || trimmed.ends_with("AM")
            || trimmed.ends_with("PM")
        {
            let (prefix, suffix) = trimmed.split_at(trimmed.len().saturating_sub(2));
            format!("{}{}", prefix, suffix.to_uppercase())
        } else {
            trimmed.to_string()
        };

        FORMATS
            .iter()
            .find_map(|fmt| NaiveDateTime::parse_from_str(&normalized, fmt).ok())
            .map(|naive| mongodb::bson::DateTime::from_millis(naive.and_utc().timestamp_millis()))
    })
}

#[derive(Debug, thiserror::Error)]
enum UserSyncError {
    #[error("Error al consultar MySQL: {0}")]
    Mysql(#[from] sea_orm::DbErr),
    #[error("Error de MongoDB: {0}")]
    Mongo(#[from] mongodb::error::Error),
    #[error("No se pudo serializar el documento MongoDB: {0}")]
    Bson(#[from] mongodb::bson::ser::Error),
}

#[derive(Debug, thiserror::Error)]
enum TicketSyncError {
    #[error("No se encontraron registros para el caso {0}")]
    CaseNotFound(String),
    #[error("El caso {0} no cuenta con correo de configuración (Corre_Telcel)")]
    MissingConfigurationEmail(String),
    #[error("No se encontró configuración en MongoDB para el correo {0}")]
    ConfigurationNotFound(String),
    #[error(transparent)]
    Mysql(#[from] sea_orm::DbErr),
    #[error(transparent)]
    Ssh(#[from] SshError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),
}

impl TicketSyncError {
    fn status_code(&self) -> StatusCode {
        match self {
            TicketSyncError::CaseNotFound(_) | TicketSyncError::ConfigurationNotFound(_) => {
                StatusCode::NOT_FOUND
            }
            TicketSyncError::MissingConfigurationEmail(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
