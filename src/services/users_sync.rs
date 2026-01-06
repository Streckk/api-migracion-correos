use std::{collections::HashMap, env};

use chrono::{NaiveDateTime, TimeZone};
use chrono_tz::America::Monterrey;
use mongodb::bson::Document;
use sea_orm::{ConnectionTrait, DbBackend, Statement, Value};
use tracing::{error, info, warn};

use crate::{
    entities::user_migration::{
        hash_password_argon2id, normalize_campaign_name, normalize_lookup_key, split_name,
        MongoUserDocument, MysqlUser, NormalizedUser, UserSettingsDocument,
    },
    services::mongo::{self, BranchOfficeRef, BranchOfficeRefs, UpsertResult},
    state::AppState,
};

const DEFAULT_USER_BATCH_SIZE: usize = 25;

#[derive(Debug)]
pub enum UserSyncResult {
    NoUsers,
    Summary(UserSyncSummary),
}

#[derive(Debug, Default)]
pub struct UserSyncSummary {
    pub processed: usize,
    pub inserted: usize,
    pub skipped: usize,
    pub errors: usize,
    pub skipped_missing_reference: usize,
    pub skipped_duplicate: usize,
    pub missing_references: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum UserSyncError {
    #[error("Error al consultar MySQL: {0}")]
    Mysql(#[from] sea_orm::DbErr),
    #[error("Error de MongoDB: {0}")]
    Mongo(#[from] mongodb::error::Error),
    #[error("No se pudo serializar el documento MongoDB: {0}")]
    Bson(#[from] mongodb::bson::ser::Error),
}

pub async fn sync_users(
    state: &AppState,
    requested_ids: Vec<String>,
) -> Result<UserSyncResult, UserSyncError> {
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
        fetch_all_mysql_users(state).await?
    } else {
        fetch_mysql_users_by_ids(state, &ids_to_fetch).await?
    };

    if mysql_users.is_empty() {
        return Ok(UserSyncResult::NoUsers);
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
        mongo::filter_pending_users(&collection_docs, mysql_users)
            .await
            .map_err(UserSyncError::Mongo)?;
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
    let refs = mongo::load_branch_office_refs(state, &users_db_name)
        .await
        .map_err(UserSyncError::Mongo)?;

    let mut summary = UserSyncSummary::default();
    summary.skipped_duplicate = already_present;

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
            summary.processed += 1;
            let normalized = normalize_mysql_user(raw_user, &aliases);
            let Some(reference) = resolve_user_references(&normalized, &refs) else {
                warn!(
                    "Usuarios - No se encontró referencia para campaña '{}' y departamento '{}' (usuario {})",
                    normalized.campaign_name,
                    normalized.department_name,
                    normalized.username
                );
                summary.skipped += 1;
                summary.skipped_missing_reference += 1;
                if summary.missing_references.len() < 10 {
                    summary.missing_references.push(format!(
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
            let settings_id = match mongo::insert_user_settings(&settings_collection, &settings_doc)
                .await
            {
                Ok(Some(value)) => value,
                Ok(None) => {
                    summary.errors += 1;
                    error!(
                        "Usuarios - No se obtuvo ObjectId para settings de {}",
                        normalized.username
                    );
                    continue;
                }
                Err(err) => {
                    summary.errors += 1;
                    error!(
                        "Usuarios - Fallo al insertar settings de {}: {}",
                        normalized.username, err
                    );
                    continue;
                }
            };
            settings_doc.id = Some(settings_id);
            info!(
                "Usuarios - Settings insertado en {}.{} con _id {}",
                users_db_name,
                settings_collection_name,
                settings_id.to_hex()
            );

            let mongo_user = build_mongo_user_document(&normalized, &reference, settings_doc);
            match mongo::upsert_mongo_user(&collection, &mongo_user).await {
                Ok(UpsertResult::Inserted) => summary.inserted += 1,
                Ok(UpsertResult::Existing) => {
                    summary.skipped += 1;
                    summary.skipped_duplicate += 1;
                }
                Err(err) => {
                    summary.errors += 1;
                    error!(
                        "Usuarios - Fallo al insertar usuario {}: {}",
                        normalized.username, err
                    );
                }
            }
        }
    }

    Ok(UserSyncResult::Summary(summary))
}

fn user_batch_size() -> usize {
    env::var("USER_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_USER_BATCH_SIZE)
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
    document.gender = String::new();
    document.status = user.status;
    document.deleted = 0;
    document.date_registration = user
        .created_at
        .map(|value| mongodb::bson::DateTime::from_millis(value.and_utc().timestamp_millis()));
    document.date_change_password = user
        .password_changed_at
        .map(|value| mongodb::bson::DateTime::from_millis(value.and_utc().timestamp_millis()));
    document.date_last_connection = user
        .last_connection
        .map(|value| mongodb::bson::DateTime::from_millis(value.and_utc().timestamp_millis()));
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
    let agendado_ticket = get_i32(&row, &["Agendado_Ticket", "agendado_ticket"]);
    let eliminacion_casos = get_i32(&row, &["Eliminacion_Casos", "eliminacion_casos"]);
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


fn get_any_string(row: &sea_orm::QueryResult, columns: &[&str]) -> Option<String> {
    columns.iter().find_map(|column| get_string(row, column))
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
