use std::env;

use axum::{
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use chrono::NaiveDateTime;
use mime_guess::MimeGuess;
use mongodb::bson::{doc, oid::ObjectId};
use sea_orm::{ConnectionTrait, DbBackend, Statement};
use tracing::{error, info};

use crate::{
    entities::{
        email_config::EmailConfigDocument,
        msg_mime::{MsgMimeDocument, MsgMimeFile},
        msg_struct::{MsgContact, MsgStructDocument},
    },
    routes::structs::TicketSyncResponse,
    ssh::config::RemoteDirEntry,
    ssh::SshError,
    state::AppState,
    storage::StorageError,
};

pub fn router() -> Router<AppState> {
    Router::new().route("/tickets/:case_id/sync", post(sync_single_ticket))
}

async fn sync_single_ticket(
    State(state): State<AppState>,
    AxumPath(case_id): AxumPath<String>,
) -> Response {
    match sync_ticket_case(&state, &case_id).await {
        Ok(summary) => (StatusCode::OK, Json(summary)).into_response(),
        Err(err) => {
            error!("Fallo al sincronizar ticket {}: {}", case_id, err);
            let status = err.status_code();
            let body = Json(TicketSyncResponse {
                status: "error".to_string(),
                detail: err.to_string(),
                uploaded: Vec::new(),
                mongo_id: None,
                msg_struct_id: None,
            });
            (status, body).into_response()
        }
    }
}

async fn sync_ticket_case(
    state: &AppState,
    case_id: &str,
) -> Result<TicketSyncResponse, TicketSyncError> {
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

    let sanitized_case = sanitize_segment(case_id);
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

    let mut uploaded_urls = Vec::new();
    let mut mime_files = Vec::new();
    let mut html_body = String::new();

    for task in tasks {
        let bytes = state
            .ssh_service
            .read_remote_file(&task.remote_path)
            .await?;

        if task.capture_main_html {
            html_body = String::from_utf8(bytes.clone())
                .unwrap_or_else(|_| String::from_utf8_lossy(&bytes).into_owned());
        }

        state
            .storage
            .upload_object(&task.key, bytes.clone(), Some(&task.file_mime))
            .await?;

        let url = state.storage.object_url(&task.key);
        uploaded_urls.push(url.clone());

        mime_files.push(MsgMimeFile {
            id: ObjectId::new().to_hex(),
            file_image_html: task.file_mime.starts_with("image/")
                || task.file_mime.contains("html"),
            file_name: task.original_name.clone(),
            file_type: task.file_mime.clone(),
            file_size: task.file_size.to_string(),
            file_url: url,
            is_file_local: false,
        });
    }

    let mime_collection = state
        .mongo
        .database(&db_name)
        .collection::<MsgMimeDocument>("msg-mime");

    let html_content = if html_body.is_empty() {
        mysql_record.subject.clone().unwrap_or_default()
    } else {
        html_body
    };

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

    let subject = get_any_string(&row, &["Asunto"]);
    let from_email = get_any_string(&row, &["Correo_Entrada", "Correo"]);
    let from_name = get_any_string(&row, &["Remitente", "Nombre_cliente", "Nombre_Cliente"]);
    let to_emails = split_emails(get_any_string(&row, &["Correo_Destino", "Correos_To"]));
    let cc_emails = split_emails(get_any_string(&row, &["Correos_CC"]));
    let config_email = get_any_string(&row, &["Correo_Telcel"]);
    let id_mail = row
        .try_get("", "Id_Correo")
        .ok()
        .or_else(|| row.try_get("", "IdCorreo").ok());

    let fechas_estatus =
        get_any_string(&row, &["Fechas_estatus", "Fecha_Estatus", "Fechas_Estatus"]);
    let nombre_cliente = get_any_string(&row, &["Nombre_cliente", "Nombre_Cliente"]);
    let estatus = get_any_string(&row, &["Estatus"]);
    let agente_asignado = get_any_string(&row, &["Agente_asignado", "Agente_Asignado"]);
    let categoria = get_any_string(&row, &["Categoria"]);
    let subcategoria = get_any_string(&row, &["Subcategoria", "Subcategoría", "SubCategoría"]);
    let fecha_cerrada = get_any_string(&row, &["Fecha_cerrada", "Fecha_Cerrada"]);
    let fecha_cliente = get_any_string(&row, &["Fecha_cliente", "Fecha_Cliente"]);
    let numero_lineas = get_any_string(&row, &["Numero_lineas", "Numero_Lineas"]);
    let lista_caso = get_any_string(&row, &["Lista_caso", "Lista_Caso"]);
    let fecha_buzon = get_any_string(
        &row,
        &["Fecha_buzon", "Fecha_Buzon", "Fecha_Buzón", "Fecha_buzón"],
    );
    let fecha_de_registro = get_any_string(
        &row,
        &["Fecha_de_Registro", "Fecha_de_registro", "Fecha_Registro"],
    );

    Ok(MysqlEmailRecord {
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
    })
}

async fn gather_case_files(
    state: &AppState,
    remote_case_path: &str,
    s3_prefix: &str,
    tasks: &mut Vec<FileTask>,
) -> Result<(), TicketSyncError> {
    let entries = state.ssh_service.list_remote_dir(remote_case_path).await?;

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
    let entries = state.ssh_service.list_remote_dir(remote_dir).await?;
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
    let notes = state.ssh_service.list_remote_dir(remote_dir).await?;
    for note in notes {
        if !note.is_dir {
            continue;
        }
        let note_prefix = format!("{}/notas/{}", s3_prefix, sanitize_segment(&note.name));
        let contents = state.ssh_service.list_remote_dir(&note.path).await?;
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
    let responses = state.ssh_service.list_remote_dir(remote_dir).await?;
    for response in responses {
        if !response.is_dir {
            continue;
        }
        let response_prefix = format!(
            "{}/respuestas/{}",
            s3_prefix,
            sanitize_segment(&response.name)
        );
        let contents = state.ssh_service.list_remote_dir(&response.path).await?;
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

struct MysqlEmailRecord {
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

fn get_string(row: &sea_orm::QueryResult, column: &str) -> Option<String> {
    row.try_get::<String>("", column)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
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
