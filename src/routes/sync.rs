use std::{collections::HashSet, env, sync::Arc};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use chrono::NaiveDateTime;
use futures::stream::{self, StreamExt, TryStreamExt};
use mime_guess::MimeGuess;
use mongodb::bson::{doc, oid::ObjectId, Bson};
use sea_orm::{ConnectionTrait, DbBackend, Statement, Value};
use serde::Deserialize;
use tracing::{error, info, warn};

use crate::{
    entities::{
        email_config::EmailConfigDocument,
        msg_mime::{MsgMimeDocument, MsgMimeFile},
        msg_struct::{MsgContact, MsgStructDocument},
    },
    routes::structs::{MessageSyncResponse, TicketBatchResponse, TicketSyncResponse},
    ssh::config::RemoteDirEntry,
    ssh::SshError,
    state::AppState,
    storage::StorageError,
};

const CASE_BATCH_SIZE: usize = 10;
const FILE_UPLOAD_CONCURRENCY: usize = 4;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tickets/sync", post(sync_tickets_by_range))
        .route("/tickets/obtener_correos_notas", post(sync_notes_by_range))
        .route(
            "/tickets/obtener_correos_respuesta",
            post(sync_responses_by_range),
        )
}

#[derive(Debug, Default, Deserialize)]
struct DateRangeQuery {
    #[serde(default)]
    fecha_inicio: Option<String>,
    #[serde(default)]
    fecha_fin: Option<String>,
}

impl DateRangeQuery {
    fn resolved_bounds(&self) -> Option<DateBounds> {
        let start = self
            .fecha_inicio
            .as_ref()
            .map(|value| extract_date(value))?;
        let end = self
            .fecha_fin
            .as_ref()
            .map(|value| extract_date(value))
            .unwrap_or_else(|| current_date_string());
        Some(DateBounds { start, end })
    }
}

#[derive(Clone)]
struct DateBounds {
    start: String,
    end: String,
}

fn current_date_string() -> String {
    chrono::Local::now()
        .date_naive()
        .format("%Y-%m-%d")
        .to_string()
}

fn extract_date(value: &str) -> String {
    value
        .split(|c| c == ' ' || c == 'T')
        .next()
        .filter(|part| !part.is_empty())
        .map(|part| part.to_string())
        .unwrap_or_else(|| value.to_string())
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

    let total_batches = (cases.len() + CASE_BATCH_SIZE - 1) / CASE_BATCH_SIZE;
    info!(
        "Se procesarán {} casos en {} lote(s) de hasta {} elementos",
        cases.len(),
        total_batches,
        CASE_BATCH_SIZE
    );

    let mut successes = Vec::new();
    let mut failures = Vec::new();
    let mut iter = cases.into_iter();
    let mut current_batch = 0usize;

    loop {
        let batch: Vec<MysqlEmailRecord> = iter.by_ref().take(CASE_BATCH_SIZE).collect();
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

        for record in batch {
            let case_id = record.case_id.clone();
            match sync_ticket_case(&state, record).await {
                Ok(_) => successes.push(case_id),
                Err(err) => {
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
    let Some(bounds) = range.resolved_bounds() else {
        let body = Json(MessageSyncResponse {
            status: "error".to_string(),
            detail: "Debes proporcionar al menos fecha_inicio".to_string(),
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

    if case_ids.is_empty() {
        let body = Json(MessageSyncResponse {
            status: "ok".to_string(),
            detail: "No se encontraron notas en el rango solicitado".to_string(),
            inserted: 0,
            mime_ids: Vec::new(),
            msg_struct_ids: Vec::new(),
        });
        return (StatusCode::OK, body).into_response();
    }

    let mut total_inserted = 0;
    let mut mime_ids = Vec::new();
    let mut msg_struct_ids = Vec::new();
    let mut failures = Vec::new();

    for case_id in case_ids {
        match sync_notes_for_case(&state, &case_id, &bounds).await {
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
    let Some(bounds) = range.resolved_bounds() else {
        let body = Json(MessageSyncResponse {
            status: "error".to_string(),
            detail: "Debes proporcionar al menos fecha_inicio".to_string(),
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

    if case_ids.is_empty() {
        let body = Json(MessageSyncResponse {
            status: "ok".to_string(),
            detail: "No se encontraron respuestas en el rango solicitado".to_string(),
            inserted: 0,
            mime_ids: Vec::new(),
            msg_struct_ids: Vec::new(),
        });
        return (StatusCode::OK, body).into_response();
    }

    let mut total_inserted = 0;
    let mut mime_ids = Vec::new();
    let mut msg_struct_ids = Vec::new();
    let mut failures = Vec::new();

    for case_id in case_ids {
        match sync_responses_for_case(&state, &case_id, &bounds).await {
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
    bounds: &DateBounds,
) -> Result<MessageSyncResponse, TicketSyncError> {
    let mysql_record = fetch_mysql_record(state, case_id).await?;
    let db_name = env::var("MONGO_DB_NAME").unwrap_or_else(|_| "correos_exchange_queretaro".into());
    let config_collection_name =
        env::var("MONGO_CONFIG_COLLECTION").unwrap_or_else(|_| "configuration".into());

    if case_already_synced(state, &db_name, case_id).await? {
        return Ok(MessageSyncResponse {
            status: "skipped".to_string(),
            detail: format!("El caso {case_id} ya existe en MongoDB"),
            inserted: 0,
            mime_ids: Vec::new(),
            msg_struct_ids: Vec::new(),
        });
    }

    let config_email = mysql_record
        .config_email
        .clone()
        .ok_or_else(|| TicketSyncError::MissingConfigurationEmail(case_id.to_string()))?;
    let configuration_id =
        resolve_configuration_id(state, &db_name, &config_collection_name, &config_email).await?;

    let notes = fetch_case_notes(state, case_id, bounds).await?;
    if notes.is_empty() {
        return Ok(MessageSyncResponse {
            status: "ok".to_string(),
            detail: format!("No se encontraron notas para el caso {}", case_id),
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

    for note in notes {
        if struct_collection
            .find_one(doc! { "id_mail": note.id })
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
    bounds: &DateBounds,
) -> Result<MessageSyncResponse, TicketSyncError> {
    let mysql_record = fetch_mysql_record(state, case_id).await?;
    let db_name = env::var("MONGO_DB_NAME").unwrap_or_else(|_| "correos_exchange_queretaro".into());
    let config_collection_name =
        env::var("MONGO_CONFIG_COLLECTION").unwrap_or_else(|_| "configuration".into());

    if case_already_synced(state, &db_name, case_id).await? {
        return Ok(MessageSyncResponse {
            status: "skipped".to_string(),
            detail: format!("El caso {case_id} ya existe en MongoDB"),
            inserted: 0,
            mime_ids: Vec::new(),
            msg_struct_ids: Vec::new(),
        });
    }

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

    for response in responses {
        if struct_collection
            .find_one(doc! { "id_mail": response.id })
            .await?
            .is_some()
        {
            info!(
                "Respuesta {} del caso {} ya existe en MongoDB, se omite",
                response.id, case_id
            );
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
    bounds: &DateBounds,
) -> Result<Vec<MysqlNoteRecord>, TicketSyncError> {
    let mut sql =
        "SELECT Id, Num_Caso, Usuario, Fecha_de_Registro FROM correos_notas WHERE Num_Caso = ?"
            .to_string();
    let mut values = vec![case_id.to_string().into()];
    append_date_filters(&mut sql, &mut values, "Fecha_de_Registro", bounds);
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
    bounds: &DateBounds,
) -> Result<Vec<MysqlResponseRecord>, TicketSyncError> {
    let mut sql = "SELECT * FROM correos_respuesta WHERE Num_Caso = ?".to_string();
    let mut values = vec![case_id.to_string().into()];
    append_date_filters(&mut sql, &mut values, "Fecha_de_Registro", bounds);
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
    sql.push_str(&format!(" AND DATE({}) >= DATE(?)", column));
    values.push(bounds.start.clone().into());
    sql.push_str(&format!(" AND DATE({}) <= DATE(?)", column));
    values.push(bounds.end.clone().into());
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

async fn process_case_file_tasks(
    state: &AppState,
    tasks: Vec<FileTask>,
) -> Result<(Vec<String>, Vec<MsgMimeFile>, Option<String>), TicketSyncError> {
    if tasks.is_empty() {
        return Ok((Vec::new(), Vec::new(), None));
    }

    let ssh_service = Arc::clone(&state.ssh_service);
    let storage = Arc::clone(&state.storage);

    let results = stream::iter(tasks.into_iter().map(|task| {
        let ssh = Arc::clone(&ssh_service);
        let storage = Arc::clone(&storage);

        async move {
            let bytes = ssh.read_remote_file(&task.remote_path).await?;
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
    .buffer_unordered(FILE_UPLOAD_CONCURRENCY)
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
