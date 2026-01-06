use std::env;

use axum::{
    extract::{ Query, State },
    http::StatusCode,
    response::{ IntoResponse, Response },
};
use mongodb::bson::doc;
use serde::Deserialize;
use tracing::{ error, info, warn };

use crate::{
    mappers::ticket_documents::{
        build_note_mime,
        build_note_struct,
        build_response_mime,
        build_response_struct,
        build_ticket_mime,
        build_ticket_struct,
        current_date,
        parse_date_only,
        sanitize_segment,
        select_html_content,
    },
    routes::structs::{ MessageSyncResponse, TicketSyncResponse },
    services::tickets_batches::{ case_batch_size, case_parallelism, run_batches },
    services::tickets_files::{
        build_files_from_s3, gather_case_files, process_case_file_tasks, TicketsFilesError,
    },
    services::tickets_responses::{ message_sync_body, ticket_batch_body },
    services::mysql::{
        fetch_case_ids_by_range,
        fetch_case_notes,
        fetch_case_responses,
        fetch_mysql_record,
        fetch_note_case_ids,
        fetch_response_case_ids,
        DateBounds,
        MysqlEmailRecord,
    },
    services::mongo,
    state::AppState,
};

#[derive(Debug, Default, Deserialize)]
pub struct DateRangeQuery {
    #[serde(default)]
    fecha_inicio: Option<String>,
    #[serde(default)]
    fecha_fin: Option<String>,
    #[serde(default)]
    num_caso: Option<String>,
}

impl DateRangeQuery {
    fn resolved_bounds(&self) -> Option<DateBounds> {
        let start = self.fecha_inicio.as_ref().and_then(|value| parse_date_only(value))?;
        let end = self.fecha_fin
            .as_ref()
            .and_then(|value| parse_date_only(value))
            .unwrap_or_else(current_date);
        Some(DateBounds { start, end })
    }
}

pub async fn sync_tickets_by_range(
    State(state): State<AppState>,
    Query(range): Query<DateRangeQuery>
) -> Response {
    let Some(bounds) = range.resolved_bounds() else {
        let body = ticket_batch_body(
            "error",
            "Debes proporcionar al menos fecha_inicio".to_string(),
            0,
            Vec::new(),
            Vec::new(),
        );
        return (StatusCode::BAD_REQUEST, body).into_response();
    };

    let total_cases;
    let cases = match fetch_case_ids_by_range(&state, &bounds).await {
        Ok(ids) => ids,
        Err(err) => {
            let err = TicketSyncError::from(err);
            let status = err.status_code();
            let body = ticket_batch_body("error", err.to_string(), 0, Vec::new(), Vec::new());
            return (status, body).into_response();
        }
    };

    total_cases = cases.len();
    if total_cases == 0 {
        let body = ticket_batch_body(
            "ok",
            "No se encontraron casos en el rango solicitado".to_string(),
            0,
            Vec::new(),
            Vec::new(),
        );
        return (StatusCode::OK, body).into_response();
    }

    let db_name = env::var("MONGO_DB_NAME").unwrap_or_else(|_| "correos_exchange_queretaro".into());
    let (cases, already_present) = match mongo::filter_pending_cases(&state, &db_name, cases).await {
        Ok(result) => result,
        Err(err) => {
            let err = TicketSyncError::Mongo(err);
            let status = err.status_code();
            let body = ticket_batch_body("error", err.to_string(), 0, Vec::new(), Vec::new());
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
        let body = ticket_batch_body(
            "ok",
            format!(
                "Los {} casos encontrados ya estaban sincronizados en MongoDB",
                total_cases
            ),
            0,
            Vec::new(),
            Vec::new(),
        );
        return (StatusCode::OK, body).into_response();
    }

    let batch_size = case_batch_size();
    let case_parallel = case_parallelism();
    let results = run_batches(
        &state,
        cases,
        batch_size,
        case_parallel,
        "Tickets",
        |record| record.case_id.clone(),
        |state, record| async move { sync_ticket_case(&state, record).await },
    )
    .await;

    let mut successes = Vec::new();
    let mut failures = Vec::new();
    for (case_id, result) in results {
        match result {
            Ok(_) => successes.push(case_id),
            Err(err) => {
                error!("Fallo al sincronizar ticket {}: {}", case_id, err);
                failures.push(format!("{}: {}", case_id, err));
            }
        }
    }

    let processed = successes.len() + failures.len();
    let status = if failures.is_empty() { "ok" } else { "partial" };
    let detail = if failures.is_empty() {
        format!("Se sincronizaron {} casos", processed)
    } else {
        format!("Se sincronizaron {} casos, {} fallaron", successes.len(), failures.len())
    };

    if !failures.is_empty() {
        if let Err(err) = write_errors_file("tickets_sync_errors.txt", &failures).await {
            warn!("No se pudo escribir archivo de errores: {err}");
        }
    }

    let body = ticket_batch_body(status, detail, processed, successes, failures);
    (StatusCode::OK, body).into_response()
}

pub async fn sync_notes_by_range(
    State(state): State<AppState>,
    Query(range): Query<DateRangeQuery>
) -> Response {
    let case_override = range.num_caso
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string());

    let (bounds, case_ids) = if let Some(case_id) = case_override.clone() {
        info!("Notas - Modo num_caso activo: {}", case_id);
        (None, vec![case_id])
    } else {
        let Some(bounds) = range.resolved_bounds() else {
            let body = message_sync_body(
                "error",
                "Debes proporcionar al menos fecha_inicio o num_caso".to_string(),
                0,
                Vec::new(),
                Vec::new(),
            );
            return (StatusCode::BAD_REQUEST, body).into_response();
        };

        let case_ids = match fetch_note_case_ids(&state, &bounds).await {
            Ok(ids) => ids,
            Err(err) => {
                let err = TicketSyncError::from(err);
                let status = err.status_code();
                let body = message_sync_body("error", err.to_string(), 0, Vec::new(), Vec::new());
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

    info!("Notas - Casos antes de filtrar en MongoDB: {}", case_ids.len());

    if case_ids.is_empty() {
        let detail = if let Some(case_id) = case_override {
            format!("No se encontraron notas para el caso {}", case_id)
        } else {
            "No se encontraron notas en el rango solicitado".to_string()
        };
        let body = message_sync_body("ok", detail, 0, Vec::new(), Vec::new());
        return (StatusCode::OK, body).into_response();
    }

    let db_name = env::var("MONGO_DB_NAME").unwrap_or_else(|_| "correos_exchange_queretaro".into());
    let (case_ids, already_present) = match
        mongo::filter_pending_case_ids_by_type(&state, &db_name, case_ids, "nota").await
    {
        Ok(result) => result,
        Err(err) => {
            let err = TicketSyncError::Mongo(err);
            let status = err.status_code();
            let body = message_sync_body("error", err.to_string(), 0, Vec::new(), Vec::new());
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
        let body = message_sync_body(
            "ok",
            "Todos los casos de notas ya existen en MongoDB".to_string(),
            0,
            Vec::new(),
            Vec::new(),
        );
        return (StatusCode::OK, body).into_response();
    }

    let batch_size = case_batch_size();
    let case_parallel = case_parallelism();
    let bounds = bounds.clone();
    let results = run_batches(
        &state,
        case_ids,
        batch_size,
        case_parallel,
        "Notas",
        |case_id| case_id.clone(),
        move |state, case_id| {
            let bounds = bounds.clone();
            async move { sync_notes_for_case(&state, &case_id, bounds.as_ref()).await }
        },
    )
    .await;

    let mut total_inserted = 0;
    let mut mime_ids = Vec::new();
    let mut msg_struct_ids = Vec::new();
    let mut failures = Vec::new();
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

    let status = if failures.is_empty() { "ok" } else { "partial" };
    let detail = if failures.is_empty() {
        format!("Se migraron {} notas", total_inserted)
    } else {
        format!("Se migraron {} notas; {} casos fallaron", total_inserted, failures.len())
    };

    if !failures.is_empty() {
        if let Err(err) = write_errors_file("tickets_notes_errors.txt", &failures).await {
            warn!("No se pudo escribir archivo de errores: {err}");
        }
    }

    let body = message_sync_body(status, detail, total_inserted, mime_ids, msg_struct_ids);
    (StatusCode::OK, body).into_response()
}

pub async fn sync_responses_by_range(
    State(state): State<AppState>,
    Query(range): Query<DateRangeQuery>
) -> Response {
    let case_override = range.num_caso
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string());

    let (bounds, case_ids) = if let Some(case_id) = case_override.clone() {
        info!("Respuestas - Modo num_caso activo: {}", case_id);
        (None, vec![case_id])
    } else {
        let Some(bounds) = range.resolved_bounds() else {
            let body = message_sync_body(
                "error",
                "Debes proporcionar al menos fecha_inicio o num_caso".to_string(),
                0,
                Vec::new(),
                Vec::new(),
            );
            return (StatusCode::BAD_REQUEST, body).into_response();
        };

        let case_ids = match fetch_response_case_ids(&state, &bounds).await {
            Ok(ids) => ids,
            Err(err) => {
                let err = TicketSyncError::from(err);
                let status = err.status_code();
                let body = message_sync_body("error", err.to_string(), 0, Vec::new(), Vec::new());
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

    info!("Respuestas - Casos antes de filtrar en MongoDB: {}", case_ids.len());

    if case_ids.is_empty() {
        let detail = if let Some(case_id) = case_override {
            format!("No se encontraron respuestas para el caso {}", case_id)
        } else {
            "No se encontraron respuestas en el rango solicitado".to_string()
        };
        let body = message_sync_body("ok", detail, 0, Vec::new(), Vec::new());
        return (StatusCode::OK, body).into_response();
    }

    let db_name = env::var("MONGO_DB_NAME").unwrap_or_else(|_| "correos_exchange_queretaro".into());
    let (case_ids, already_present) = match
        mongo::filter_pending_case_ids_by_type(&state, &db_name, case_ids, "respuesta").await
    {
        Ok(result) => result,
        Err(err) => {
            let err = TicketSyncError::Mongo(err);
            let status = err.status_code();
            let body = message_sync_body("error", err.to_string(), 0, Vec::new(), Vec::new());
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
        let body = message_sync_body(
            "ok",
            "Todos los casos de respuestas ya existen en MongoDB".to_string(),
            0,
            Vec::new(),
            Vec::new(),
        );
        return (StatusCode::OK, body).into_response();
    }

    let batch_size = case_batch_size();
    let case_parallel = case_parallelism();
    let bounds = bounds.clone();
    let results = run_batches(
        &state,
        case_ids,
        batch_size,
        case_parallel,
        "Respuestas",
        |case_id| case_id.clone(),
        move |state, case_id| {
            let bounds = bounds.clone();
            async move { sync_responses_for_case(&state, &case_id, bounds.as_ref()).await }
        },
    )
    .await;

    let mut total_inserted = 0;
    let mut mime_ids = Vec::new();
    let mut msg_struct_ids = Vec::new();
    let mut failures = Vec::new();
    for (case_id, result) in results {
        match result {
            Ok(summary) => {
                total_inserted += summary.inserted;
                mime_ids.extend(summary.mime_ids);
                msg_struct_ids.extend(summary.msg_struct_ids);
            }
            Err(err) => {
                error!("Fallo al sincronizar respuestas del ticket {}: {}", case_id, err);
                failures.push(format!("{}: {}", case_id, err));
            }
        }
    }

    let status = if failures.is_empty() { "ok" } else { "partial" };
    let detail = if failures.is_empty() {
        format!("Se migraron {} respuestas", total_inserted)
    } else {
        format!("Se migraron {} respuestas; {} casos fallaron", total_inserted, failures.len())
    };

    if !failures.is_empty() {
        if let Err(err) = write_errors_file("tickets_responses_errors.txt", &failures).await {
            warn!("No se pudo escribir archivo de errores: {err}");
        }
    }

    let body = message_sync_body(status, detail, total_inserted, mime_ids, msg_struct_ids);
    (StatusCode::OK, body).into_response()
}

async fn sync_ticket_case(
    state: &AppState,
    mysql_record: MysqlEmailRecord
) -> Result<TicketSyncResponse, TicketSyncError> {
    let case_id = mysql_record.case_id.clone();
    let db_name = env::var("MONGO_DB_NAME").unwrap_or_else(|_| "correos_exchange_queretaro".into());
    let config_collection_name = env
        ::var("MONGO_CONFIG_COLLECTION")
        .unwrap_or_else(|_| "configuration".into());

    if mongo::case_already_synced(state, &db_name, &case_id).await? {
        return Ok(TicketSyncResponse {
            status: "skipped".to_string(),
            detail: format!("El caso {case_id} ya existe en MongoDB"),
            uploaded: Vec::new(),
            mongo_id: None,
            msg_struct_id: None,
        });
    }

    let config_email = mysql_record.config_email
        .clone()
        .ok_or_else(|| TicketSyncError::MissingConfigurationEmail(case_id.to_string()))?;
    let Some(configuration_id) = mongo::find_configuration_id(
        state,
        &db_name,
        &config_collection_name,
        &config_email
    ).await? else {
        return Err(TicketSyncError::ConfigurationNotFound(config_email));
    };

    let sanitized_case = sanitize_segment(&case_id);
    let remote_case_path = format!("{} {}", state.ssh_service.base_path(), case_id);

    let tasks = gather_case_files(
        state,
        &remote_case_path,
        &format!("tickets/{}", sanitized_case),
    ).await?;

    if tasks.is_empty() {
        return Err(TicketSyncError::CaseNotFound(case_id.to_string()));
    }

    let (uploaded_urls, mime_files, html_body) = process_case_file_tasks(state, tasks).await?;

    if mime_files.is_empty() {
        return Err(TicketSyncError::CaseNotFound(case_id.to_string()));
    }

    let html_content = select_html_content(
        html_body,
        &mime_files,
        mysql_record.subject.clone().unwrap_or_default(),
        "tickets/sync",
        &case_id
    );

    let mime_document = build_ticket_mime(
        configuration_id,
        mysql_record.subject.clone().unwrap_or_default(),
        html_content,
        mime_files
    );

    let inserted_id = mime_document.id;
    mongo::insert_msg_mime(state, &db_name, &mime_document).await?;

    let msg_struct_doc = build_ticket_struct(
        &mysql_record,
        configuration_id,
        inserted_id,
        &case_id
    );

    mongo::insert_msg_struct(state, &db_name, &msg_struct_doc).await?;

    info!("Ticket {} sincronizado. Archivos subidos: {}", case_id, uploaded_urls.len());

    Ok(TicketSyncResponse {
        status: "ok".to_string(),
        detail: format!("Sincronización completada con {} archivos", uploaded_urls.len()),
        uploaded: uploaded_urls,
        mongo_id: Some(inserted_id.to_hex()),
        msg_struct_id: Some(msg_struct_doc.id.to_hex()),
    })
}

async fn sync_notes_for_case(
    state: &AppState,
    case_id: &str,
    bounds: Option<&DateBounds>
) -> Result<MessageSyncResponse, TicketSyncError> {
    let mysql_record = fetch_mysql_record(state, case_id).await?.ok_or_else(||
        TicketSyncError::CaseNotFound(case_id.to_string())
    )?;
    let db_name = env::var("MONGO_DB_NAME").unwrap_or_else(|_| "correos_exchange_queretaro".into());
    let config_collection_name = env
        ::var("MONGO_CONFIG_COLLECTION")
        .unwrap_or_else(|_| "configuration".into());

    let config_email = mysql_record.config_email
        .clone()
        .ok_or_else(|| TicketSyncError::MissingConfigurationEmail(case_id.to_string()))?;
    let Some(configuration_id) = mongo::find_configuration_id(
        state,
        &db_name,
        &config_collection_name,
        &config_email
    ).await? else {
        return Err(TicketSyncError::ConfigurationNotFound(config_email));
    };

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
        if
            mongo::msg_struct_exists(
                state,
                &db_name,
                doc! { "id_mail": note.id, "message_type": "nota", "num_caso": case_id }
            ).await?
        {
            info!("Nota {} del caso {} ya existe en MongoDB, se omite", note.id, case_id);
            continue;
        }

        let folder = sanitize_segment(&format!("Nota_{}", note.id));
        let prefix = format!("{}/notas/{}", base_prefix, folder);
        let (files, html_body) = build_files_from_s3(state, &prefix).await?;
        let html_content = select_html_content(
            html_body,
            &files,
            format!("Nota {}", note.id),
            "tickets/obtener_correos_notas",
            &case_id
        );
        let summary = note.usuario.clone().unwrap_or_default();

        let mime_document = build_note_mime(configuration_id, summary.clone(), html_content, files);
        let mime_id = mime_document.id;
        mongo::insert_msg_mime(state, &db_name, &mime_document).await?;

        let msg_struct_doc = build_note_struct(&note, configuration_id, mime_id, case_id, &summary);

        mongo::insert_msg_struct(state, &db_name, &msg_struct_doc).await?;

        inserted_mime_ids.push(mime_id.to_hex());
        inserted_struct_ids.push(msg_struct_doc.id.to_hex());
    }

    let inserted = inserted_struct_ids.len();
    info!("Notas caso {}: total {} | insertadas {}", case_id, inserted, inserted);
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
    bounds: Option<&DateBounds>
) -> Result<MessageSyncResponse, TicketSyncError> {
    let mysql_record = fetch_mysql_record(state, case_id).await?.ok_or_else(||
        TicketSyncError::CaseNotFound(case_id.to_string())
    )?;
    let db_name = env::var("MONGO_DB_NAME").unwrap_or_else(|_| "correos_exchange_queretaro".into());
    let config_collection_name = env
        ::var("MONGO_CONFIG_COLLECTION")
        .unwrap_or_else(|_| "configuration".into());

    let config_email = mysql_record.config_email
        .clone()
        .ok_or_else(|| TicketSyncError::MissingConfigurationEmail(case_id.to_string()))?;
    let Some(configuration_id) = mongo::find_configuration_id(
        state,
        &db_name,
        &config_collection_name,
        &config_email
    ).await? else {
        return Err(TicketSyncError::ConfigurationNotFound(config_email));
    };

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

    let sanitized_case = sanitize_segment(case_id);
    let base_prefix = format!("tickets/{}", sanitized_case);

    let mut inserted_mime_ids = Vec::new();
    let mut inserted_struct_ids = Vec::new();
    let mut skipped = 0usize;

    for response in responses {
        if
            mongo::msg_struct_exists(
                state,
                &db_name,
                doc! {
                "id_mail": response.id,
                "message_type": "respuesta",
                "num_caso": case_id
            }
            ).await?
        {
            info!("Respuesta {} del caso {} ya existe en MongoDB, se omite", response.id, case_id);
            skipped += 1;
            continue;
        }

        let folder = sanitize_segment(&format!("Respuesta_Num_-_{}", response.id));
        let prefix = format!("{}/respuestas/{}", base_prefix, folder);
        let (files, html_body) = build_files_from_s3(state, &prefix).await?;
        let subject = response.asunto
            .clone()
            .unwrap_or_else(|| format!("Respuesta {}", response.id));
        let html_content = select_html_content(
            html_body,
            &files,
            subject.clone(),
            "tickets/obtener_correos_respuesta",
            &case_id
        );

        let mime_document = build_response_mime(
            configuration_id,
            subject.clone(),
            html_content,
            files
        );
        let mime_id = mime_document.id;
        mongo::insert_msg_mime(state, &db_name, &mime_document).await?;

        let msg_struct_doc = build_response_struct(
            &response,
            configuration_id,
            mime_id,
            case_id,
            &subject
        );

        mongo::insert_msg_struct(state, &db_name, &msg_struct_doc).await?;

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
        detail: format!("Se migraron {} respuestas para el caso {}", inserted, case_id),
        inserted,
        mime_ids: inserted_mime_ids,
        msg_struct_ids: inserted_struct_ids,
    })
}

async fn write_errors_file(path: &str, failures: &[String]) -> Result<(), std::io::Error> {
    if failures.is_empty() {
        return Ok(());
    }
    let contents = failures.join("\n");
    tokio::fs::write(path, contents).await
}

#[derive(Debug, thiserror::Error)]
enum TicketSyncError {
    #[error("No se encontraron registros para el caso {0}")] CaseNotFound(String),
    #[error(
        "El caso {0} no cuenta con correo de configuración (Corre_Telcel)"
    )] MissingConfigurationEmail(String),
    #[error("No se encontró configuración en MongoDB para el correo {0}")] ConfigurationNotFound(
        String,
    ),
    #[error(transparent)] Mysql(#[from] sea_orm::DbErr),
    #[error(transparent)] Files(#[from] TicketsFilesError),
    #[error(transparent)] Mongo(#[from] mongodb::error::Error),
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
