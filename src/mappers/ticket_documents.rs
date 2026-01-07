use chrono::NaiveDate;
use mongodb::bson::oid::ObjectId;
use tracing::warn;

use crate::{
    entities::msg_mime::{MsgMimeDocument, MsgMimeFile},
    entities::msg_struct::{MsgContact, MsgStructDocument},
    services::mysql::{MysqlEmailRecord, MysqlNoteRecord, MysqlOutgoingRecord, MysqlResponseRecord},
};

const MAX_HTML_BYTES: usize = 15 * 1024 * 1024;

pub fn build_ticket_mime(
    configuration_id: ObjectId,
    subject: String,
    html: String,
    files: Vec<MsgMimeFile>,
) -> MsgMimeDocument {
    MsgMimeDocument {
        id: ObjectId::new(),
        configuration_id,
        message_type: "entrada".to_string(),
        text: subject.clone(),
        file_mime: "text/html".to_string(),
        html,
        is_file_local: false,
        files,
    }
}

pub fn build_ticket_struct(
    record: &MysqlEmailRecord,
    configuration_id: ObjectId,
    mime_id: ObjectId,
    case_id: &str,
) -> MsgStructDocument {
    MsgStructDocument {
        id: ObjectId::new(),
        id_mail: record.id_mail.unwrap_or_default(),
        mime_id,
        configuration_id,
        date_creation: parse_mysql_datetime(record.fecha_de_registro.as_deref()),
        date_buzon: parse_mysql_datetime(record.fecha_buzon.as_deref()),
        from: record.from_email.as_ref().map(|email| MsgContact {
            name: record.from_name.clone(),
            email: Some(email.clone()),
        }),
        to: contacts_from_emails(&record.to_emails),
        cc: contacts_from_emails(&record.cc_emails),
        message_type: "entrada".to_string(),
        subject: record.subject.clone(),
        conversation: Some(case_id.to_string()),
        num_caso: Some(case_id.to_string()),
        fechas_estatus: parse_mysql_datetime(record.fechas_estatus.as_deref()),
        nombre_cliente: record.nombre_cliente.clone(),
        estatus: record.estatus.clone(),
        agente_asignado: record.agente_asignado.clone(),
        categoria: record.categoria.clone(),
        subcategoria: record.subcategoria.clone(),
        fecha_cerrada: parse_mysql_datetime(record.fecha_cerrada.as_deref()),
        fecha_cliente: parse_mysql_datetime(record.fecha_cliente.as_deref()),
        numero_lineas: record.numero_lineas.clone(),
        lista_caso: record.lista_caso.clone(),
    }
}

pub fn build_note_mime(
    configuration_id: ObjectId,
    summary: String,
    html: String,
    files: Vec<MsgMimeFile>,
) -> MsgMimeDocument {
    MsgMimeDocument {
        id: ObjectId::new(),
        configuration_id,
        message_type: "nota".to_string(),
        text: summary,
        file_mime: "text/html".to_string(),
        html,
        is_file_local: false,
        files,
    }
}

pub fn build_note_struct(
    record: &MysqlNoteRecord,
    configuration_id: ObjectId,
    mime_id: ObjectId,
    case_id: &str,
    summary: &str,
) -> MsgStructDocument {
    MsgStructDocument {
        id: ObjectId::new(),
        id_mail: record.id,
        mime_id,
        configuration_id,
        date_creation: parse_mysql_datetime(record.fecha_registro.as_deref()),
        date_buzon: None,
        from: None,
        to: Vec::new(),
        cc: Vec::new(),
        message_type: "nota".to_string(),
        subject: if summary.is_empty() {
            None
        } else {
            Some(summary.to_string())
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
    }
}

pub fn build_response_mime(
    configuration_id: ObjectId,
    subject: String,
    html: String,
    files: Vec<MsgMimeFile>,
) -> MsgMimeDocument {
    MsgMimeDocument {
        id: ObjectId::new(),
        configuration_id,
        message_type: "respuesta".to_string(),
        text: subject,
        file_mime: "text/html".to_string(),
        html,
        is_file_local: false,
        files,
    }
}

pub fn build_response_struct(
    record: &MysqlResponseRecord,
    configuration_id: ObjectId,
    mime_id: ObjectId,
    case_id: &str,
    subject: &str,
) -> MsgStructDocument {
    MsgStructDocument {
        id: ObjectId::new(),
        id_mail: record.id,
        mime_id,
        configuration_id,
        date_creation: parse_mysql_datetime(record.fecha_registro.as_deref()),
        date_buzon: None,
        from: None,
        to: contacts_from_emails(&record.correos_to),
        cc: contacts_from_emails(&record.correos_cc),
        message_type: "respuesta".to_string(),
        subject: Some(subject.to_string()),
        conversation: Some(case_id.to_string()),
        num_caso: Some(case_id.to_string()),
        fechas_estatus: None,
        nombre_cliente: None,
        estatus: None,
        agente_asignado: None,
        categoria: None,
        subcategoria: None,
        fecha_cerrada: None,
        fecha_cliente: parse_mysql_datetime(record.fecha_cliente.as_deref()),
        numero_lineas: None,
        lista_caso: None,
    }
}

pub fn build_outgoing_mime(
    configuration_id: ObjectId,
    subject: String,
    html: String,
    files: Vec<MsgMimeFile>,
) -> MsgMimeDocument {
    MsgMimeDocument {
        id: ObjectId::new(),
        configuration_id,
        message_type: "salida".to_string(),
        text: subject,
        file_mime: "text/html".to_string(),
        html,
        is_file_local: false,
        files,
    }
}

pub fn build_outgoing_struct(
    record: &MysqlOutgoingRecord,
    configuration_id: ObjectId,
    mime_id: ObjectId,
    case_id: &str,
    subject: &str,
) -> MsgStructDocument {
    let agente_asignado = record.usuario.clone();
    let mut to_emails = record.correo_para.clone();
    if let Some(dest) = record.correo_destinatario.clone() {
        if !to_emails.contains(&dest) {
            to_emails.push(dest);
        }
    }
    let mut cc_emails = record.correo_cc.clone();
    for email in &record.correo_cco {
        if !cc_emails.contains(email) {
            cc_emails.push(email.clone());
        }
    }

    MsgStructDocument {
        id: ObjectId::new(),
        id_mail: record.id,
        mime_id,
        configuration_id,
        date_creation: parse_mysql_datetime(record.fecha_registro.as_deref()),
        date_buzon: None,
        from: record.correo_remitente.as_ref().map(|email| MsgContact {
            name: agente_asignado.clone(),
            email: Some(email.clone()),
        }),
        to: contacts_from_emails(&to_emails),
        cc: contacts_from_emails(&cc_emails),
        message_type: "salida".to_string(),
        subject: Some(subject.to_string()),
        conversation: Some(case_id.to_string()),
        num_caso: Some(case_id.to_string()),
        fechas_estatus: None,
        nombre_cliente: None,
        estatus: record.estatus.clone(),
        agente_asignado,
        categoria: None,
        subcategoria: None,
        fecha_cerrada: None,
        fecha_cliente: None,
        numero_lineas: None,
        lista_caso: None,
    }
}

pub fn contacts_from_emails(emails: &[String]) -> Vec<MsgContact> {
    emails
        .iter()
        .map(|email| MsgContact {
            name: None,
            email: Some(email.clone()),
        })
        .collect()
}

pub fn select_html_content(
    html_body: Option<String>,
    files: &[MsgMimeFile],
    fallback: String,
    endpoint: &str,
    case_id: &str,
) -> String {
    let html = html_body.filter(|content| !content.is_empty());
    let Some(html) = html else {
        return fallback;
    };

    if html.as_bytes().len() <= MAX_HTML_BYTES {
        return html;
    }

    if let Some(url) = find_index_html_url(files, endpoint) {
        warn!(
            "HTML excede limite ({} bytes). Usando URL en {} para caso {}",
            html.as_bytes().len(),
            endpoint,
            case_id
        );
        return url;
    }

    warn!(
        "HTML excede limite ({} bytes) y no se encontro index.html en {} para caso {}",
        html.as_bytes().len(),
        endpoint,
        case_id
    );
    fallback
}

pub fn parse_mysql_datetime(raw: Option<&str>) -> Option<mongodb::bson::DateTime> {
    const FORMATS: [&str; 3] = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %I:%M:%S%p", "%Y-%m-%d %H:%M"];
    raw.and_then(|value| {
        let trimmed = value.trim();
        let normalized = if
            trimmed
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
        } else if
            trimmed.ends_with("am") ||
            trimmed.ends_with("pm") ||
            trimmed.ends_with("AM") ||
            trimmed.ends_with("PM")
        {
            let (prefix, suffix) = trimmed.split_at(trimmed.len().saturating_sub(2));
            format!("{}{}", prefix, suffix.to_uppercase())
        } else {
            trimmed.to_string()
        };

        FORMATS
            .iter()
            .find_map(|fmt| chrono::NaiveDateTime::parse_from_str(&normalized, fmt).ok())
            .map(|naive| mongodb::bson::DateTime::from_millis(naive.and_utc().timestamp_millis()))
    })
}

pub fn parse_date_only(value: &str) -> Option<NaiveDate> {
    let part = value
        .split(|c| c == ' ' || c == 'T')
        .next()
        .unwrap_or(value)
        .trim();
    NaiveDate::parse_from_str(part, "%Y-%m-%d").ok()
}

pub fn current_date() -> NaiveDate {
    chrono::Local::now().date_naive()
}

pub fn sanitize_segment(value: &str) -> String {
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

fn find_index_html_url(files: &[MsgMimeFile], endpoint: &str) -> Option<String> {
    let preferred = match endpoint {
        "tickets/sync" => "/original/index.html",
        "tickets/obtener_correos_notas" => "/notas/",
        "tickets/obtener_correos_respuesta" => "/respuestas/",
        "tickets/obtener_correos_salida" => "/salidas/",
        _ => "",
    };

    if !preferred.is_empty() {
        if
            let Some(file) = files.iter().find(|file| {
                file.file_name.eq_ignore_ascii_case("index.html")
                    && file.file_url.contains(preferred)
            })
        {
            return Some(file.file_url.clone());
        }
    }

    files
        .iter()
        .find(|file| file.file_name.eq_ignore_ascii_case("index.html"))
        .map(|file| file.file_url.clone())
}
