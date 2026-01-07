use chrono::{NaiveDate, NaiveDateTime};
use sea_orm::{ConnectionTrait, DbBackend, Statement, Value};

use crate::state::AppState;

#[derive(Clone)]
pub struct DateBounds {
    pub start: NaiveDate,
    pub end: NaiveDate,
}

pub struct MysqlEmailRecord {
    pub case_id: String,
    pub subject: Option<String>,
    pub from_email: Option<String>,
    pub from_name: Option<String>,
    pub to_emails: Vec<String>,
    pub cc_emails: Vec<String>,
    pub id_mail: Option<i64>,
    pub config_email: Option<String>,
    pub fecha_buzon: Option<String>,
    pub fechas_estatus: Option<String>,
    pub nombre_cliente: Option<String>,
    pub estatus: Option<String>,
    pub agente_asignado: Option<String>,
    pub categoria: Option<String>,
    pub subcategoria: Option<String>,
    pub fecha_cerrada: Option<String>,
    pub fecha_cliente: Option<String>,
    pub numero_lineas: Option<String>,
    pub lista_caso: Option<String>,
    pub fecha_de_registro: Option<String>,
}

pub struct MysqlNoteRecord {
    pub id: i64,
    pub usuario: Option<String>,
    pub fecha_registro: Option<String>,
}

pub struct MysqlResponseRecord {
    pub id: i64,
    pub correos_to: Vec<String>,
    pub correos_cc: Vec<String>,
    pub asunto: Option<String>,
    pub fecha_registro: Option<String>,
    pub fecha_cliente: Option<String>,
}

pub struct MysqlOutgoingRecord {
    pub id: i64,
    pub fecha_registro: Option<String>,
    pub usuario: Option<String>,
    pub correo_remitente: Option<String>,
    pub correo_destinatario: Option<String>,
    pub correo_para: Vec<String>,
    pub correo_cc: Vec<String>,
    pub correo_cco: Vec<String>,
    pub asunto: Option<String>,
    pub mensaje_txt: Option<String>,
    pub num_caso: Option<String>,
    pub estatus: Option<String>,
}

pub async fn fetch_case_ids_by_range(
    state: &AppState,
    bounds: &DateBounds,
) -> Result<Vec<MysqlEmailRecord>, sea_orm::DbErr> {
    fetch_case_records_by_range(state, bounds).await
}

pub async fn fetch_note_case_ids(
    state: &AppState,
    bounds: &DateBounds,
) -> Result<Vec<String>, sea_orm::DbErr> {
    fetch_distinct_case_ids(state, "correos_notas", "Fecha_de_Registro", bounds).await
}

pub async fn fetch_response_case_ids(
    state: &AppState,
    bounds: &DateBounds,
) -> Result<Vec<String>, sea_orm::DbErr> {
    fetch_distinct_case_ids(state, "correos_respuesta", "Fecha_de_Registro", bounds).await
}

pub async fn fetch_outgoing_case_ids(
    state: &AppState,
    bounds: &DateBounds,
) -> Result<Vec<String>, sea_orm::DbErr> {
    fetch_distinct_case_ids(state, "correos_salida", "Fecha_de_Registro", bounds).await
}

pub async fn fetch_mysql_record(
    state: &AppState,
    case_id: &str,
) -> Result<Option<MysqlEmailRecord>, sea_orm::DbErr> {
    let statement = Statement::from_sql_and_values(
        DbBackend::MySql,
        "SELECT * FROM correos_entrada WHERE Num_Caso = ? ORDER BY Fecha_de_Registro ASC LIMIT 1",
        vec![case_id.to_string().into()],
    );
    let rows = state.mysql.query_all_raw(statement).await?;
    let row = rows.into_iter().next();
    Ok(row.map(|row| build_mysql_record(&row, Some(case_id.to_string()))))
}

pub async fn fetch_case_records_by_range(
    state: &AppState,
    bounds: &DateBounds,
) -> Result<Vec<MysqlEmailRecord>, sea_orm::DbErr> {
    let mut sql =
        "SELECT * FROM correos_entrada WHERE Fecha_de_Registro >= ? AND Fecha_de_Registro < ?"
            .to_string();
    let values = vec![
        bounds.start.to_string().into(),
        bounds.end.succ_opt().unwrap_or(bounds.end).to_string().into(),
    ];
    sql.push_str(" ORDER BY Fecha_de_Registro ASC");

    let statement = Statement::from_sql_and_values(DbBackend::MySql, sql, values);
    let rows = state.mysql.query_all_raw(statement).await?;

    let mut records = Vec::new();
    for row in rows {
        records.push(build_mysql_record(&row, None));
    }

    Ok(records)
}

pub async fn fetch_case_notes(
    state: &AppState,
    case_id: &str,
    bounds: Option<&DateBounds>,
) -> Result<Vec<MysqlNoteRecord>, sea_orm::DbErr> {
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
            usuario: get_any_string(&row, &["Usuario", "usuario"]),
            fecha_registro: get_string(&row, "Fecha_de_Registro"),
        });
    }

    Ok(records)
}

pub async fn fetch_case_responses(
    state: &AppState,
    case_id: &str,
    bounds: Option<&DateBounds>,
) -> Result<Vec<MysqlResponseRecord>, sea_orm::DbErr> {
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

pub async fn fetch_case_outgoing(
    state: &AppState,
    case_id: &str,
    bounds: Option<&DateBounds>,
) -> Result<Vec<MysqlOutgoingRecord>, sea_orm::DbErr> {
    let mut sql = "SELECT * FROM correos_salida WHERE Num_Caso = ?".to_string();
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
        let correo_destinatario = get_any_string(&row, &["Correo_Destinatario", "correo_Destinatario"]);
        let correo_para = split_emails(get_any_string(&row, &["correo_Para", "Correo_Para", "Correo_para"]));
        let correo_cc = split_emails(get_any_string(&row, &["correo_CC", "Correo_CC", "Correo_cc"]));
        let correo_cco = split_emails(get_any_string(&row, &["correo_CCO", "Correo_CCO", "Correo_cco"]));

        records.push(MysqlOutgoingRecord {
            id,
            fecha_registro: get_string(&row, "Fecha_de_Registro"),
            usuario: get_string(&row, "Usuario"),
            correo_remitente: get_any_string(&row, &["Correo_Remitente", "correo_Remitente"]),
            correo_destinatario,
            correo_para,
            correo_cc,
            correo_cco,
            asunto: get_string(&row, "Asunto"),
            mensaje_txt: get_any_string(&row, &["Mensaje_txt", "Mensaje_Txt", "mensaje_txt"]),
            num_caso: get_string(&row, "Num_Caso"),
            estatus: get_string(&row, "Estatus"),
        });
    }

    Ok(records)
}

pub async fn fetch_distinct_case_ids(
    state: &AppState,
    table: &str,
    date_column: &str,
    bounds: &DateBounds,
) -> Result<Vec<String>, sea_orm::DbErr> {
    let mut sql = format!("SELECT DISTINCT Num_Caso FROM {table} WHERE Num_Caso IS NOT NULL");
    let mut values = Vec::new();
    append_date_filters(&mut sql, &mut values, date_column, bounds);
    sql.push_str(" ORDER BY Num_Caso ASC");

    let statement = Statement::from_sql_and_values(DbBackend::MySql, sql, values);
    let rows = state.mysql.query_all_raw(statement).await?;

    Ok(rows
        .into_iter()
        .filter_map(|row| get_string(&row, "Num_Caso"))
        .collect())
}

fn append_date_filters(
    sql: &mut String,
    values: &mut Vec<Value>,
    column: &str,
    bounds: &DateBounds,
) {
    sql.push_str(&format!(" AND {column} >= ? AND {column} < ?"));
    values.push(Value::String(Some(
        bounds.start.format("%Y-%m-%d").to_string(),
    )));
    let next_day = bounds.end.succ_opt().unwrap_or(bounds.end);
    values.push(Value::String(Some(
        next_day.format("%Y-%m-%d").to_string(),
    )));
}

fn build_mysql_record(row: &sea_orm::QueryResult, fallback_case_id: Option<String>) -> MysqlEmailRecord {
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

fn get_string(row: &sea_orm::QueryResult, column: &str) -> Option<String> {
    row.try_get::<String>("", column)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| row.try_get::<i64>("", column).ok().map(|value| value.to_string()))
        .or_else(|| {
            row.try_get::<NaiveDateTime>("", column)
                .ok()
                .map(|value| value.format("%Y-%m-%d %H:%M:%S").to_string())
        })
}

fn get_any_string(row: &sea_orm::QueryResult, columns: &[&str]) -> Option<String> {
    columns.iter().find_map(|column| get_string(row, column))
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
