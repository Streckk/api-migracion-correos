use argon2::{
    password_hash::{PasswordHasher, SaltString},
    Algorithm, Argon2, Params, Version,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use mongodb::bson::{oid::ObjectId, DateTime as BsonDateTime};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MysqlUser {
    pub usuario: String,
    pub password: String,
    pub nombre: String,
    pub numero_empleado: String,
    pub turno: String,
    pub campana: String,
    pub departamento: String,
    pub supervisor: String,
    pub imagen_fondo: String,
    pub fecha_password: Option<NaiveDateTime>,
    pub fecha_creacion: Option<NaiveDateTime>,
    pub fecha_conexion: Option<NaiveDateTime>,
    pub estatus: i32,
    pub id_confg: i32,
    pub actividad: i32,
    pub agendado_ticket: Option<i32>,
    pub eliminacion_casos: Option<i32>,
    pub resolver_casos: Option<i32>,
    pub creacion_casos: Option<i32>,
    pub correo_salida: Option<i32>,
    pub graficas: Option<i32>,
    pub publicaciones_favoritas: Option<String>,
    pub extension: String,
}

#[derive(Debug, Clone)]
pub struct NormalizedUser {
    pub username: String,
    pub password: String,
    pub first_names: String,
    pub last_names: String,
    pub campaign_name: String,
    pub department_name: String,
    pub campaign_key: String,
    pub department_key: String,
    pub created_at: Option<NaiveDateTime>,
    pub last_connection: Option<NaiveDateTime>,
    pub status: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MongoUserDocument {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    #[serde(rename = "cu_user")]
    pub user: String,
    #[serde(rename = "cu_password")]
    pub password: String,
    #[serde(rename = "cu_first_names")]
    pub first_names: String,
    #[serde(rename = "cu_last_names")]
    pub last_names: String,
    #[serde(rename = "cu_branch_office")]
    pub branch_office_ids: Vec<String>,
    #[serde(rename = "cu_client")]
    pub client_ids: Vec<String>,
    #[serde(rename = "cu_campaign")]
    pub campaign_ids: Vec<String>,
    #[serde(rename = "cu_department")]
    pub department_id: String,
    #[serde(rename = "cu_gender")]
    pub gender: String,
    #[serde(rename = "cu_date_birthday")]
    pub date_birthday: Option<DateTime<Utc>>,
    #[serde(rename = "cu_id_confg")]
    pub config_id: Option<ObjectId>,
    #[serde(rename = "cu_status")]
    pub status: i32,
    #[serde(rename = "cu_delete")]
    pub deleted: i32,
    #[serde(rename = "cu_date_registration")]
    pub date_registration: Option<BsonDateTime>,
    #[serde(rename = "cu_date_change_password")]
    pub date_change_password: Option<DateTime<Utc>>,
    #[serde(rename = "cu_date_last_connection")]
    pub date_last_connection: Option<BsonDateTime>,
    #[serde(rename = "cu_color")]
    pub color: String,
    pub settings: UserSettingsDocument,
    #[serde(rename = "cu_time_server")]
    pub time_server: Option<DateTime<Utc>>,
    #[serde(rename = "cu_pauses_list")]
    pub pauses_list: Vec<String>,
    #[serde(rename = "cu_security_questions")]
    pub security_questions: String,
}

impl Default for MongoUserDocument {
    fn default() -> Self {
        Self {
            id: None,
            user: String::new(),
            password: String::new(),
            first_names: String::new(),
            last_names: String::new(),
            branch_office_ids: Vec::new(),
            client_ids: Vec::new(),
            campaign_ids: Vec::new(),
            department_id: String::new(),
            gender: String::new(),
            date_birthday: None,
            config_id: None,
            status: 0,
            deleted: 0,
            date_registration: None,
            date_change_password: None,
            date_last_connection: None,
            color: String::new(),
            settings: UserSettingsDocument::default(),
            time_server: None,
            pauses_list: Vec::new(),
            security_questions: "{}".to_string(),
        }
    }
}

pub fn hash_password_argon2id(plain: &str) -> Result<String, String> {
    let params =
        Params::new(4096, 3, 1, None).map_err(|err| format!("Params inválidos: {err}"))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let salt = SaltString::generate(&mut OsRng);

    argon2
        .hash_password(plain.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|err| format!("No se pudo hashear la contraseña: {err}"))
}

pub fn split_name(nombre: &str) -> (String, String) {
    let mut iter = nombre.split('|').map(|part| part.trim());
    let first = iter.next().unwrap_or("").trim();
    let rest = iter.collect::<Vec<_>>();
    let last = rest.join(" ").trim().to_string();

    (first.to_string(), last)
}

pub fn normalize_campaign_name(name: &str, aliases: &HashMap<String, String>) -> String {
    let normalized = normalize_lookup_key(name);
    aliases
        .get(&normalized)
        .cloned()
        .unwrap_or(normalized)
}

pub fn normalize_key(value: &str) -> String {
    let trimmed = value.trim();
    let mut normalized = String::with_capacity(trimmed.len());
    let mut prev_space = false;

    for ch in trimmed.chars() {
        if ch.is_whitespace() {
            if !prev_space && !normalized.is_empty() {
                normalized.push(' ');
            }
            prev_space = true;
            continue;
        }

        let mapped = match ch {
            'á' | 'à' | 'ä' | 'â' => 'a',
            'é' | 'è' | 'ë' | 'ê' => 'e',
            'í' | 'ì' | 'ï' | 'î' => 'i',
            'ó' | 'ò' | 'ö' | 'ô' => 'o',
            'ú' | 'ù' | 'ü' | 'û' => 'u',
            'Á' | 'À' | 'Ä' | 'Â' => 'A',
            'É' | 'È' | 'Ë' | 'Ê' => 'E',
            'Í' | 'Ì' | 'Ï' | 'Î' => 'I',
            'Ó' | 'Ò' | 'Ö' | 'Ô' => 'O',
            'Ú' | 'Ù' | 'Ü' | 'Û' => 'U',
            'ñ' => 'n',
            'Ñ' => 'N',
            'ç' => 'c',
            'Ç' => 'C',
            _ => ch,
        };

        normalized.push(mapped);
        prev_space = false;
    }

    if normalized.ends_with(' ') {
        normalized.pop();
    }

    normalized
}

pub fn normalize_lookup_key(value: &str) -> String {
    normalize_key(value).to_lowercase()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserSettingsDocument {
    #[serde(rename = "cc_correos")]
    pub cc_emails: bool,
    #[serde(rename = "cc_whatsapp")]
    pub cc_whatsapp: bool,
    #[serde(rename = "cc_redes_sociales")]
    pub cc_social_networks: bool,
    #[serde(rename = "cc_ticket")]
    pub cc_ticket: bool,
    #[serde(rename = "cc_formularios")]
    pub cc_forms: bool,
    #[serde(rename = "cc_formularios_aeromar")]
    pub cc_forms_aeromar: bool,
    #[serde(rename = "cc_informes")]
    pub cc_reports: bool,
    #[serde(rename = "cc_graficas")]
    pub cc_charts: bool,
    #[serde(rename = "cc_calidad")]
    pub cc_quality: bool,
    #[serde(rename = "cc_directorio")]
    pub cc_directory: bool,
    #[serde(rename = "cc_difusion")]
    pub cc_broadcast: bool,
    #[serde(rename = "cc_grabaciones")]
    pub cc_recordings: bool,
    #[serde(rename = "cc_capacitacion")]
    pub cc_training: bool,
    #[serde(rename = "cc_reclutamiento")]
    pub cc_recruitment: bool,
    #[serde(rename = "cc_administracion")]
    pub cc_administration: bool,
    #[serde(rename = "cc_aditional")]
    pub cc_additional: String,
    #[serde(rename = "cc_date_registration")]
    pub cc_date_registration: Option<DateTime<Utc>>,
    #[serde(rename = "_id")]
    pub id: Option<ObjectId>,
}

impl Default for UserSettingsDocument {
    fn default() -> Self {
        Self {
            cc_emails: false,
            cc_whatsapp: false,
            cc_social_networks: false,
            cc_ticket: true,
            cc_forms: false,
            cc_forms_aeromar: false,
            cc_reports: false,
            cc_charts: false,
            cc_quality: false,
            cc_directory: false,
            cc_broadcast: false,
            cc_recordings: false,
            cc_training: false,
            cc_recruitment: false,
            cc_administration: false,
            cc_additional: "{}".to_string(),
            cc_date_registration: None,
            id: None,
        }
    }
}
