use std::{
    collections::{HashMap, HashSet},
    env,
};

use futures::stream::TryStreamExt;
use mongodb::{
    bson::{doc, oid::ObjectId, Bson, Document},
    Collection,
};

use crate::{
    entities::{
        email_config::EmailConfigDocument,
        msg_mime::MsgMimeDocument,
        msg_struct::MsgStructDocument,
        user_migration::{normalize_lookup_key, MongoUserDocument, MysqlUser, UserSettingsDocument},
    },
    services::mysql::MysqlEmailRecord,
    state::AppState,
};

#[derive(Debug, Clone)]
pub struct BranchOfficeRef {
    pub branch_office_id: ObjectId,
    pub client_id: String,
    pub campaign_id: String,
    pub department_id: String,
}

#[derive(Debug, Default)]
pub struct BranchOfficeRefs {
    pub by_campaign_department: HashMap<String, BranchOfficeRef>,
    pub by_campaign: HashMap<String, BranchOfficeRef>,
}

#[derive(Debug)]
pub enum UpsertResult {
    Inserted,
    Existing,
}

pub async fn filter_pending_cases(
    state: &AppState,
    db_name: &str,
    cases: Vec<MysqlEmailRecord>,
) -> Result<(Vec<MysqlEmailRecord>, usize), mongodb::error::Error> {
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

pub async fn filter_pending_case_ids_by_type(
    state: &AppState,
    db_name: &str,
    case_ids: Vec<String>,
    message_type: &str,
) -> Result<(Vec<String>, usize), mongodb::error::Error> {
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

pub async fn case_already_synced(
    state: &AppState,
    db_name: &str,
    case_id: &str,
) -> Result<bool, mongodb::error::Error> {
    let collection = state
        .mongo
        .database(db_name)
        .collection::<MsgStructDocument>("msg-struct");
    Ok(collection
        .find_one(doc! { "num_caso": case_id })
        .await?
        .is_some())
}

pub async fn find_configuration_id(
    state: &AppState,
    db_name: &str,
    collection_name: &str,
    email: &str,
) -> Result<Option<ObjectId>, mongodb::error::Error> {
    let collection = state
        .mongo
        .database(db_name)
        .collection::<EmailConfigDocument>(collection_name);
    let document = collection
        .find_one(doc! { "incoming_email": email })
        .await?;
    Ok(document.map(|config| config.id))
}

pub async fn insert_msg_mime(
    state: &AppState,
    db_name: &str,
    document: &MsgMimeDocument,
) -> Result<(), mongodb::error::Error> {
    let collection = state
        .mongo
        .database(db_name)
        .collection::<MsgMimeDocument>("msg-mime");
    collection.insert_one(document).await?;
    Ok(())
}

pub async fn insert_msg_struct(
    state: &AppState,
    db_name: &str,
    document: &MsgStructDocument,
) -> Result<(), mongodb::error::Error> {
    let collection = state
        .mongo
        .database(db_name)
        .collection::<MsgStructDocument>("msg-struct");
    collection.insert_one(document).await?;
    Ok(())
}

pub async fn msg_struct_exists(
    state: &AppState,
    db_name: &str,
    filter: Document,
) -> Result<bool, mongodb::error::Error> {
    let collection = state
        .mongo
        .database(db_name)
        .collection::<MsgStructDocument>("msg-struct");
    Ok(collection.find_one(filter).await?.is_some())
}

pub async fn filter_pending_users(
    collection: &Collection<Document>,
    users: Vec<MysqlUser>,
) -> Result<(Vec<MysqlUser>, usize, Vec<String>), mongodb::error::Error> {
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

pub async fn load_branch_office_refs(
    state: &AppState,
    db_name: &str,
) -> Result<BranchOfficeRefs, mongodb::error::Error> {
    let branch_db_name = env::var("MONGO_BRANCH_OFFICE_DB").unwrap_or_else(|_| db_name.to_string());
    let collection_name = env::var("MONGO_BRANCH_OFFICE_COLLECTION")
        .unwrap_or_else(|_| "crm-branch-office".into());
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
            let campaigns =
                get_doc_array(&client, &["list_campaigns", "campaigns", "campanas", "campañas"]);
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
                let departments =
                    get_doc_array(&campaign, &["list_departments", "departments", "departamentos"]);
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

pub async fn insert_user_settings(
    collection: &Collection<UserSettingsDocument>,
    settings: &UserSettingsDocument,
) -> Result<Option<ObjectId>, mongodb::error::Error> {
    let result = collection.insert_one(settings).await?;
    Ok(result.inserted_id.as_object_id())
}

pub async fn upsert_mongo_user(
    collection: &Collection<MongoUserDocument>,
    user: &MongoUserDocument,
) -> Result<UpsertResult, mongodb::error::Error> {
    match collection.insert_one(user).await {
        Ok(_) => Ok(UpsertResult::Inserted),
        Err(err) => {
            if is_duplicate_key_error(&err) {
                Ok(UpsertResult::Existing)
            } else {
                Err(err)
            }
        }
    }
}

fn branch_office_filter_id() -> Option<ObjectId> {
    env::var("MONGO_BRANCH_OFFICE_ID")
        .ok()
        .and_then(|raw| ObjectId::parse_str(raw.trim()).ok())
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

fn is_duplicate_key_error(err: &mongodb::error::Error) -> bool {
    let message = err.to_string();
    message.contains("E11000") || message.contains("duplicate key")
}
