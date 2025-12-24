use sea_orm::JsonValue;
use serde::Serialize;

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub service: String,
}

#[derive(Serialize)]
pub struct DbCheckResponse {
    pub status: String,
    pub detail: String,
}

#[derive(Serialize)]
pub struct S3CheckResponse {
    pub status: String,
    pub detail: String,
}

#[derive(Serialize)]
pub struct MailConfigListResponse {
    pub status: String,
    pub rows: Vec<JsonValue>,
    pub detail: Option<String>,
}

#[derive(Serialize)]
pub struct IncomingMailResponse {
    pub status: String,
    pub rows: Vec<JsonValue>,
    pub detail: Option<String>,
}

#[derive(Serialize)]
pub struct TicketSyncResponse {
    pub status: String,
    pub detail: String,
    pub uploaded: Vec<String>,
    pub mongo_id: Option<String>,
    pub msg_struct_id: Option<String>,
}

#[derive(Serialize)]
pub struct TicketBatchResponse {
    pub status: String,
    pub detail: String,
    pub processed: usize,
    pub successes: Vec<String>,
    pub failures: Vec<String>,
}

#[derive(Serialize)]
pub struct MessageSyncResponse {
    pub status: String,
    pub detail: String,
    pub inserted: usize,
    pub mime_ids: Vec<String>,
    pub msg_struct_ids: Vec<String>,
}

#[derive(Serialize)]
pub struct UserSyncResponse {
    pub status: String,
    pub detail: String,
    pub processed: usize,
    pub inserted: usize,
    pub skipped: usize,
    pub errors: usize,
    pub skipped_missing_reference: usize,
    pub skipped_duplicate: usize,
    pub missing_references: Vec<String>,
}

#[derive(Serialize)]
pub struct MongoCheckResponse {
    pub status: String,
    pub detail: String,
}

#[derive(Serialize)]
pub struct MongoSetupResponse {
    pub status: String,
    pub detail: String,
}

#[derive(Serialize)]
pub struct MongoSyncResponse {
    pub status: String,
    pub detail: String,
}
