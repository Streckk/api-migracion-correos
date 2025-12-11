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
