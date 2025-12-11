use mongodb::bson::{oid::ObjectId, DateTime};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MsgTeamsDocument {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    #[serde(rename = "organizerEmail")]
    pub organizer_email: String,
    pub title: String,
    pub invited: Vec<String>,
    pub description: Option<String>,
    #[serde(rename = "startDateTime")]
    pub start_date_time: String,
    #[serde(rename = "endDateTime")]
    pub end_date_time: String,
    #[serde(rename = "joinUrl")]
    pub join_url: String,
    #[serde(rename = "eventId")]
    pub event_id: String,
    #[serde(rename = "createdAt")]
    pub created_at: Option<DateTime>,
    pub expired: bool,
}
