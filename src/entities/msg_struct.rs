use mongodb::bson::{oid::ObjectId, DateTime};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MsgContact {
    pub name: Option<String>,
    pub email: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MsgStructDocument {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    #[serde(rename = "id_mail")]
    pub id_mail: i64,
    #[serde(rename = "_id_mime")]
    pub mime_id: ObjectId,
    #[serde(rename = "date_creation")]
    pub date_creation: Option<DateTime>,
    #[serde(rename = "date_buzon")]
    pub date_buzon: Option<DateTime>,
    #[serde(rename = "date_meeting")]
    pub date_meeting: Option<DateTime>,
    pub from: Option<MsgContact>,
    pub to: Vec<MsgContact>,
    pub cc: Vec<MsgContact>,
    pub subject: Option<String>,
    pub conversation: Option<String>,
    pub meeting: Option<String>,
    pub viewed: bool,
    pub remove: bool,
    pub priority: Option<String>,
    pub spam: bool,
    pub file: bool,
    pub me: bool,
    pub category: Option<String>,
    pub folder: Option<String>,
}
