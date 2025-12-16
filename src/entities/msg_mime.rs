use mongodb::bson::oid::ObjectId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MsgMimeFile {
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(rename = "fileImageHTML")]
    pub file_image_html: bool,
    #[serde(rename = "fileName")]
    pub file_name: String,
    #[serde(rename = "fileType")]
    pub file_type: String,
    #[serde(rename = "fileSize")]
    pub file_size: String,
    #[serde(rename = "fileUrl")]
    pub file_url: String,
    #[serde(rename = "isFileLocal")]
    pub is_file_local: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MsgMimeDocument {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    #[serde(rename = "_id_configuration")]
    pub configuration_id: ObjectId,
    pub text: String,
    #[serde(rename = "fileMime")]
    pub file_mime: String,
    pub html: String,
    #[serde(rename = "isFileLocal")]
    pub is_file_local: bool,
    pub files: Vec<MsgMimeFile>,
}
