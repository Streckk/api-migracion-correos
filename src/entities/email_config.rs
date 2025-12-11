use mongodb::bson::oid::ObjectId;
use serde::{ Deserialize, Serialize };

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailConfigDocument {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    pub name_session: String,
    pub email: String,
    pub outbound_email: String,
    pub incoming_server: String,
    pub outbound_server: String,
    pub imap_server: String,
    pub password: String,
    pub port: i32,
    pub port_outbound: i32,
    #[serde(rename = "isBlockSendEmail")]
    pub is_block_send_email: bool,
    pub tls: bool,
    pub client_id: String,
    pub tenant_id: String,
    pub client_secret_id: String,
    pub access_token: String,
}
