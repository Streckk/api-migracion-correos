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
    pub from: Option<MsgContact>,
    pub to: Vec<MsgContact>,
    pub cc: Vec<MsgContact>,
    pub subject: Option<String>,
    pub conversation: Option<String>,
    #[serde(rename = "num_caso")]
    pub num_caso: Option<String>,
    #[serde(rename = "Fechas_estatus")]
    pub fechas_estatus: Option<DateTime>,
    #[serde(rename = "Nombre_cliente")]
    pub nombre_cliente: Option<String>,
    #[serde(rename = "Estatus")]
    pub estatus: Option<String>,
    #[serde(rename = "Agente_asignado")]
    pub agente_asignado: Option<String>,
    #[serde(rename = "Categoria")]
    pub categoria: Option<String>,
    #[serde(rename = "Subcategoria")]
    pub subcategoria: Option<String>,
    #[serde(rename = "Fecha_cerrada")]
    pub fecha_cerrada: Option<DateTime>,
    #[serde(rename = "Fecha_cliente")]
    pub fecha_cliente: Option<DateTime>,
    #[serde(rename = "Numero_lineas")]
    pub numero_lineas: Option<String>,
    #[serde(rename = "Lista_caso")]
    pub lista_caso: Option<String>,
}
