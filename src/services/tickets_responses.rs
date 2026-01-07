use axum::Json;

use crate::routes::structs::{MessageSyncResponse, TicketBatchResponse};

pub fn ticket_batch_body(
    status: &str,
    detail: String,
    processed: usize,
    successes: Vec<String>,
    failures: Vec<String>,
) -> Json<TicketBatchResponse> {
    Json(TicketBatchResponse {
        status: status.to_string(),
        detail,
        processed,
        successes,
        failures,
    })
}

pub fn message_sync_body(
    status: &str,
    detail: String,
    inserted: usize,
    mime_ids: Vec<String>,
    msg_struct_ids: Vec<String>,
) -> Json<MessageSyncResponse> {
    Json(MessageSyncResponse {
        status: status.to_string(),
        detail,
        inserted,
        mime_ids,
        msg_struct_ids,
    })
}
