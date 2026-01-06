use futures::stream::{self, StreamExt};
use tracing::info;

use crate::state::AppState;

const DEFAULT_CASE_BATCH_SIZE: usize = 10;
const DEFAULT_CASE_PARALLELISM: usize = 2;

pub fn case_batch_size() -> usize {
    std::env::var("CASE_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_CASE_BATCH_SIZE)
}

pub fn case_parallelism() -> usize {
    std::env::var("CASE_PARALLELISM")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_CASE_PARALLELISM)
}

pub async fn run_batches<T, F, Fut, R, E, IdFn>(
    state: &AppState,
    items: Vec<T>,
    batch_size: usize,
    parallelism: usize,
    label: &str,
    id_fn: IdFn,
    sync_fn: F,
) -> Vec<(String, Result<R, E>)>
where
    T: Send + 'static,
    F: Fn(AppState, T) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<R, E>> + Send,
    IdFn: Fn(&T) -> String + Send + Sync,
    R: Send + 'static,
    E: Send + 'static,
{
    if items.is_empty() {
        return Vec::new();
    }

    let total_batches = (items.len() + batch_size - 1) / batch_size;
    info!(
        "{} - Se procesarán {} elemento(s) en {} lote(s) de hasta {} elementos",
        label,
        items.len(),
        total_batches,
        batch_size
    );

    let mut results = Vec::with_capacity(items.len());
    let mut iter = items.into_iter();
    let mut current_batch = 0usize;
    let id_fn = &id_fn;
    let sync_fn = &sync_fn;

    loop {
        let batch: Vec<T> = iter.by_ref().take(batch_size).collect();
        if batch.is_empty() {
            break;
        }

        current_batch += 1;
        info!(
            "{} - Iniciando lote {}/{} con {} elemento(s)",
            label,
            current_batch,
            total_batches,
            batch.len()
        );

        let batch_results = stream::iter(batch.into_iter().map(|item| {
            let state = state.clone();
            let item_id = id_fn(&item);
            async move {
                let result = sync_fn(state, item).await;
                (item_id, result)
            }
        }))
        .buffer_unordered(parallelism)
        .collect::<Vec<_>>()
        .await;

        results.extend(batch_results);

        info!("{} - Lote {}/{} finalizado", label, current_batch, total_batches);
    }

    results
}
