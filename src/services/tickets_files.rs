use std::{env, sync::Arc};

use futures::stream::{self, StreamExt, TryStreamExt};
use mime_guess::MimeGuess;
use mongodb::bson::oid::ObjectId;
use tracing::{error, info, warn};

use crate::{
    entities::msg_mime::MsgMimeFile,
    mappers::ticket_documents::sanitize_segment,
    ssh::config::RemoteDirEntry,
    ssh::SshError,
    state::AppState,
    storage::StorageError,
};

const DEFAULT_FILE_UPLOAD_CONCURRENCY: usize = 4;

#[derive(Debug, thiserror::Error)]
pub enum TicketsFilesError {
    #[error(transparent)]
    Ssh(#[from] SshError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

#[derive(Clone)]
pub(crate) struct FileTask {
    remote_path: String,
    key: String,
    original_name: String,
    file_mime: String,
    file_size: u64,
    capture_main_html: bool,
}

impl FileTask {
    fn from_entry(entry: RemoteDirEntry, key: String, capture_main_html: bool) -> Self {
        let guess: MimeGuess = MimeGuess::from_path(&entry.name);
        let mime = guess.first_raw().unwrap_or("application/octet-stream").to_string();
        Self {
            remote_path: entry.path,
            key,
            original_name: entry.name,
            file_mime: mime,
            file_size: entry.size,
            capture_main_html,
        }
    }
}

fn file_upload_concurrency() -> usize {
    env::var("FILE_UPLOAD_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_FILE_UPLOAD_CONCURRENCY)
}

pub async fn gather_case_files(
    state: &AppState,
    remote_case_path: &str,
    s3_prefix: &str,
) -> Result<Vec<FileTask>, TicketsFilesError> {
    info!("Listando archivos del caso en ruta remota: {}", remote_case_path);
    let mut tasks = Vec::new();
    gather_case_files_into(state, remote_case_path, s3_prefix, &mut tasks).await?;
    Ok(tasks)
}

async fn gather_case_files_into(
    state: &AppState,
    remote_case_path: &str,
    s3_prefix: &str,
    tasks: &mut Vec<FileTask>,
) -> Result<(), TicketsFilesError> {
    let entries = state
        .ssh_service
        .list_remote_dir(remote_case_path)
        .await
        .map_err(|err| {
            error!("Error al listar directorio remoto {}: {}", remote_case_path, err);
            err
        })?;

    for entry in entries {
        if entry.is_dir {
            match entry.name.to_lowercase().as_str() {
                "archivos" => {
                    gather_simple_dir(
                        state,
                        &entry.path,
                        &format!("{}/attachments/archivos", s3_prefix),
                        tasks,
                    )
                    .await?;
                }
                "imagenes" => {
                    gather_simple_dir(
                        state,
                        &entry.path,
                        &format!("{}/attachments/imagenes", s3_prefix),
                        tasks,
                    )
                    .await?;
                }
                "notas" => {
                    gather_notes(state, &entry.path, s3_prefix, tasks).await?;
                }
                "respuesta" => {
                    gather_responses(state, &entry.path, s3_prefix, tasks).await?;
                }
                _ => {}
            }
        } else {
            let lowered = entry.name.to_lowercase();
            if lowered.ends_with(".eml") {
                tasks.push(FileTask::from_entry(
                    entry,
                    format!("{}/original/{}", s3_prefix, sanitize_segment(&lowered)),
                    false,
                ));
            } else if lowered == "index.html" {
                tasks.push(FileTask::from_entry(
                    entry,
                    format!("{}/original/index.html", s3_prefix),
                    true,
                ));
            }
        }
    }

    Ok(())
}

async fn gather_simple_dir(
    state: &AppState,
    remote_dir: &str,
    key_prefix: &str,
    tasks: &mut Vec<FileTask>,
) -> Result<(), TicketsFilesError> {
    let entries = state
        .ssh_service
        .list_remote_dir(remote_dir)
        .await
        .map_err(|err| {
            error!("Error al listar directorio remoto {}: {}", remote_dir, err);
            err
        })?;
    for entry in entries {
        if entry.is_dir {
            continue;
        }
        let key = format!("{}/{}", key_prefix, sanitize_segment(&entry.name));
        tasks.push(FileTask::from_entry(entry, key, false));
    }
    Ok(())
}

async fn gather_notes(
    state: &AppState,
    remote_dir: &str,
    s3_prefix: &str,
    tasks: &mut Vec<FileTask>,
) -> Result<(), TicketsFilesError> {
    let notes = state
        .ssh_service
        .list_remote_dir(remote_dir)
        .await
        .map_err(|err| {
            error!("Error al listar directorio remoto {}: {}", remote_dir, err);
            err
        })?;
    for note in notes {
        if !note.is_dir {
            continue;
        }
        let note_prefix = format!("{}/notas/{}", s3_prefix, sanitize_segment(&note.name));
        let contents = state
            .ssh_service
            .list_remote_dir(&note.path)
            .await
            .map_err(|err| {
                error!("Error al listar directorio remoto {}: {}", note.path, err);
                err
            })?;
        for content in contents {
            if content.is_dir {
                let dir_name = content.name.to_lowercase();
                if dir_name == "archivos" {
                    gather_simple_dir(
                        state,
                        &content.path,
                        &format!("{}/archivos", note_prefix),
                        tasks,
                    )
                    .await?;
                }
                continue;
            }
            let key = format!("{}/{}", note_prefix, sanitize_segment(&content.name));
            tasks.push(FileTask::from_entry(content, key, false));
        }
    }
    Ok(())
}

async fn gather_responses(
    state: &AppState,
    remote_dir: &str,
    s3_prefix: &str,
    tasks: &mut Vec<FileTask>,
) -> Result<(), TicketsFilesError> {
    let responses = state
        .ssh_service
        .list_remote_dir(remote_dir)
        .await
        .map_err(|err| {
            error!("Error al listar directorio remoto {}: {}", remote_dir, err);
            err
        })?;
    for response in responses {
        if !response.is_dir {
            continue;
        }
        let response_prefix = format!("{}/respuestas/{}", s3_prefix, sanitize_segment(&response.name));
        let contents = state
            .ssh_service
            .list_remote_dir(&response.path)
            .await
            .map_err(|err| {
                error!("Error al listar directorio remoto {}: {}", response.path, err);
                err
            })?;
        for content in contents {
            if content.is_dir {
                match content.name.to_lowercase().as_str() {
                    "archivos" => {
                        gather_simple_dir(
                            state,
                            &content.path,
                            &format!("{}/archivos", response_prefix),
                            tasks,
                        )
                        .await?;
                    }
                    "imagenes" => {
                        gather_simple_dir(
                            state,
                            &content.path,
                            &format!("{}/imagenes", response_prefix),
                            tasks,
                        )
                        .await?;
                    }
                    _ => {}
                }
                continue;
            }
            let lowered = content.name.to_lowercase();
            let (key_suffix, capture_html) = if lowered == "index.html" {
                ("index.html".to_string(), false)
            } else {
                (sanitize_segment(&content.name), false)
            };
            let key = format!("{}/{}", response_prefix, key_suffix);
            tasks.push(FileTask::from_entry(content, key, capture_html));
        }
    }
    Ok(())
}

pub async fn process_case_file_tasks(
    state: &AppState,
    tasks: Vec<FileTask>,
) -> Result<(Vec<String>, Vec<MsgMimeFile>, Option<String>), TicketsFilesError> {
    if tasks.is_empty() {
        return Ok((Vec::new(), Vec::new(), None));
    }

    let ssh_service = Arc::clone(&state.ssh_service);
    let storage = Arc::clone(&state.storage);

    let upload_concurrency = file_upload_concurrency();

    let results = stream::iter(tasks.into_iter().map(|task| {
        let ssh = Arc::clone(&ssh_service);
        let storage = Arc::clone(&storage);

        async move {
            let bytes = ssh
                .read_remote_file(&task.remote_path)
                .await
                .map_err(|err| {
                    error!("Error al leer archivo remoto {}: {}", task.remote_path, err);
                    err
                })?;
            let html_body = if task.capture_main_html {
                Some(String::from_utf8(bytes.clone()).unwrap_or_else(|_| {
                    String::from_utf8_lossy(&bytes).into_owned()
                }))
            } else {
                None
            };

            storage
                .upload_object(&task.key, bytes, Some(&task.file_mime))
                .await?;

            let url = storage.object_url(&task.key);
            let mime_file = MsgMimeFile {
                id: ObjectId::new().to_hex(),
                file_image_html: task.file_mime.starts_with("image/") || task.file_mime.contains("html"),
                file_name: task.original_name,
                file_type: task.file_mime,
                file_size: task.file_size.to_string(),
                file_url: url.clone(),
                is_file_local: false,
            };

            Ok::<_, TicketsFilesError>((url, mime_file, html_body))
        }
    }))
    .buffer_unordered(upload_concurrency)
    .try_collect::<Vec<_>>()
    .await?;

    let mut uploaded_urls = Vec::with_capacity(results.len());
    let mut mime_files = Vec::with_capacity(results.len());
    let mut html_body = None;

    for (url, file, html_candidate) in results {
        if html_body.is_none() {
            html_body = html_candidate;
        }
        uploaded_urls.push(url);
        mime_files.push(file);
    }

    Ok((uploaded_urls, mime_files, html_body))
}

pub async fn build_files_from_s3(
    state: &AppState,
    prefix: &str,
) -> Result<(Vec<MsgMimeFile>, Option<String>), TicketsFilesError> {
    let normalized_prefix = prefix.trim_end_matches('/');
    let list_prefix = if normalized_prefix.is_empty() {
        normalized_prefix.to_string()
    } else {
        format!("{}/", normalized_prefix)
    };
    let mut objects = state.storage.list_objects(&list_prefix).await?;
    objects.sort_by(|a, b| a.key.cmp(&b.key));

    let mut files = Vec::new();
    let mut html_body = None;

    for object in objects {
        if object.key.ends_with('/') {
            continue;
        }
        let key = object.key;
        let key_str = key.as_str();
        let file_name = key_str.rsplit('/').next().unwrap_or(key_str).to_string();

        if html_body.is_none() && file_name.eq_ignore_ascii_case("index.html") {
            match state.storage.get_object(&key).await {
                Ok(bytes) => {
                    let text = String::from_utf8(bytes).unwrap_or_else(|err| {
                        String::from_utf8_lossy(err.as_bytes()).into_owned()
                    });
                    html_body = Some(text);
                }
                Err(err) => {
                    warn!("No se pudo leer {key} desde S3 para generar HTML: {err}");
                }
            }
        }

        let mime = MimeGuess::from_path(&file_name)
            .first_raw()
            .unwrap_or("application/octet-stream")
            .to_string();

        files.push(MsgMimeFile {
            id: ObjectId::new().to_hex(),
            file_image_html: mime.starts_with("image/") || mime.contains("html"),
            file_name,
            file_type: mime,
            file_size: object.size.max(0).to_string(),
            file_url: state.storage.object_url(&key),
            is_file_local: false,
        });
    }

    Ok((files, html_body))
}
