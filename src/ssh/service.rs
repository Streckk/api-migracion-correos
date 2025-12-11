use std::{env, sync::Arc};

use crate::ssh::{
    client::{SshClient, SshResult},
    config::{ConnectionConfig, RemoteDirEntry},
};

#[derive(Clone)]
pub struct SshService {
    client: Arc<dyn SshClient>,
    config: ConnectionConfig,
    base_path: String,
}

impl SshService {
    pub fn new(
        client: Arc<dyn SshClient>,
        config: ConnectionConfig,
        base_path: impl Into<String>,
    ) -> Self {
        Self {
            client,
            config,
            base_path: normalize_path(base_path.into()),
        }
    }

    pub fn from_env(client: Arc<dyn SshClient>) -> Result<Self, String> {
        let config = ConnectionConfig::from_env()?;
        let base_path = env::var("SSH_REMOTE_BASE_PATH").unwrap_or_else(|_| "/".to_string());
        Ok(Self::new(client, config, base_path))
    }

    pub fn resolved_path(&self, path: &str) -> String {
        resolve_path(&self.base_path, path)
    }

    pub fn base_path(&self) -> &str {
        &self.base_path
    }

    pub async fn test_connection(&self) -> SshResult<()> {
        let connection = self.client.connect(&self.config).await?;
        let _ = self.client.exec(&connection, "echo ok").await?;
        Ok(())
    }

    pub async fn list_remote_dir(&self, path: &str) -> SshResult<Vec<RemoteDirEntry>> {
        let resolved = self.resolved_path(path);
        tracing::info!(target = "ssh", requested = %path, resolved = %resolved, "Consultando directorio remoto");
        let connection = self.client.connect(&self.config).await?;
        self.client.list_dir(&connection, &resolved).await
    }

    pub async fn read_remote_file(&self, path: &str) -> SshResult<Vec<u8>> {
        let resolved = self.resolved_path(path);
        let connection = self.client.connect(&self.config).await?;
        self.client.read_file(&connection, &resolved).await
    }
}

fn resolve_path(base_path: &str, raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return base_path.to_string();
    }

    if trimmed.starts_with('/') {
        return normalize_path(trimmed.to_string());
    }

    let relative = trimmed.trim_matches('/');
    if base_path == "/" {
        format!("/{}", relative)
    } else {
        format!("{}/{}", base_path.trim_end_matches('/'), relative)
    }
}

fn normalize_path(value: impl Into<String>) -> String {
    let value = value.into();
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "/".to_string();
    }
    let without_slashes = trimmed.trim_matches('/');
    if without_slashes.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", without_slashes)
    }
}
