use serde::Serialize;
use std::{env, time::Duration};

pub const DEFAULT_SSH_TIMEOUT_SECS: u64 = 20;
pub const DEFAULT_SSH_PORT: u16 = 22;

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub auth: AuthMethod,
    pub timeout: Duration,
    pub known_hosts_path: Option<String>,
}

#[derive(Debug, Clone)]
pub enum AuthMethod {
    Password { password: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct RemoteDirEntry {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    pub size: u64,
    pub modified: Option<i64>,
}

impl ConnectionConfig {
    pub fn from_env() -> Result<Self, String> {
        let host = env::var("SSH_SERVER_HOST")
            .map_err(|_| "SSH_SERVER_HOST no está configurada".to_string())?;
        let username = env::var("SSH_SERVER_USER")
            .map_err(|_| "SSH_SERVER_USER no está configurada".to_string())?;
        let password = env::var("SSH_SERVER_PASSWORD")
            .map_err(|_| "SSH_SERVER_PASSWORD no está configurada".to_string())?;

        let port = env::var("SSH_SERVER_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_SSH_PORT);

        let timeout_secs = env::var("SSH_SERVER_TIMEOUT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_SSH_TIMEOUT_SECS);

        let known_hosts_path = env::var("SSH_KNOWN_HOSTS_FILE").ok();

        Ok(Self {
            host,
            port,
            username,
            auth: AuthMethod::Password { password },
            timeout: Duration::from_secs(timeout_secs),
            known_hosts_path,
        })
    }
}
