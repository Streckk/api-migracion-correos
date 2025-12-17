use std::{
    io::{Read, Write},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use ssh2::{CheckResult, KnownHostFileKind, Session};
use thiserror::Error;
use tokio::task;

use crate::ssh::config::{AuthMethod, ConnectionConfig, RemoteDirEntry};

#[derive(Debug, Error)]
pub enum SshError {
    #[error("Error de E/S: {0}")]
    Io(#[from] std::io::Error),
    #[error("Error SSH: {0}")]
    Ssh2(#[from] ssh2::Error),
    #[error("Host key desconocida para {host}")]
    HostKeyUnknown { host: String },
    #[error("Host key no coincide para {host}")]
    HostKeyMismatch { host: String },
    #[error("Fallo de autenticación SSH")]
    AuthenticationFailed,
    #[error("Solicitud inválida: {0}")]
    InvalidRequest(String),
    #[error("Error interno al ejecutar tarea SSH: {0}")]
    BlockingTaskFailed(String),
}

pub type SshResult<T> = Result<T, SshError>;

#[derive(Clone)]
pub struct SshConnection {
    pub(crate) session: Arc<Mutex<Session>>,
}

impl SshConnection {
    fn new(session: Session) -> Self {
        Self {
            session: Arc::new(Mutex::new(session)),
        }
    }
}

#[async_trait]
pub trait SshClient: Send + Sync {
    async fn connect(&self, config: &ConnectionConfig) -> SshResult<SshConnection>;
    async fn exec(&self, connection: &SshConnection, command: &str) -> SshResult<String>;
    async fn list_dir(
        &self,
        connection: &SshConnection,
        path: &str,
    ) -> SshResult<Vec<RemoteDirEntry>>;
    async fn read_file(&self, connection: &SshConnection, path: &str) -> SshResult<Vec<u8>>;
    async fn write_file(
        &self,
        connection: &SshConnection,
        path: &str,
        contents: &[u8],
    ) -> SshResult<()>;
}

#[derive(Clone, Default)]
pub struct Ssh2Client;

#[async_trait]
impl SshClient for Ssh2Client {
    async fn connect(&self, config: &ConnectionConfig) -> SshResult<SshConnection> {
        let host = config.host.clone();
        let port = config.port;
        let username = config.username.clone();
        let timeout = config.timeout;
        let known_hosts_path = config.known_hosts_path.clone();
        let auth_method = config.auth.clone();

        let session = task::spawn_blocking(move || -> SshResult<Session> {
            let tcp = std::net::TcpStream::connect((host.as_str(), port))?;
            tcp.set_read_timeout(Some(timeout))?;
            tcp.set_write_timeout(Some(timeout))?;

            let mut session = Session::new().map_err(|err| SshError::Ssh2(err))?;
            session.set_tcp_stream(tcp);
            session.set_timeout(timeout_as_millis(timeout));
            session.handshake()?;

            if let Some(path) = known_hosts_path {
                verify_host_key(&session, &host, &path)?;
            }

            match auth_method {
                AuthMethod::Password { password } => {
                    session.userauth_password(&username, &password)?;
                }
                AuthMethod::KeyPath {
                    private_key_path,
                    passphrase,
                } => {
                    session.userauth_pubkey_file(
                        &username,
                        None,
                        Path::new(&private_key_path),
                        passphrase.as_deref(),
                    )?;
                }
            }

            if !session.authenticated() {
                return Err(SshError::AuthenticationFailed);
            }

            Ok(session)
        })
        .await
        .map_err(|err| SshError::BlockingTaskFailed(err.to_string()))??;

        Ok(SshConnection::new(session))
    }

    async fn exec(&self, connection: &SshConnection, command: &str) -> SshResult<String> {
        let command = command.to_string();
        let session = connection.session.clone();
        task::spawn_blocking(move || -> SshResult<String> {
            let guard = session
                .lock()
                .map_err(|_| SshError::BlockingTaskFailed("Mutex poisoned".to_string()))?;
            let mut channel = guard.channel_session()?;
            channel.exec(&command)?;
            let mut buffer = String::new();
            channel.read_to_string(&mut buffer)?;
            channel.wait_close()?;
            Ok(buffer)
        })
        .await
        .map_err(|err| SshError::BlockingTaskFailed(err.to_string()))?
    }

    async fn list_dir(
        &self,
        connection: &SshConnection,
        path: &str,
    ) -> SshResult<Vec<RemoteDirEntry>> {
        let target_path = path.to_string();
        let session = connection.session.clone();
        task::spawn_blocking(move || -> SshResult<Vec<RemoteDirEntry>> {
            let guard = session
                .lock()
                .map_err(|_| SshError::BlockingTaskFailed("Mutex poisoned".to_string()))?;
            let sftp = guard.sftp()?;
            let entries = sftp.readdir(Path::new(&target_path))?;
            Ok(entries
                .into_iter()
                .map(|(path, stat)| RemoteDirEntry {
                    name: path
                        .file_name()
                        .map(|value| value.to_string_lossy().into_owned())
                        .unwrap_or_else(|| path.to_string_lossy().into_owned()),
                    path: path.to_string_lossy().into_owned(),
                    is_dir: is_dir_from_perm(stat.perm),
                    size: stat.size.unwrap_or(0),
                    modified: stat.mtime.map(|value| value as i64),
                })
                .collect())
        })
        .await
        .map_err(|err| SshError::BlockingTaskFailed(err.to_string()))?
    }

    async fn read_file(&self, connection: &SshConnection, path: &str) -> SshResult<Vec<u8>> {
        let target_path = path.to_string();
        let session = connection.session.clone();
        task::spawn_blocking(move || -> SshResult<Vec<u8>> {
            let guard = session
                .lock()
                .map_err(|_| SshError::BlockingTaskFailed("Mutex poisoned".to_string()))?;
            let sftp = guard.sftp()?;
            let mut file = sftp.open(Path::new(&target_path))?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;
            Ok(buffer)
        })
        .await
        .map_err(|err| SshError::BlockingTaskFailed(err.to_string()))?
    }

    async fn write_file(
        &self,
        connection: &SshConnection,
        path: &str,
        contents: &[u8],
    ) -> SshResult<()> {
        let target_path = path.to_string();
        let payload = contents.to_vec();
        let session = connection.session.clone();
        task::spawn_blocking(move || -> SshResult<()> {
            let guard = session
                .lock()
                .map_err(|_| SshError::BlockingTaskFailed("Mutex poisoned".to_string()))?;
            let sftp = guard.sftp()?;
            let mut file = sftp.create(Path::new(&target_path))?;
            file.write_all(&payload)?;
            file.flush()?;
            Ok(())
        })
        .await
        .map_err(|err| SshError::BlockingTaskFailed(err.to_string()))?
    }
}

fn verify_host_key(session: &Session, host: &str, known_hosts_path: &str) -> SshResult<()> {
    let mut known_hosts = session.known_hosts()?;
    known_hosts.read_file(Path::new(known_hosts_path), KnownHostFileKind::OpenSSH)?;
    let (host_key, _) = session.host_key().ok_or_else(|| SshError::HostKeyUnknown {
        host: host.to_string(),
    })?;

    match known_hosts.check(host, host_key) {
        CheckResult::Match => Ok(()),
        CheckResult::NotFound => Err(SshError::HostKeyUnknown {
            host: host.to_string(),
        }),
        CheckResult::Mismatch => Err(SshError::HostKeyMismatch {
            host: host.to_string(),
        }),
        CheckResult::Failure => Err(SshError::BlockingTaskFailed(
            "No se pudo verificar la host key".to_string(),
        )),
    }
}

fn is_dir_from_perm(perm: Option<u32>) -> bool {
    const S_IFMT: u32 = 0o170000;
    const S_IFDIR: u32 = 0o040000;
    perm.map(|value| (value & S_IFMT) == S_IFDIR)
        .unwrap_or(false)
}

fn timeout_as_millis(duration: Duration) -> u32 {
    duration.as_millis().try_into().unwrap_or(u32::MAX)
}
