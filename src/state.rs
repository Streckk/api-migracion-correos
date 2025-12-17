use std::sync::Arc;

use mongodb::Client;
use sea_orm::DatabaseConnection;

use crate::{
    ssh::{Ssh2Client, SshClient, SshService},
    storage::StorageService,
};

#[derive(Clone)]
pub struct AppState {
    pub mysql: DatabaseConnection,
    pub mongo: Client,
    pub ssh_service: Arc<SshService>,
    pub storage: Arc<StorageService>,
}

impl AppState {
    pub fn new(mysql: DatabaseConnection, mongo: Client) -> Self {
        let ssh_client: Arc<dyn SshClient> = Arc::new(Ssh2Client::default());
        let ssh_service = Arc::new(
            SshService::from_env(ssh_client)
                .expect("Configura SSH_SERVER_HOST, SSH_SERVER_USER y SSH_SERVER_PASSWORD"),
        );
        let storage_service =
            Arc::new(StorageService::from_env().expect("Configura credenciales de S3"));

        Self {
            mysql,
            mongo,
            ssh_service,
            storage: storage_service,
        }
    }
}
