use std::sync::Arc;

use mongodb::Client;
use sea_orm::DatabaseConnection;

use crate::ssh::{Ssh2Client, SshClient, SshService};

#[derive(Clone)]
pub struct AppState {
    pub mysql: DatabaseConnection,
    pub mongo: Client,
    pub ssh_service: Arc<SshService>,
}

impl AppState {
    pub fn new(mysql: DatabaseConnection, mongo: Client) -> Self {
        let ssh_client: Arc<dyn SshClient> = Arc::new(Ssh2Client::default());
        let ssh_service = Arc::new(
            SshService::from_env(ssh_client)
                .expect("Configura SSH_SERVER_HOST, SSH_SERVER_USER y SSH_SERVER_PASSWORD"),
        );
        Self {
            mysql,
            mongo,
            ssh_service,
        }
    }
}
