use std::{collections::HashSet, env, sync::Arc};

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
    auth_tokens: Arc<HashSet<String>>,
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
        let auth_tokens = Arc::new(Self::load_api_tokens());

        Self {
            mysql,
            mongo,
            ssh_service,
            storage: storage_service,
            auth_tokens,
        }
    }

    fn load_api_tokens() -> HashSet<String> {
        let raw_tokens = env::var("API_TOKENS")
            .expect("Configura API_TOKENS con una lista separada por comas de tokens válidos");
        let tokens = raw_tokens
            .split(',')
            .map(|token| token.trim().to_string())
            .filter(|token| !token.is_empty())
            .collect::<HashSet<_>>();

        if tokens.is_empty() {
            panic!("API_TOKENS no contiene ningún token válido");
        }

        tokens
    }

    pub fn is_token_valid(&self, candidate: &str) -> bool {
        self.auth_tokens.contains(candidate)
    }
}
