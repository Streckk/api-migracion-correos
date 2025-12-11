use mongodb::Client;
use sea_orm::DatabaseConnection;

#[derive(Clone)]
pub struct AppState {
    pub mysql: DatabaseConnection,
    pub mongo: Client,
}

impl AppState {
    pub fn new(mysql: DatabaseConnection, mongo: Client) -> Self {
        Self { mysql, mongo }
    }
}
