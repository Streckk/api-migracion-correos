use std::env;

use aws_sdk_s3::{
    config::{BehaviorVersion, Credentials, Region},
    primitives::ByteStream,
    types::ObjectCannedAcl,
    Client,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Configuración S3 incompleta: {0}")]
    MissingConfig(String),
    #[error("Error al interactuar con S3: {0}")]
    Client(String),
}

#[derive(Clone)]
pub struct StorageService {
    client: Client,
    bucket: String,
    base_url: String,
}

#[derive(Debug, Clone)]
pub struct StoredObject {
    pub key: String,
    pub size: i64,
}

impl StorageService {
    pub fn from_env() -> Result<Self, StorageError> {
        let bucket = env::var("S3_BUCKET")
            .map_err(|_| StorageError::MissingConfig("S3_BUCKET no está configurada".into()))?;
        let region = env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let access_key = env::var("S3_ACCESS_KEY")
            .map_err(|_| StorageError::MissingConfig("S3_ACCESS_KEY no está configurada".into()))?;
        let secret_key = env::var("S3_SECRET_KEY")
            .map_err(|_| StorageError::MissingConfig("S3_SECRET_KEY no está configurada".into()))?;

        let credentials = Credentials::new(access_key, secret_key, None, None, "env");
        let region_conf = Region::new(region.clone());
        let mut config_builder = aws_sdk_s3::Config::builder()
            .credentials_provider(credentials)
            .region(region_conf)
            .behavior_version(BehaviorVersion::latest())
            .force_path_style(true);

        let endpoint = env::var("S3_ENDPOINT").ok();
        if let Some(ref endpoint_url) = endpoint {
            config_builder = config_builder.endpoint_url(endpoint_url);
        }

        let config = config_builder.build();
        let client = Client::from_conf(config);

        let base_url = if let Some(endpoint_url) = endpoint {
            format!(
                "{}/{}",
                endpoint_url.trim_end_matches('/'),
                bucket.trim_matches('/')
            )
        } else {
            format!("https://{}.s3.{}.amazonaws.com", bucket, region)
        };

        Ok(Self {
            client,
            bucket,
            base_url,
        })
    }

    pub async fn upload_object(
        &self,
        key: &str,
        contents: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<(), StorageError> {
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(contents))
            .acl(ObjectCannedAcl::PublicRead);

        if let Some(kind) = content_type {
            request = request.content_type(kind);
        }

        request
            .send()
            .await
            .map_err(|err| StorageError::Client(err.to_string()))?;

        Ok(())
    }

    pub fn object_url(&self, key: &str) -> String {
        format!("{}/{}", self.base_url.trim_end_matches('/'), key)
    }

    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| StorageError::Client(err.to_string()))?;

        let data = response
            .body
            .collect()
            .await
            .map_err(|err| StorageError::Client(err.to_string()))?;

        Ok(data.into_bytes().to_vec())
    }

    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<StoredObject>, StorageError> {
        let mut continuation = None;
        let mut objects = Vec::new();

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);

            if let Some(token) = &continuation {
                request = request.continuation_token(token);
            }

            let response = request
                .send()
                .await
                .map_err(|err| StorageError::Client(err.to_string()))?;

            if let Some(contents) = response.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        objects.push(StoredObject {
                            key,
                            size: object.size.unwrap_or(0),
                        });
                    }
                }
            }

            if response.next_continuation_token.is_none() {
                break;
            }

            continuation = response.next_continuation_token;
        }

        Ok(objects)
    }
}
