use aws_sdk_s3::{
    config::{BehaviorVersion, Credentials, Region},
    error::SdkError,
    Client,
};
use axum::{http::StatusCode, routing::get, Json, Router};
use std::env;
use tracing::{error, info};

use crate::{routes::structs::S3CheckResponse, state::AppState};

async fn s3_connection_check() -> impl axum::response::IntoResponse {
    let bucket_name = match env::var("S3_BUCKET") {
        Ok(value) => value,
        Err(_) => {
            error!("S3_BUCKET no está configurado");
            return (
                StatusCode::BAD_REQUEST,
                Json(S3CheckResponse {
                    status: "error".to_string(),
                    detail: "S3_BUCKET no está configurado".to_string(),
                }),
            );
        }
    };

    let region_name = env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let access_key = match env::var("S3_ACCESS_KEY") {
        Ok(value) => value,
        Err(_) => {
            error!("S3_ACCESS_KEY no está configurado");
            return (
                StatusCode::BAD_REQUEST,
                Json(S3CheckResponse {
                    status: "error".to_string(),
                    detail: "S3_ACCESS_KEY no está configurado".to_string(),
                }),
            );
        }
    };
    let secret_key = match env::var("S3_SECRET_KEY") {
        Ok(value) => value,
        Err(_) => {
            error!("S3_SECRET_KEY no está configurado");
            return (
                StatusCode::BAD_REQUEST,
                Json(S3CheckResponse {
                    status: "error".to_string(),
                    detail: "S3_SECRET_KEY no está configurado".to_string(),
                }),
            );
        }
    };

    let credentials = Credentials::new(access_key, secret_key, None, None, "env");
    let region = Region::new(region_name.clone());

    let mut config_builder = aws_sdk_s3::Config::builder()
        .credentials_provider(credentials)
        .region(region)
        .behavior_version(BehaviorVersion::latest())
        .force_path_style(true);

    if let Ok(endpoint_url) = env::var("S3_ENDPOINT") {
        config_builder = config_builder.endpoint_url(endpoint_url);
    }

    let config = config_builder.build();

    let client = Client::from_conf(config);

    match client
        .head_bucket()
        .bucket(bucket_name.clone())
        .send()
        .await
    {
        Ok(_) => {
            info!("Conexión correcta al bucket {bucket_name}");
            (
                StatusCode::OK,
                Json(S3CheckResponse {
                    status: "ok".to_string(),
                    detail: format!("Conexión correcta al bucket {bucket_name}"),
                }),
            )
        }
        Err(err) => {
            let status = if let SdkError::ServiceError(service_error) = &err {
                StatusCode::from_u16(service_error.raw().status().as_u16())
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            error!("Error consultando el bucket {bucket_name}: {err:?}");
            (
                status,
                Json(S3CheckResponse {
                    status: "error".to_string(),
                    detail: format!("Error consultando el bucket: {err:?}"),
                }),
            )
        }
    }
}

pub fn router() -> Router<AppState> {
    Router::new().route("/s3-connection", get(s3_connection_check))
}
