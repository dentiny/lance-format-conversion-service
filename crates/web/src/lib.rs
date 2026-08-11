use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub mod config;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use lance_conversion_core::{
    job::{Job, JobKind, NewJob},
    location::DatasetLocation,
};
use lance_job_store::{JobStore, StoreError};

const DEFAULT_JOB_LIST_LIMIT: usize = 100;

#[derive(Clone)]
struct AppState {
    store: Arc<dyn JobStore>,
}

pub fn router(store: Arc<dyn JobStore>) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/v1/jobs", post(create_job).get(list_jobs))
        .with_state(AppState { store })
}

#[derive(Debug, Serialize)]
struct Health {
    status: &'static str,
}

async fn health() -> Json<Health> {
    Json(Health { status: "ok" })
}

#[derive(Debug, Deserialize)]
struct CreateJobRequest {
    creator: String,
    source_uri: String,
    kind: JobKind,
    destination_uri: String,
}

async fn create_job(
    State(state): State<AppState>,
    Json(request): Json<CreateJobRequest>,
) -> Result<StatusCode, ApiError> {
    let source = DatasetLocation::parse_location(request.source_uri)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let destination = DatasetLocation::parse_location(request.destination_uri)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    state
        .store
        .create_job(NewJob {
            creator: request.creator,
            source,
            kind: request.kind,
            destination,
            creation_timestamp_ms: now_ms()?,
        })
        .await?;
    Ok(StatusCode::ACCEPTED)
}

async fn list_jobs(State(state): State<AppState>) -> Result<Json<Vec<Job>>, ApiError> {
    Ok(Json(state.store.list_jobs(DEFAULT_JOB_LIST_LIMIT).await?))
}

fn now_ms() -> Result<i64, ApiError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ApiError::Internal(error.to_string()))?
        .as_millis();
    i64::try_from(millis).map_err(|error| ApiError::Internal(error.to_string()))
}

#[derive(Debug, Error)]
enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Store(#[from] StoreError),
    #[error("{0}")]
    Internal(String),
}

#[derive(Debug, Serialize, Deserialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::BadRequest(_) | Self::Store(StoreError::InvalidInput(_)) => {
                StatusCode::BAD_REQUEST
            }
            Self::Store(StoreError::NotFound) => StatusCode::NOT_FOUND,
            Self::Store(
                StoreError::LeaseLost
                | StoreError::Conflict(_)
                | StoreError::Database(_)
                | StoreError::Worker(_),
            )
            | Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let error = if status == StatusCode::INTERNAL_SERVER_ERROR {
            "internal server error".to_owned()
        } else {
            self.to_string()
        };
        (status, Json(ErrorBody { error })).into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
        response::IntoResponse,
    };
    use tower::ServiceExt;

    use lance_job_store::StoreError;
    use lance_job_store_sqlite::SqliteJobStore;

    use crate::{ApiError, ErrorBody, router};

    #[tokio::test]
    async fn health_endpoint_is_available() {
        let store = SqliteJobStore::open(":memory:").await.unwrap();
        let response = router(Arc::new(store))
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn job_submission_accepts_source_uri() {
        let store = SqliteJobStore::open(":memory:").await.unwrap();
        let app = router(Arc::new(store));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"creator":"test-user","source_uri":"s3://source-bucket/data","kind":"copy","destination_uri":"s3://destination-bucket/data.lance"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn internal_errors_are_not_disclosed() {
        let response =
            ApiError::Store(StoreError::Database("secret path".to_owned())).into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let error: ErrorBody =
            serde_json::from_slice(&to_bytes(response.into_body(), 64 * 1024).await.unwrap())
                .unwrap();
        assert_eq!(error.error, "internal server error");
    }
}
