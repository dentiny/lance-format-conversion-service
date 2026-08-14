use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub mod config;

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use lance_converter::{SourceSchemaInspection, inspect_source_schema};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use lance_conversion_core::{
    job::{BlobColumnSpec, IndexSpec, Job, NewJob},
    location::DatasetLocation,
};
use lance_job_store::{JobOrderField, JobQuery, JobStore, StoreError};

const DEFAULT_JOB_LIST_LIMIT: usize = 100;
const INDEX_HTML: &str = include_str!("../static/index.html");
const JOBS_HTML: &str = include_str!("../static/jobs.html");
const APP_CSS: &str = include_str!("../static/app.css");
const APP_JS: &str = include_str!("../static/app.js");

#[derive(Clone)]
struct AppState {
    store: Arc<dyn JobStore>,
}

pub fn router(store: Arc<dyn JobStore>) -> Router {
    Router::new()
        .route("/", get(index_page))
        .route("/jobs", get(jobs_page))
        .route("/app.css", get(stylesheet))
        .route("/app.js", get(javascript))
        .route("/healthz", get(health))
        .route("/v1/jobs", post(create_job).get(list_jobs))
        .route("/v1/sources/inspect", post(inspect_source))
        .with_state(AppState { store })
}

/// Serves the embedded MVP application shell.
async fn index_page() -> Html<&'static str> {
    Html(INDEX_HTML)
}

/// Serves the conversion job monitoring page.
async fn jobs_page() -> Html<&'static str> {
    Html(JOBS_HTML)
}

/// Serves the embedded application stylesheet.
async fn stylesheet() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], APP_CSS)
}

/// Serves the embedded application JavaScript.
async fn javascript() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        APP_JS,
    )
}

#[derive(Debug, Serialize)]
struct Health {
    status: &'static str,
}

async fn health() -> Json<Health> {
    Json(Health { status: "ok" })
}

#[derive(Debug, Deserialize)]
struct InspectSourceRequest {
    source_uri: String,
}

/// Inspects and validates a source schema before job submission.
async fn inspect_source(
    Json(request): Json<InspectSourceRequest>,
) -> Result<Json<SourceSchemaInspection>, ApiError> {
    inspect_source_schema(&request.source_uri)
        .await
        .map(Json)
        .map_err(|error| ApiError::BadRequest(error.to_string()))
}

#[derive(Debug, Deserialize)]
struct CreateJobRequest {
    creator: String,
    source_uri: String,
    destination_uri: String,
    #[serde(default)]
    blob_columns: Vec<BlobColumnSpec>,
    #[serde(default)]
    indices: Vec<IndexSpec>,
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
            destination,
            blob_columns: request.blob_columns,
            indices: request.indices,
            creation_timestamp_ms: now_ms()?,
        })
        .await?;
    Ok(StatusCode::ACCEPTED)
}

#[derive(Debug, Default, Deserialize)]
struct ListJobsQuery {
    creator: Option<String>,
    failed_only: Option<bool>,
    ongoing_only: Option<bool>,
    creation_timestamp_ms_from: Option<i64>,
    creation_timestamp_ms_to: Option<i64>,
    order_by: Option<String>,
    order: Option<String>,
    limit: Option<usize>,
}

/// Lists jobs matching the indexed creator, creation-time, and failure filters.
async fn list_jobs(
    State(state): State<AppState>,
    Query(query): Query<ListJobsQuery>,
) -> Result<Json<Vec<Job>>, ApiError> {
    let creator = query
        .creator
        .map(|creator| creator.trim().to_owned())
        .filter(|creator| !creator.is_empty());
    let limit = query
        .limit
        .unwrap_or(DEFAULT_JOB_LIST_LIMIT)
        .min(DEFAULT_JOB_LIST_LIMIT);
    let order_by = match query.order_by.as_deref() {
        None | Some("creation") => JobOrderField::CreationTimestamp,
        Some("update") => JobOrderField::UpdateTimestamp,
        Some(field) => {
            return Err(ApiError::BadRequest(format!(
                "unsupported order field '{field}'; expected 'creation' or 'update'"
            )));
        }
    };
    let descending = match query.order.as_deref() {
        None | Some("desc") => true,
        Some("asc") => false,
        Some(order) => {
            return Err(ApiError::BadRequest(format!(
                "unsupported order '{order}'; expected 'asc' or 'desc'"
            )));
        }
    };
    Ok(Json(
        state
            .store
            .query_jobs(JobQuery {
                creator,
                failed_only: query.failed_only.unwrap_or(false),
                ongoing_only: query.ongoing_only.unwrap_or(false),
                creation_timestamp_ms_from: query.creation_timestamp_ms_from,
                creation_timestamp_ms_to: query.creation_timestamp_ms_to,
                order_by,
                descending,
                limit,
            })
            .await?,
    ))
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

    use lance_conversion_core::job::IndexType;
    use lance_job_store::StoreError;
    use lance_job_store_sqlite::SqliteJobStore;

    use crate::{ApiError, ErrorBody, router};

    const TEST_RESPONSE_BODY_LIMIT: usize = 64 * 1024;

    #[tokio::test]
    async fn root_serves_the_mvp_ui() {
        let store = SqliteJobStore::open(":memory:").await.unwrap();
        let response = router(Arc::new(store))
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), TEST_RESPONSE_BODY_LIMIT)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).contains("Lance Format Conversion"));
    }

    #[tokio::test]
    async fn jobs_page_is_served_separately() {
        let store = SqliteJobStore::open(":memory:").await.unwrap();
        let response = router(Arc::new(store))
            .oneshot(Request::builder().uri("/jobs").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), TEST_RESPONSE_BODY_LIMIT)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).contains("Conversion jobs"));
    }

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
                        r#"{"creator":"test-user","source_uri":"s3://source-bucket/data","destination_uri":"s3://destination-bucket/data.lance"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn job_query_filters_by_creator() {
        let store = Arc::new(SqliteJobStore::open(":memory:").await.unwrap());
        let app = router(store);
        for (creator, destination) in [
            ("alice", "s3://destination-bucket/alice.lance"),
            ("bob", "s3://destination-bucket/bob.lance"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/jobs")
                        .header("content-type", "application/json")
                        .body(Body::from(format!(
                            r#"{{"creator":"{creator}","source_uri":"s3://source-bucket/data","destination_uri":"{destination}"}}"#
                        )))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::ACCEPTED);
        }

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/jobs?creator=alice&ongoing_only=true&order_by=update&order=asc")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let jobs: Vec<lance_conversion_core::job::Job> = serde_json::from_slice(
            &to_bytes(response.into_body(), TEST_RESPONSE_BODY_LIMIT)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].creator, "alice");
    }

    #[tokio::test]
    async fn job_submission_maps_blob_and_index_specs() {
        let store = Arc::new(SqliteJobStore::open(":memory:").await.unwrap());
        let app = router(store.clone());
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{
                            "creator":"test-user",
                            "source_uri":"s3://source-bucket/data",
                            "destination_uri":"s3://destination-bucket/specs.lance",
                            "blob_columns":[{"column":"image"}],
                            "indices":[{
                                "columns":["embedding"],
                                "index_type":"vector"
                            }]
                        }"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/jobs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let jobs: Vec<lance_conversion_core::job::Job> = serde_json::from_slice(
            &to_bytes(response.into_body(), TEST_RESPONSE_BODY_LIMIT)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(jobs[0].blob_columns[0].column, "image");
        assert_eq!(jobs[0].indices[0].columns, ["embedding"]);
        assert_eq!(jobs[0].indices[0].index_type, IndexType::Vector);
    }

    #[tokio::test]
    async fn internal_errors_are_not_disclosed() {
        let response =
            ApiError::Store(StoreError::Database("secret path".to_owned())).into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let error: ErrorBody = serde_json::from_slice(
            &to_bytes(response.into_body(), TEST_RESPONSE_BODY_LIMIT)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(error.error, "internal server error");
    }
}
