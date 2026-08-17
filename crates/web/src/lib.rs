use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};

pub mod config;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderValue, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use lance_converter::{SourceSchemaInspection, inspect_source_schema};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use lance_conversion_core::{
    job::{BlobColumnSpec, IndexSpec, Job, NewJob},
    location::DatasetLocation,
};
use lance_job_store::{JobOrderField, JobQuery, JobStore, StoreError, now_ms};

const DEFAULT_JOB_LIST_LIMIT: usize = 100;
const JOB_PATH_SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');
const INDEX_HTML: &str = include_str!("../static/index.html");
const JOBS_HTML: &str = include_str!("../static/jobs.html");
const APP_CSS: &str = include_str!("../static/app.css");
const APP_JS: &str = include_str!("../static/app.js");
const NO_STORE: HeaderValue = HeaderValue::from_static("no-store");

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
        .route("/v1/jobs/{destination_uri}", get(get_job))
        .route("/v1/sources/inspect", post(inspect_source))
        .with_state(AppState { store })
}

/// Serves the embedded MVP application shell.
async fn index_page() -> impl IntoResponse {
    html_page(INDEX_HTML)
}

/// Serves the conversion job monitoring page.
async fn jobs_page() -> impl IntoResponse {
    html_page(JOBS_HTML)
}

/// Serves the embedded application stylesheet.
async fn stylesheet() -> impl IntoResponse {
    static_asset("text/css; charset=utf-8", APP_CSS)
}

/// Serves the embedded application JavaScript.
async fn javascript() -> impl IntoResponse {
    static_asset("text/javascript; charset=utf-8", APP_JS)
}

fn content_token(body: &str) -> String {
    let mut hasher = DefaultHasher::new();
    body.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

fn css_token() -> &'static str {
    static TOKEN: OnceLock<String> = OnceLock::new();
    TOKEN.get_or_init(|| content_token(APP_CSS))
}

fn js_token() -> &'static str {
    static TOKEN: OnceLock<String> = OnceLock::new();
    TOKEN.get_or_init(|| content_token(APP_JS))
}

fn with_asset_tokens(html: &str) -> String {
    html.replace(
        "href=\"/app.css\"",
        &format!("href=\"/app.css?v={}\"", css_token()),
    )
    .replace(
        "href=\"app.css\"",
        &format!("href=\"/app.css?v={}\"", css_token()),
    )
    .replace(
        "src=\"/app.js\"",
        &format!("src=\"/app.js?v={}\"", js_token()),
    )
    .replace(
        "src=\"app.js\"",
        &format!("src=\"/app.js?v={}\"", js_token()),
    )
}

fn no_store(content_type: &'static str) -> [(header::HeaderName, HeaderValue); 3] {
    [
        (header::CONTENT_TYPE, HeaderValue::from_static(content_type)),
        (header::CACHE_CONTROL, NO_STORE),
        (
            header::HeaderName::from_static("cdn-cache-control"),
            NO_STORE,
        ),
    ]
}

fn html_page(html: &'static str) -> impl IntoResponse {
    (
        no_store("text/html; charset=utf-8"),
        Html(with_asset_tokens(html)),
    )
}

fn static_asset(content_type: &'static str, body: &'static str) -> impl IntoResponse {
    (no_store(content_type), body)
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
) -> Result<impl IntoResponse, ApiError> {
    let creator = request.creator.trim();
    if creator.is_empty() {
        return Err(ApiError::BadRequest("creator must not be empty".to_owned()));
    }
    let source = DatasetLocation::parse_location(request.source_uri)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let destination = DatasetLocation::parse_location(request.destination_uri)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let destination_uri = destination.uri().to_owned();
    state
        .store
        .create_job(NewJob {
            creator: creator.to_owned(),
            source,
            destination,
            blob_columns: request.blob_columns,
            indices: request.indices,
            creation_timestamp_ms: now_ms()?,
        })
        .await?;
    let job = state.store.get_job(&destination_uri).await?;
    let location = job_location(&job.destination_uri)?;
    Ok((
        StatusCode::CREATED,
        [(header::LOCATION, location)],
        Json(job),
    ))
}

/// Returns the job identified by its destination URI.
async fn get_job(
    State(state): State<AppState>,
    Path(destination_uri): Path<String>,
) -> Result<Json<Job>, ApiError> {
    Ok(Json(state.store.get_job(&destination_uri).await?))
}

fn job_location(destination_uri: &str) -> Result<HeaderValue, ApiError> {
    let location = format!(
        "/v1/jobs/{}",
        utf8_percent_encode(destination_uri, JOB_PATH_SEGMENT)
    );
    HeaderValue::from_str(&location)
        .map_err(|error| ApiError::BadRequest(format!("invalid job location: {error}")))
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

#[derive(Debug, Error)]
enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Store(#[from] StoreError),
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
            Self::Store(StoreError::Conflict(_)) => StatusCode::CONFLICT,
            Self::Store(
                StoreError::LeaseLost | StoreError::Database(_) | StoreError::Worker(_),
            ) => StatusCode::INTERNAL_SERVER_ERROR,
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
        http::{Request, StatusCode, header},
        response::IntoResponse,
    };
    use serial_test::serial;
    use tower::ServiceExt;

    use lance_conversion_core::job::{IndexType, JobStatus};
    use lance_job_store::{JobStore, StoreError};
    use lance_job_store_postgres::test_utils::open_isolated;
    use lance_job_store_sqlite::SqliteJobStore;

    use crate::{ApiError, ErrorBody, job_location, router};

    const TEST_RESPONSE_BODY_LIMIT: usize = 64 * 1024;

    fn job_uri(destination_uri: &str) -> String {
        job_location(destination_uri)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned()
    }

    async fn test_stores() -> [(&'static str, Arc<dyn JobStore>); 2] {
        [
            (
                "sqlite",
                Arc::new(SqliteJobStore::open(":memory:").await.unwrap()),
            ),
            ("postgres", Arc::new(open_isolated().await)),
        ]
    }

    #[tokio::test]
    #[serial]
    async fn root_serves_the_mvp_ui() {
        for (backend, store) in test_stores().await {
            root_serves_the_mvp_ui_impl(backend, store).await;
        }
    }

    async fn root_serves_the_mvp_ui_impl(backend: &str, store: Arc<dyn JobStore>) {
        let response = router(store)
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{backend}");
        let body = to_bytes(response.into_body(), TEST_RESPONSE_BODY_LIMIT)
            .await
            .unwrap();
        assert!(
            String::from_utf8_lossy(&body).contains("Lance Format Conversion"),
            "{backend}"
        );
    }

    #[tokio::test]
    #[serial]
    async fn jobs_page_is_served_separately() {
        for (backend, store) in test_stores().await {
            jobs_page_is_served_separately_impl(backend, store).await;
        }
    }

    async fn jobs_page_is_served_separately_impl(backend: &str, store: Arc<dyn JobStore>) {
        let response = router(store)
            .oneshot(Request::builder().uri("/jobs").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{backend}");
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .map(|value| value.as_bytes()),
            Some(b"no-store".as_slice()),
            "{backend}"
        );
        let page_bytes = to_bytes(response.into_body(), TEST_RESPONSE_BODY_LIMIT)
            .await
            .unwrap();
        let page = String::from_utf8_lossy(&page_bytes);
        assert!(page.contains("Conversion jobs"), "{backend}");
        assert!(
            page.contains(&format!("/app.js?v={}", crate::js_token())),
            "{backend}"
        );
    }

    #[tokio::test]
    async fn javascript_is_not_cacheable() {
        let store = Arc::new(SqliteJobStore::open(":memory:").await.unwrap());
        let response = router(store)
            .oneshot(
                Request::builder()
                    .uri("/app.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .map(|value| value.as_bytes()),
            Some(b"no-store".as_slice())
        );
        let js_bytes = to_bytes(response.into_body(), TEST_RESPONSE_BODY_LIMIT)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&js_bytes).contains("setInterval"));
    }

    #[tokio::test]
    #[serial]
    async fn health_endpoint_is_available() {
        for (backend, store) in test_stores().await {
            health_endpoint_is_available_impl(backend, store).await;
        }
    }

    async fn health_endpoint_is_available_impl(backend: &str, store: Arc<dyn JobStore>) {
        let response = router(store)
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{backend}");
    }

    #[tokio::test]
    #[serial]
    async fn job_submission_accepts_source_uri() {
        for (backend, store) in test_stores().await {
            job_submission_accepts_source_uri_impl(backend, store).await;
        }
    }

    async fn job_submission_accepts_source_uri_impl(backend: &str, store: Arc<dyn JobStore>) {
        let destination_uri = "s3://destination-bucket/data.lance";
        let app = router(store);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/jobs")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"creator":"test-user","source_uri":"s3://source-bucket/data","destination_uri":"{destination_uri}"}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED, "{backend}");
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            &job_uri(destination_uri),
            "{backend}"
        );
        let job: lance_conversion_core::job::Job = serde_json::from_slice(
            &to_bytes(response.into_body(), TEST_RESPONSE_BODY_LIMIT)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(job.destination_uri, destination_uri, "{backend}");
        assert_eq!(job.status, JobStatus::Queuing, "{backend}");
        assert_eq!(job.attempt, 0, "{backend}");

        let response = app
            .oneshot(
                Request::builder()
                    .uri(job_uri(destination_uri))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{backend}");
        let fetched: lance_conversion_core::job::Job = serde_json::from_slice(
            &to_bytes(response.into_body(), TEST_RESPONSE_BODY_LIMIT)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(fetched.destination_uri, destination_uri, "{backend}");
        assert_eq!(fetched.creator, "test-user", "{backend}");
    }

    #[tokio::test]
    #[serial]
    async fn job_query_filters_by_creator() {
        for (backend, store) in test_stores().await {
            job_query_filters_by_creator_impl(backend, store).await;
        }
    }

    async fn job_query_filters_by_creator_impl(backend: &str, store: Arc<dyn JobStore>) {
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
            assert_eq!(response.status(), StatusCode::CREATED, "{backend}");
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
        assert_eq!(response.status(), StatusCode::OK, "{backend}");
        let jobs: Vec<lance_conversion_core::job::Job> = serde_json::from_slice(
            &to_bytes(response.into_body(), TEST_RESPONSE_BODY_LIMIT)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(jobs.len(), 1, "{backend}");
        assert_eq!(jobs[0].creator, "alice", "{backend}");
    }

    #[tokio::test]
    #[serial]
    async fn job_submission_maps_blob_and_index_specs() {
        for (backend, store) in test_stores().await {
            job_submission_maps_blob_and_index_specs_impl(backend, store).await;
        }
    }

    async fn job_submission_maps_blob_and_index_specs_impl(
        backend: &str,
        store: Arc<dyn JobStore>,
    ) {
        let app = router(store);
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
                                "column":"embedding",
                                "index_type":"vector"
                            }]
                        }"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED, "{backend}");

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
        assert_eq!(jobs[0].blob_columns[0].column, "image", "{backend}");
        assert_eq!(jobs[0].indices[0].column, "embedding", "{backend}");
        assert_eq!(
            jobs[0].indices[0].index_type,
            IndexType::Vector,
            "{backend}"
        );
    }

    #[tokio::test]
    #[serial]
    async fn duplicate_destination_conflicts() {
        for (backend, store) in test_stores().await {
            duplicate_destination_conflicts_impl(backend, store).await;
        }
    }

    async fn duplicate_destination_conflicts_impl(backend: &str, store: Arc<dyn JobStore>) {
        let app = router(store);
        let body = r#"{"creator":"test-user","source_uri":"s3://source-bucket/data","destination_uri":"s3://destination-bucket/dup.lance"}"#;
        for expected in [StatusCode::CREATED, StatusCode::CONFLICT] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/jobs")
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "{backend}");
        }
    }

    #[tokio::test]
    #[serial]
    async fn missing_job_is_not_found() {
        for (backend, store) in test_stores().await {
            missing_job_is_not_found_impl(backend, store).await;
        }
    }

    async fn missing_job_is_not_found_impl(backend: &str, store: Arc<dyn JobStore>) {
        let response = router(store)
            .oneshot(
                Request::builder()
                    .uri(job_uri("s3://destination-bucket/missing.lance"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{backend}");
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
