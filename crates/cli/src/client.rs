use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use lance_conversion_core::job::{BlobColumnSpec, IndexSpec, Job};

const JOB_PATH_SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    http: reqwest::Client,
}

#[derive(Debug, Serialize)]
struct CreateJobRequest<'a> {
    creator: &'a str,
    source_uri: &'a str,
    destination_uri: &'a str,
    blob_columns: Vec<BlobColumnSpec>,
    indices: &'a [IndexSpec],
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    error: String,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("invalid API URL '{url}'")]
    InvalidUrl { url: String },
    #[error("{status}: {message}")]
    Api { status: u16, message: String },
    #[error("request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("{0}")]
    Json(#[from] serde_json::Error),
}

impl Client {
    /// Builds an HTTP client for the conversion control plane.
    ///
    /// # Errors
    ///
    /// Returns an error when the base URL is empty.
    pub fn new(base_url: &str) -> Result<Self, ClientError> {
        let trimmed = base_url.trim().trim_end_matches('/');
        if trimmed.is_empty() {
            return Err(ClientError::InvalidUrl {
                url: base_url.to_owned(),
            });
        }
        Ok(Self {
            base_url: trimmed.to_owned(),
            http: reqwest::Client::new(),
        })
    }

    /// Calls `POST /v1/jobs`.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or the service reports an error.
    pub async fn submit_job(
        &self,
        creator: &str,
        source_uri: &str,
        destination_uri: &str,
        blob_columns: Vec<String>,
        indices: &[IndexSpec],
    ) -> Result<Job, ClientError> {
        self.post_json(
            "/v1/jobs",
            &CreateJobRequest {
                creator,
                source_uri,
                destination_uri,
                blob_columns: blob_columns
                    .into_iter()
                    .map(|column| BlobColumnSpec { column })
                    .collect(),
                indices,
            },
        )
        .await
    }

    /// Calls `GET /v1/jobs` with optional filters.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or the service reports an error.
    pub async fn list_jobs(
        &self,
        creator: Option<&str>,
        failed_only: bool,
        ongoing_only: bool,
        limit: Option<usize>,
    ) -> Result<Vec<Job>, ClientError> {
        let mut query = Vec::new();
        if let Some(creator) = creator {
            query.push(("creator", creator.to_owned()));
        }
        if failed_only {
            query.push(("failed_only", "true".to_owned()));
        }
        if ongoing_only {
            query.push(("ongoing_only", "true".to_owned()));
        }
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        let response = self
            .http
            .get(self.endpoint("/v1/jobs"))
            .query(&query)
            .send()
            .await?;
        decode_json(response).await
    }

    /// Calls `GET /v1/jobs/{destination_uri}`.
    ///
    /// # Errors
    ///
    /// Returns an error when the request fails or the service reports an error.
    pub async fn get_job(&self, destination_uri: &str) -> Result<Job, ClientError> {
        self.get_json(&job_path(destination_uri)).await
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        let response = self.http.get(self.endpoint(path)).send().await?;
        decode_json(response).await
    }

    async fn post_json<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, ClientError> {
        let response = self
            .http
            .post(self.endpoint(path))
            .json(body)
            .send()
            .await?;
        decode_json(response).await
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }
}

pub(crate) fn job_path(destination_uri: &str) -> String {
    format!(
        "/v1/jobs/{}",
        utf8_percent_encode(destination_uri, JOB_PATH_SEGMENT)
    )
}

async fn decode_json<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, ClientError> {
    let status = response.status();
    let bytes = response.bytes().await?;
    if status.is_success() {
        return Ok(serde_json::from_slice(&bytes)?);
    }
    let message = serde_json::from_slice::<ErrorBody>(&bytes).map_or_else(
        |_| String::from_utf8_lossy(&bytes).trim().to_owned(),
        |body| body.error,
    );
    Err(ClientError::Api {
        status: status.as_u16(),
        message: if message.is_empty() {
            status.to_string()
        } else {
            message
        },
    })
}

#[cfg(test)]
mod tests {
    use super::job_path;

    #[test]
    fn encodes_destination_uri_as_one_path_segment() {
        assert_eq!(
            job_path("s3://destination-bucket/data.lance"),
            "/v1/jobs/s3%3A%2F%2Fdestination-bucket%2Fdata.lance"
        );
    }
}
