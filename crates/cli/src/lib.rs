mod args;

use clap::Parser;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use lance_conversion_core::job::{BlobColumnSpec, IndexSpec, Job};

use args::{Cli, Command};

const JOB_PATH_SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

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
pub enum Error {
    #[error("invalid API URL '{url}'")]
    InvalidUrl { url: String },
    #[error("{status}: {message}")]
    Api { status: u16, message: String },
    #[error("request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("{0}")]
    Json(#[from] serde_json::Error),
}

struct Client {
    base_url: String,
    http: reqwest::Client,
}

impl Client {
    fn new(base_url: &str) -> Result<Self, Error> {
        let trimmed = base_url.trim().trim_end_matches('/');
        if trimmed.is_empty() {
            return Err(Error::InvalidUrl {
                url: base_url.to_owned(),
            });
        }
        Ok(Self {
            base_url: trimmed.to_owned(),
            http: reqwest::Client::new(),
        })
    }

    async fn submit_job(
        &self,
        creator: &str,
        source_uri: &str,
        destination_uri: &str,
        blob_columns: Vec<String>,
        indices: &[IndexSpec],
    ) -> Result<Job, Error> {
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

    async fn list_jobs(
        &self,
        creator: Option<&str>,
        failed_only: bool,
        ongoing_only: bool,
        limit: Option<usize>,
    ) -> Result<Vec<Job>, Error> {
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

    async fn get_job(&self, destination_uri: &str) -> Result<Job, Error> {
        self.get_json(&job_path(destination_uri)).await
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T, Error> {
        let response = self.http.get(self.endpoint(path)).send().await?;
        decode_json(response).await
    }

    async fn post_json<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, Error> {
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

fn job_path(destination_uri: &str) -> String {
    format!(
        "/v1/jobs/{}",
        utf8_percent_encode(destination_uri, JOB_PATH_SEGMENT)
    )
}

async fn decode_json<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, Error> {
    let status = response.status();
    let bytes = response.bytes().await?;
    if status.is_success() {
        return Ok(serde_json::from_slice(&bytes)?);
    }
    let message = serde_json::from_slice::<ErrorBody>(&bytes).map_or_else(
        |_| String::from_utf8_lossy(&bytes).trim().to_owned(),
        |body| body.error,
    );
    Err(Error::Api {
        status: status.as_u16(),
        message: if message.is_empty() {
            status.to_string()
        } else {
            message
        },
    })
}

/// Parses CLI arguments and executes the selected command.
///
/// # Errors
///
/// Returns an error when arguments are invalid, the API is unreachable, or the
/// service rejects the request.
pub async fn run() -> Result<(), Error> {
    let cli = Cli::parse();
    let client = Client::new(&cli.url)?;
    match cli.command {
        Command::Submit {
            creator,
            source,
            destination,
            blob_columns,
            indices,
        } => print_json(
            &client
                .submit_job(&creator, &source, &destination, blob_columns, &indices)
                .await?,
        )?,
        Command::List {
            creator,
            failed,
            ongoing,
            limit,
        } => print_json(
            &client
                .list_jobs(creator.as_deref(), failed, ongoing, limit)
                .await?,
        )?,
        Command::Status { destination } => print_json(&client.get_job(&destination).await?)?,
    }
    Ok(())
}

fn print_json(value: &impl serde::Serialize) -> Result<(), Error> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
