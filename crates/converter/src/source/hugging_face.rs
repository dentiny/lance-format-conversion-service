use std::sync::Arc;

use async_trait::async_trait;
use lance::io::ObjectStoreParams;
use lance_conversion_core::location::DatasetLocation;
use object_store::{ClientOptions, ObjectStore, http::HttpBuilder, path::Path as ObjectPath};
use reqwest::Url;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::{PreparedParquetFile, StorageBackend};
use crate::ConversionError;

const PARQUET_API_URL: &str = "https://datasets-server.huggingface.co/parquet";
const EXPECTED_URI: &str = "expected hf://datasets/owner/name@revision";

pub(super) struct HuggingFaceBackend {
    location: DatasetLocation,
    client: reqwest::Client,
    client_options: ClientOptions,
}

impl HuggingFaceBackend {
    pub(super) fn new(location: DatasetLocation) -> Self {
        Self {
            location,
            client: reqwest::Client::new(),
            client_options: ClientOptions::new(),
        }
    }
}

#[async_trait]
impl StorageBackend for HuggingFaceBackend {
    async fn list_files(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<PreparedParquetFile>, ConversionError> {
        let files = list_parquet_files(&self.client, self.location.uri(), limit).await?;
        let mut prepared = Vec::with_capacity(files.len());
        for file in files {
            prepared.push(prepare_file(file, &self.client_options)?);
        }
        Ok(prepared)
    }

    fn lance_storage_options(&self) -> Result<Option<ObjectStoreParams>, ConversionError> {
        Err(ConversionError::Unsupported(
            "Hugging Face is not a writable Lance destination".to_owned(),
        ))
    }
}

async fn list_parquet_files(
    client: &reqwest::Client,
    source_uri: &str,
    limit: Option<usize>,
) -> Result<Vec<HuggingFaceParquetFile>, ConversionError> {
    let parsed = HuggingFaceLocation::parse(source_uri)?;
    let mut files =
        hf_json::<HuggingFaceParquetResponse>(client.get(PARQUET_API_URL).query(&parsed))
            .await?
            .parquet_files;
    if let Some(limit) = limit {
        files.truncate(limit);
    }
    Ok(files)
}

async fn hf_json<T: DeserializeOwned>(
    request: reqwest::RequestBuilder,
) -> Result<T, ConversionError> {
    let response = request
        .send()
        .await
        .map_err(|error| ConversionError::Read(error.to_string()))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|error| ConversionError::Read(error.to_string()))?;
    if !status.is_success() {
        return Err(ConversionError::Read(format!(
            "Hugging Face HTTP {status}: {body}"
        )));
    }
    serde_json::from_str(&body).map_err(|error| {
        ConversionError::Read(format!("Hugging Face response is not valid JSON: {error}"))
    })
}

/// Builds an HTTP object store whose base is the parquet file URL.
///
/// Hugging Face convert URLs use one revision segment, `refs%2Fconvert%2Fparquet`.
/// Splitting that URL into origin + path lets `object_store` decode `%2F` to
/// `refs/convert/parquet`, which Hugging Face rejects with 404. Passing the
/// full URL as `with_url` and an empty object path keeps the encoding intact.
fn prepare_file(
    file: HuggingFaceParquetFile,
    client_options: &ClientOptions,
) -> Result<PreparedParquetFile, ConversionError> {
    if file.size == 0 {
        return Err(ConversionError::Read(format!(
            "Hugging Face parquet file '{}' is missing a size",
            file.url
        )));
    }
    Url::parse(&file.url).map_err(|error| {
        ConversionError::Read(format!("Hugging Face parquet URL is invalid: {error}"))
    })?;
    let store: Arc<dyn ObjectStore> = Arc::new(
        HttpBuilder::new()
            .with_url(&file.url)
            .with_client_options(client_options.clone())
            .build()
            .map_err(|error| ConversionError::Read(error.to_string()))?,
    );
    Ok(PreparedParquetFile::object(
        store,
        ObjectPath::default(),
        file.size,
        file.url,
    ))
}

#[derive(Deserialize)]
struct HuggingFaceParquetResponse {
    parquet_files: Vec<HuggingFaceParquetFile>,
}

#[derive(Deserialize)]
struct HuggingFaceParquetFile {
    url: String,
    size: u64,
}

#[derive(Serialize)]
struct HuggingFaceLocation {
    dataset: String,
    revision: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    split: Option<String>,
}

impl HuggingFaceLocation {
    fn parse(source_uri: &str) -> Result<Self, ConversionError> {
        let url = Url::parse(source_uri)
            .map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
        if url.scheme() != "hf" || url.host_str() != Some("datasets") {
            return Err(ConversionError::InvalidSource(EXPECTED_URI.to_owned()));
        }
        let path = url.path().trim_matches('/');
        let (dataset, revision) = path.rsplit_once('@').unwrap_or((path, "main"));
        if dataset.split('/').filter(|part| !part.is_empty()).count() != 2 || revision.is_empty() {
            return Err(ConversionError::InvalidSource(EXPECTED_URI.to_owned()));
        }
        Ok(Self {
            dataset: dataset.to_owned(),
            revision: revision.to_owned(),
            config: query_param(&url, "config"),
            split: query_param(&url, "split"),
        })
    }
}

fn query_param(url: &Url, key: &str) -> Option<String> {
    url.query_pairs()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned())
}

#[cfg(test)]
mod tests {
    use super::HuggingFaceLocation;

    #[test]
    fn parses_hf_dataset_uri() {
        let parsed =
            HuggingFaceLocation::parse("hf://datasets/owner/name@main?config=data&split=train")
                .unwrap();
        assert_eq!(parsed.dataset, "owner/name");
        assert_eq!(parsed.revision, "main");
        assert_eq!(parsed.config.as_deref(), Some("data"));
        assert_eq!(parsed.split.as_deref(), Some("train"));
    }
}
