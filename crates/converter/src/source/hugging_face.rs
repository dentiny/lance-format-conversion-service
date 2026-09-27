use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use lance::deps::datafusion::physical_plan::SendableRecordBatchStream;
use reqwest::Url;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::{
    Source,
    parquet::{ParquetSource, PreparedParquetFile},
};
use crate::{ConversionError, storage};

const PARQUET_API_URL: &str = "https://datasets-server.huggingface.co/parquet";
const EXPECTED_URI: &str = "expected hf://datasets/owner/name@revision";

pub(super) struct HuggingFaceSource {
    uri: String,
}

impl HuggingFaceSource {
    pub(super) fn new(uri: &str) -> Self {
        Self {
            uri: uri.to_owned(),
        }
    }

    pub(super) async fn schema(uri: &str) -> Result<SchemaRef, ConversionError> {
        let files = parquet_files(uri).await?;
        let file = files.into_iter().next().ok_or_else(no_parquet_files)?;
        Box::new(ParquetSource::from_files(vec![prepare_file(&file)?]))
            .into_stream()
            .await
            .map(|stream| stream.schema())
    }
}

#[async_trait]
impl Source for HuggingFaceSource {
    async fn into_stream(self: Box<Self>) -> Result<SendableRecordBatchStream, ConversionError> {
        let files = parquet_files(&self.uri)
            .await?
            .iter()
            .map(prepare_file)
            .collect::<Result<Vec<_>, _>>()?;
        Box::new(ParquetSource::from_files(files))
            .into_stream()
            .await
    }
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
async fn parquet_files(source_uri: &str) -> Result<Vec<HuggingFaceParquetFile>, ConversionError> {
    let location = HuggingFaceLocation::parse(source_uri)?;
    Ok(hf_json::<HuggingFaceParquetResponse>(
        reqwest::Client::new().get(PARQUET_API_URL).query(&location),
    )
    .await?
    .parquet_files
    .into_iter()
    .filter(|file| {
        location
            .config
            .as_ref()
            .is_none_or(|config| config == &file.config)
            && location
                .split
                .as_ref()
                .is_none_or(|split| split == &file.split)
    })
    .collect())
}

fn prepare_file(file: &HuggingFaceParquetFile) -> Result<PreparedParquetFile, ConversionError> {
    if file.size == 0 {
        return Err(ConversionError::Read(format!(
            "Hugging Face parquet file '{}' is missing a size",
            file.url
        )));
    }
    Ok(PreparedParquetFile::new(
        storage::http(&file.url, file.size)?,
        file.size,
    ))
}

#[derive(Deserialize)]
struct HuggingFaceParquetResponse {
    parquet_files: Vec<HuggingFaceParquetFile>,
}

#[derive(Deserialize)]
struct HuggingFaceParquetFile {
    config: String,
    split: String,
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

fn no_parquet_files() -> ConversionError {
    ConversionError::InvalidSource("source contains no Parquet files".to_owned())
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
