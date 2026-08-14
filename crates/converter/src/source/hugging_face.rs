use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use lance_conversion_core::location::DatasetLocation;
use object_store::{
    ClientOptions, ObjectStore, ObjectStoreExt, http::HttpBuilder, path::Path as ObjectPath,
};
use reqwest::{
    Url,
    header::{AUTHORIZATION, HeaderMap, HeaderValue},
};
use serde::{Deserialize, Serialize};

use super::{PreparedParquetFile, PreparedSource, SourceDataset};
use crate::ConversionError;

const PARQUET_API_URL: &str = "https://datasets-server.huggingface.co/parquet";

pub(super) struct HuggingFaceDataset {
    location: DatasetLocation,
}

impl HuggingFaceDataset {
    pub(super) const fn new(location: DatasetLocation) -> Self {
        Self { location }
    }
}

#[async_trait]
impl SourceDataset for HuggingFaceDataset {
    async fn prepare(&self) -> Result<PreparedSource, ConversionError> {
        prepare(self.location.uri()).await
    }
}

async fn prepare(source_uri: &str) -> Result<PreparedSource, ConversionError> {
    let parsed = HuggingFaceLocation::parse(source_uri)?;
    let client = reqwest::Client::new();
    let mut request = client.get(PARQUET_API_URL).query(&parsed);
    if let Ok(token) = std::env::var("HF_TOKEN") {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|error| ConversionError::Read(error.to_string()))?
        .json::<HuggingFaceParquetResponse>()
        .await
        .map_err(|error| ConversionError::Read(error.to_string()))?;
    prepare_http_files(response.parquet_files).await
}

async fn prepare_http_files(
    parquet_files: Vec<HuggingFaceParquetFile>,
) -> Result<PreparedSource, ConversionError> {
    let mut stores = HashMap::<String, Arc<dyn ObjectStore>>::new();
    let mut prepared = Vec::with_capacity(parquet_files.len());
    let client_options = http_client_options()?;
    for file in parquet_files {
        let url = Url::parse(&file.url)
            .map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
        let origin = url.origin().ascii_serialization();
        let store = if let Some(store) = stores.get(&origin) {
            Arc::clone(store)
        } else {
            let store: Arc<dyn ObjectStore> = Arc::new(
                HttpBuilder::new()
                    .with_url(&origin)
                    .with_client_options(client_options.clone())
                    .build()
                    .map_err(|error| ConversionError::Read(error.to_string()))?,
            );
            stores.insert(origin, Arc::clone(&store));
            store
        };
        let path = ObjectPath::from_url_path(url.path())
            .map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
        let metadata = store
            .head(&path)
            .await
            .map_err(|error| ConversionError::Read(error.to_string()))?;
        prepared.push(PreparedParquetFile::object(
            store,
            path,
            metadata.size,
            file.url,
        ));
    }
    PreparedSource::new(prepared).await
}

fn http_client_options() -> Result<ClientOptions, ConversionError> {
    let Ok(token) = std::env::var("HF_TOKEN") else {
        return Ok(ClientOptions::new());
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|error| ConversionError::InvalidSource(error.to_string()))?,
    );
    Ok(ClientOptions::new().with_default_headers(headers))
}

#[derive(Deserialize)]
struct HuggingFaceParquetResponse {
    parquet_files: Vec<HuggingFaceParquetFile>,
}

#[derive(Deserialize)]
struct HuggingFaceParquetFile {
    url: String,
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
            return Err(ConversionError::InvalidSource(
                "expected hf://datasets/owner/name@revision".to_owned(),
            ));
        }
        let dataset_and_revision = url.path().trim_matches('/');
        let (dataset, revision) = dataset_and_revision
            .rsplit_once('@')
            .unwrap_or((dataset_and_revision, "main"));
        if dataset.split('/').filter(|part| !part.is_empty()).count() != 2 || revision.is_empty() {
            return Err(ConversionError::InvalidSource(
                "expected hf://datasets/owner/name@revision".to_owned(),
            ));
        }
        let mut config = None;
        let mut split = None;
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "config" => config = Some(value.into_owned()),
                "split" => split = Some(value.into_owned()),
                _ => {}
            }
        }
        Ok(Self {
            dataset: dataset.to_owned(),
            revision: revision.to_owned(),
            config,
            split,
        })
    }
}
