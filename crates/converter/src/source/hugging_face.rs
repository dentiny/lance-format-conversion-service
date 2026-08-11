use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use lance_conversion_core::location::DatasetLocation;
use object_store::{ClientOptions, ObjectStore, http::HttpBuilder};
use reqwest::{
    Url,
    header::{AUTHORIZATION, HeaderMap, HeaderValue},
};
use serde::Deserialize;

use super::{PreparedSource, SourceDataset};
use crate::ConversionError;

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
    fn copy_only(&self) -> bool {
        true
    }

    async fn prepare(&self, context: &SessionContext) -> Result<PreparedSource, ConversionError> {
        prepare(context, self.location.uri()).await
    }

    async fn delete(&self) -> Result<(), ConversionError> {
        Err(ConversionError::InvalidSource(
            "Hugging Face datasets are copy-only".to_owned(),
        ))
    }
}

async fn prepare(
    context: &SessionContext,
    source_uri: &str,
) -> Result<PreparedSource, ConversionError> {
    let parsed = HuggingFaceLocation::parse(source_uri)?;
    let client = reqwest::Client::new();
    let mut query = vec![
        ("dataset", format!("{}/{}", parsed.owner, parsed.name)),
        ("revision", parsed.revision),
    ];
    if let Some(config) = parsed.config {
        query.push(("config", config));
    }
    if let Some(split) = parsed.split {
        query.push(("split", split));
    }
    let mut request = client
        .get("https://datasets-server.huggingface.co/parquet")
        .query(&query);
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
    if response.parquet_files.is_empty() {
        return Err(ConversionError::InvalidSource(
            "Hugging Face dataset returned no Parquet files".to_owned(),
        ));
    }

    let parquet_locations = response
        .parquet_files
        .into_iter()
        .map(|file| file.url)
        .collect::<Vec<_>>();
    register_http_stores(context, &parquet_locations)?;
    Ok(PreparedSource { parquet_locations })
}

fn register_http_stores(
    context: &SessionContext,
    parquet_locations: &[String],
) -> Result<(), ConversionError> {
    let mut origins = HashSet::new();
    for location in parquet_locations {
        let url = Url::parse(location)
            .map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
        let host = url
            .host_str()
            .ok_or_else(|| ConversionError::InvalidSource("HTTP host is missing".to_owned()))?;
        let origin = match url.port() {
            Some(port) => format!("{}://{host}:{port}", url.scheme()),
            None => format!("{}://{host}", url.scheme()),
        };
        if !origins.insert(origin.clone()) {
            continue;
        }
        let root = Url::parse(&origin)
            .map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
        let store: Arc<dyn ObjectStore> = Arc::new(
            HttpBuilder::new()
                .with_url(origin)
                .with_client_options(http_client_options()?)
                .build()
                .map_err(|error| ConversionError::Read(error.to_string()))?,
        );
        context.register_object_store(&root, store);
    }
    Ok(())
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

struct HuggingFaceLocation {
    owner: String,
    name: String,
    revision: String,
    config: Option<String>,
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
        let path = url.path().trim_matches('/');
        let (owner, name_and_revision) = path.split_once('/').ok_or_else(|| {
            ConversionError::InvalidSource("Hugging Face owner or dataset is missing".to_owned())
        })?;
        let (name, revision) = name_and_revision
            .rsplit_once('@')
            .map_or((name_and_revision, "main"), |(name, revision)| {
                (name, revision)
            });
        if owner.is_empty() || name.is_empty() || revision.is_empty() {
            return Err(ConversionError::InvalidSource(
                "Hugging Face owner, dataset, and revision must not be empty".to_owned(),
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
            owner: owner.to_owned(),
            name: name.to_owned(),
            revision: revision.to_owned(),
            config,
            split,
        })
    }
}
