use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use futures::TryStreamExt;
use serde::Deserialize;
use tokio::io::AsyncWriteExt;
use url::Url;

use super::PreparedSource;
use crate::ConversionError;

pub(super) async fn prepare(source_uri: &str) -> Result<PreparedSource, ConversionError> {
    let parsed = HuggingFaceSource::parse(source_uri)?;
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

    let temporary_directory = create_temporary_directory().await?;
    let mut source_bytes = 0_u64;
    for (index, parquet_file) in response.parquet_files.into_iter().enumerate() {
        let mut request = client.get(parquet_file.url);
        if let Ok(token) = std::env::var("HF_TOKEN") {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|error| ConversionError::Read(error.to_string()))?;
        let mut destination =
            tokio::fs::File::create(temporary_directory.join(format!("part-{index}.parquet")))
                .await
                .map_err(|error| ConversionError::Read(error.to_string()))?;
        let mut chunks = response.bytes_stream();
        while let Some(chunk) = chunks
            .try_next()
            .await
            .map_err(|error| ConversionError::Read(error.to_string()))?
        {
            let chunk_size = u64::try_from(chunk.len())
                .map_err(|error| ConversionError::Read(error.to_string()))?;
            source_bytes = source_bytes
                .checked_add(chunk_size)
                .ok_or_else(|| ConversionError::Read("source byte count overflow".to_owned()))?;
            destination
                .write_all(&chunk)
                .await
                .map_err(|error| ConversionError::Read(error.to_string()))?;
        }
        destination
            .flush()
            .await
            .map_err(|error| ConversionError::Read(error.to_string()))?;
    }
    Ok(PreparedSource {
        parquet_uri: temporary_directory.to_string_lossy().into_owned(),
        source_bytes,
        temporary_directory: Some(temporary_directory),
    })
}

async fn create_temporary_directory() -> Result<PathBuf, ConversionError> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ConversionError::Read(error.to_string()))?
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "lance-converter-{}-{timestamp}",
        std::process::id()
    ));
    tokio::fs::create_dir(&path)
        .await
        .map_err(|error| ConversionError::Read(error.to_string()))?;
    Ok(path)
}

#[derive(Deserialize)]
struct HuggingFaceParquetResponse {
    parquet_files: Vec<HuggingFaceParquetFile>,
}

#[derive(Deserialize)]
struct HuggingFaceParquetFile {
    url: String,
}

struct HuggingFaceSource {
    owner: String,
    name: String,
    revision: String,
    config: Option<String>,
    split: Option<String>,
}

impl HuggingFaceSource {
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
