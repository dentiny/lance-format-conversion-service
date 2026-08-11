use std::{path::Path, sync::Arc};

use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use lance_conversion_core::location::{DatasetLocation, LocationKind};
use object_store::{ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, path::Path as ObjectPath};
use reqwest::Url;

use super::PreparedSource;
use crate::ConversionError;

pub(super) async fn prepare(
    context: &SessionContext,
    source: &DatasetLocation,
) -> Result<PreparedSource, ConversionError> {
    match source.kind() {
        LocationKind::Nfs => Ok(PreparedSource {
            parquet_uri: source.uri().to_owned(),
            temporary_directory: None,
        }),
        LocationKind::S3 => prepare_s3(context, source.uri()).await,
        LocationKind::HuggingFace => Err(ConversionError::InvalidSource(
            "expected a directory-based source".to_owned(),
        )),
    }
}

async fn prepare_s3(
    context: &SessionContext,
    source_uri: &str,
) -> Result<PreparedSource, ConversionError> {
    let url = Url::parse(source_uri)
        .map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
    let bucket = url
        .host_str()
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| ConversionError::InvalidSource("S3 bucket is missing".to_owned()))?;
    let store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|error| ConversionError::Read(error.to_string()))?,
    );
    let root = Url::parse(&format!("s3://{bucket}"))
        .map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
    context.register_object_store(&root, Arc::clone(&store));
    Ok(PreparedSource {
        parquet_uri: source_uri.to_owned(),
        temporary_directory: None,
    })
}

pub(super) async fn delete(source: &DatasetLocation) -> Result<(), ConversionError> {
    match source.kind() {
        LocationKind::Nfs => delete_nfs(source.uri()).await,
        LocationKind::S3 => delete_s3_prefix(source.uri()).await,
        LocationKind::HuggingFace => Err(ConversionError::InvalidSource(
            "expected a directory-based source".to_owned(),
        )),
    }
}

async fn delete_nfs(source_uri: &str) -> Result<(), ConversionError> {
    let path = Path::new(source_uri);
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|error| ConversionError::Delete(error.to_string()))?;
    if metadata.is_dir() {
        tokio::fs::remove_dir_all(path)
            .await
            .map_err(|error| ConversionError::Delete(error.to_string()))
    } else {
        tokio::fs::remove_file(path)
            .await
            .map_err(|error| ConversionError::Delete(error.to_string()))
    }
}

async fn delete_s3_prefix(source_uri: &str) -> Result<(), ConversionError> {
    let url = Url::parse(source_uri)
        .map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
    let bucket = url
        .host_str()
        .ok_or_else(|| ConversionError::InvalidSource("S3 bucket is missing".to_owned()))?;
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()
        .map_err(|error| ConversionError::Delete(error.to_string()))?;
    let prefix = ObjectPath::from(url.path().trim_start_matches('/'));
    let objects = store
        .list(Some(&prefix))
        .map_ok(|metadata| metadata.location)
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| ConversionError::Delete(error.to_string()))?;
    for object in objects {
        store
            .delete(&object)
            .await
            .map_err(|error| ConversionError::Delete(error.to_string()))?;
    }
    Ok(())
}
