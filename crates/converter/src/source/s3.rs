use std::sync::Arc;

use datafusion::prelude::SessionContext;
use futures::{TryStreamExt, future};
use object_store::{
    ObjectStore, ObjectStoreExt, aws::AmazonS3, aws::AmazonS3Builder, path::Path as ObjectPath,
};
use reqwest::Url;

use crate::ConversionError;

use super::PreparedSource;

pub(super) async fn prepare(
    context: &SessionContext,
    source_uri: &str,
) -> Result<PreparedSource, ConversionError> {
    let (url, bucket) = parse_location(source_uri)?;
    let store = Arc::new(build_store(&bucket).map_err(read_error)?);
    let root = Url::parse(&format!("s3://{bucket}"))
        .map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
    context.register_object_store(&root, store.clone());

    let parquet_files = store
        .list(Some(&directory_prefix(&url)))
        .try_filter_map(|metadata| {
            let parquet = metadata
                .location
                .as_ref()
                .ends_with(".parquet")
                .then(|| format!("s3://{bucket}/{}", metadata.location));
            future::ready(Ok(parquet))
        })
        .try_collect()
        .await
        .map_err(read_error)?;
    PreparedSource::new(parquet_files)
}

pub(super) async fn delete(source_uri: &str) -> Result<(), ConversionError> {
    let (url, bucket) = parse_location(source_uri)?;
    let store = build_store(&bucket).map_err(delete_error)?;
    let prefix = directory_prefix(&url);
    let mut objects = store
        .list(Some(&prefix))
        .map_ok(|metadata| metadata.location);

    while let Some(object) = objects.try_next().await.map_err(delete_error)? {
        store.delete(&object).await.map_err(delete_error)?;
    }
    Ok(())
}

fn parse_location(source_uri: &str) -> Result<(Url, String), ConversionError> {
    let url = Url::parse(source_uri)
        .map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
    let bucket = url
        .host_str()
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| ConversionError::InvalidSource("S3 bucket is missing".to_owned()))?
        .to_owned();
    Ok((url, bucket))
}

fn build_store(bucket: &str) -> object_store::Result<AmazonS3> {
    AmazonS3Builder::from_env().with_bucket_name(bucket).build()
}

fn directory_prefix(url: &Url) -> ObjectPath {
    ObjectPath::from(format!("{}/", url.path().trim_matches('/')))
}

// These signatures intentionally match `Result::map_err`, which owns the error.
#[allow(clippy::needless_pass_by_value)]
fn read_error(error: object_store::Error) -> ConversionError {
    ConversionError::Read(error.to_string())
}

#[allow(clippy::needless_pass_by_value)]
fn delete_error(error: object_store::Error) -> ConversionError {
    ConversionError::Delete(error.to_string())
}
