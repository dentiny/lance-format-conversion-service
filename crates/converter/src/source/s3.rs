use std::sync::Arc;

use futures::{TryStreamExt, future};
use object_store::{
    ObjectStore, ObjectStoreExt, aws::AmazonS3, aws::AmazonS3Builder, path::Path as ObjectPath,
};
use reqwest::Url;

use crate::ConversionError;

use super::{PreparedParquetFile, PreparedSource};

pub(super) async fn prepare(source_uri: &str) -> Result<PreparedSource, ConversionError> {
    let (url, bucket) = parse_location(source_uri)?;
    let store: Arc<dyn ObjectStore> =
        Arc::new(build_store(&bucket).map_err(|error| read_error(&error))?);

    let parquet_files = store
        .list(Some(&directory_prefix(&url)))
        .try_filter_map(|metadata| {
            let store = Arc::clone(&store);
            let parquet = metadata.location.as_ref().ends_with(".parquet").then(|| {
                let location = format!("s3://{bucket}/{}", metadata.location);
                PreparedParquetFile::object(store, metadata.location, metadata.size, location)
            });
            future::ready(Ok(parquet))
        })
        .try_collect()
        .await
        .map_err(|error| read_error(&error))?;
    PreparedSource::new(parquet_files).await
}

pub(super) async fn delete(source_uri: &str) -> Result<(), ConversionError> {
    let (url, bucket) = parse_location(source_uri)?;
    let store = build_store(&bucket).map_err(|error| delete_error(&error))?;
    let prefix = directory_prefix(&url);
    let mut objects = store
        .list(Some(&prefix))
        .map_ok(|metadata| metadata.location);

    while let Some(object) = objects
        .try_next()
        .await
        .map_err(|error| delete_error(&error))?
    {
        store
            .delete(&object)
            .await
            .map_err(|error| delete_error(&error))?;
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

fn read_error(error: &object_store::Error) -> ConversionError {
    ConversionError::Read(error.to_string())
}

fn delete_error(error: &object_store::Error) -> ConversionError {
    ConversionError::Delete(error.to_string())
}
