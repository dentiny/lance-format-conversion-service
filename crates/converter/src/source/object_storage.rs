use std::sync::Arc;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, future};
use lance::io::ObjectStoreParams;
use lance_conversion_core::location::DatasetLocation;
use object_store::{ObjectStore, aws::AmazonS3, aws::AmazonS3Builder, path::Path as ObjectPath};
use reqwest::Url;

use super::{PreparedParquetFile, StorageBackend};
use crate::ConversionError;

pub(super) struct ObjectStorageBackend {
    location: DatasetLocation,
}

impl ObjectStorageBackend {
    pub(super) const fn new(location: DatasetLocation) -> Self {
        Self { location }
    }
}

#[async_trait]
impl StorageBackend for ObjectStorageBackend {
    async fn list_files(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<PreparedParquetFile>, ConversionError> {
        list_parquet_files(self.location.uri(), limit).await
    }

    fn lance_storage_options(&self) -> Result<Option<ObjectStoreParams>, ConversionError> {
        Ok(None)
    }
}

async fn list_parquet_files(
    source_uri: &str,
    limit: Option<usize>,
) -> Result<Vec<PreparedParquetFile>, ConversionError> {
    let (url, bucket) = parse_location(source_uri)?;
    let store: Arc<dyn ObjectStore> =
        Arc::new(build_store(&bucket).map_err(|error| read_error(&error))?);

    store
        .list(Some(&directory_prefix(&url)))
        .try_filter_map(|metadata| {
            let store = Arc::clone(&store);
            let parquet = metadata.location.as_ref().ends_with(".parquet").then(|| {
                let location = format!("s3://{bucket}/{}", metadata.location);
                PreparedParquetFile::object(store, metadata.location, metadata.size, location)
            });
            future::ready(Ok(parquet))
        })
        .take(limit.unwrap_or(usize::MAX))
        .try_collect()
        .await
        .map_err(|error| read_error(&error))
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

#[cfg(test)]
mod tests {
    use super::{directory_prefix, parse_location};
    use crate::ConversionError;

    #[test]
    fn parses_bucket_and_prefix() {
        let (url, bucket) = parse_location("s3://lance-test-bucket/mint-1t-html").unwrap();
        assert_eq!(bucket, "lance-test-bucket");
        assert_eq!(directory_prefix(&url).as_ref(), "mint-1t-html");
    }

    #[test]
    fn rejects_s3_uri_without_bucket() {
        assert!(matches!(
            parse_location("s3:///missing-bucket"),
            Err(ConversionError::InvalidSource(_))
        ));
    }
}
