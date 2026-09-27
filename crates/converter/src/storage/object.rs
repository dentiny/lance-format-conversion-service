use std::{ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::{
    ClientOptions, ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, http::HttpBuilder,
    path::Path as ObjectPath,
};
use reqwest::Url;
use tokio_util::io::StreamReader;

use super::{BoxAsyncRead, EntryStream, ObjectMetadata, Storage, StorageRef};
use crate::ConversionError;

#[derive(Clone)]
struct ObjectStorage {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    logical_root: String,
    known_size: Option<u64>,
}

pub(super) fn open_s3(uri: &str) -> Result<StorageRef, ConversionError> {
    let url = Url::parse(uri).map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
    let bucket = url
        .host_str()
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| ConversionError::InvalidSource("S3 bucket is missing".to_owned()))?;
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()
        .map_err(read_error)?;
    Ok(Arc::new(ObjectStorage {
        store: Arc::new(store),
        path: ObjectPath::from(url.path().trim_matches('/')),
        logical_root: format!("s3://{bucket}"),
        known_size: None,
    }))
}

pub(super) fn open_http(uri: &str, size: u64) -> Result<StorageRef, ConversionError> {
    Url::parse(uri).map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
    let store = HttpBuilder::new()
        .with_url(uri)
        .with_client_options(ClientOptions::new())
        .build()
        .map_err(read_error)?;
    Ok(Arc::new(ObjectStorage {
        store: Arc::new(store),
        path: ObjectPath::default(),
        logical_root: uri.to_owned(),
        known_size: Some(size),
    }))
}

#[async_trait]
impl Storage for ObjectStorage {
    fn path(&self) -> &str {
        self.path.as_ref()
    }

    fn logical_uri(&self) -> String {
        if self.path.as_ref().is_empty() {
            self.logical_root.clone()
        } else {
            format!("{}/{}", self.logical_root, self.path)
        }
    }

    fn child(&self, path: String) -> StorageRef {
        Arc::new(Self {
            store: Arc::clone(&self.store),
            path: ObjectPath::from(path),
            logical_root: self.logical_root.clone(),
            known_size: None,
        })
    }

    async fn metadata(&self) -> Result<ObjectMetadata, ConversionError> {
        if let Some(size) = self.known_size {
            return Ok(ObjectMetadata {
                path: self.path.to_string(),
                size,
                is_file: true,
            });
        }
        let metadata = self.store.head(&self.path).await.map_err(read_error)?;
        Ok(ObjectMetadata {
            path: metadata.location.to_string(),
            size: metadata.size,
            is_file: true,
        })
    }

    async fn list(&self) -> Result<EntryStream, ConversionError> {
        if self.known_size.is_some() {
            return Err(ConversionError::Unsupported(
                "HTTP objects cannot be listed".to_owned(),
            ));
        }
        let prefix = (!self.path.as_ref().is_empty())
            .then(|| ObjectPath::from(format!("{}/", self.path.as_ref().trim_end_matches('/'))));
        let entries = self.store.list(prefix.as_ref()).map(|entry| {
            entry
                .map(|metadata| ObjectMetadata {
                    path: metadata.location.to_string(),
                    size: metadata.size,
                    is_file: true,
                })
                .map_err(read_error)
        });
        Ok(Box::pin(entries))
    }

    async fn reader(&self) -> Result<BoxAsyncRead, ConversionError> {
        let stream = self
            .store
            .get(&self.path)
            .await
            .map_err(read_error)?
            .into_stream()
            .map_err(|error| std::io::Error::other(error.to_string()));
        Ok(Box::new(StreamReader::new(stream)))
    }

    async fn read(&self, range: Range<u64>) -> Result<Bytes, ConversionError> {
        self.store
            .get_range(&self.path, range)
            .await
            .map_err(read_error)
    }
}

fn read_error(error: impl std::fmt::Display) -> ConversionError {
    ConversionError::Read(error.to_string())
}
