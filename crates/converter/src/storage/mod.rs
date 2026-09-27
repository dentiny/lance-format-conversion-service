mod nfs;
mod object;

use std::{ops::Range, pin::Pin, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use lance::io::ObjectStoreParams;
use lance_conversion_core::location::{DatasetLocation, StorageKind};
use tokio::io::AsyncRead;

use crate::ConversionError;

pub(super) type BoxAsyncRead = Box<dyn AsyncRead + Send + Unpin>;
pub(super) type EntryStream =
    Pin<Box<dyn Stream<Item = Result<ObjectMetadata, ConversionError>> + Send>>;
pub(super) type StorageRef = Arc<dyn Storage>;

#[derive(Debug)]
pub(super) struct ObjectMetadata {
    pub(super) path: String,
    pub(super) size: u64,
    pub(super) is_file: bool,
}

/// Storage operations needed by source formats.
///
/// Implementations know about NFS, S3, or HTTP, but never about Parquet or
/// WARC. Format readers consume this interface instead.
#[async_trait]
pub(super) trait Storage: Send + Sync {
    fn path(&self) -> &str;
    fn logical_uri(&self) -> String;
    fn child(&self, path: String) -> StorageRef;
    async fn metadata(&self) -> Result<ObjectMetadata, ConversionError>;
    async fn list(&self) -> Result<EntryStream, ConversionError>;
    async fn reader(&self) -> Result<BoxAsyncRead, ConversionError>;
    async fn read(&self, range: Range<u64>) -> Result<Bytes, ConversionError>;
}

pub(super) fn open(uri: &str) -> Result<StorageRef, ConversionError> {
    let location = DatasetLocation::parse_location(uri)
        .map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
    match location.storage_kind() {
        StorageKind::Nfs => nfs::open(location.uri()),
        StorageKind::S3 => object::open_s3(location.uri()),
        StorageKind::HuggingFace => Err(ConversionError::InvalidSource(
            "Hugging Face datasets must be resolved to Parquet files first".to_owned(),
        )),
    }
}

pub(super) fn http(uri: &str, size: u64) -> Result<StorageRef, ConversionError> {
    object::open_http(uri, size)
}

/// Returns Lance writer options for a destination storage backend.
pub(super) fn lance_storage_options(
    uri: &str,
) -> Result<Option<ObjectStoreParams>, ConversionError> {
    let location = DatasetLocation::parse_location(uri)
        .map_err(|error| ConversionError::InvalidDestination(error.to_string()))?;
    match location.storage_kind() {
        StorageKind::Nfs | StorageKind::S3 => Ok(None),
        StorageKind::HuggingFace => Err(ConversionError::Unsupported(
            "Hugging Face is not a writable Lance destination".to_owned(),
        )),
    }
}
