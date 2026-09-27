use std::{
    collections::VecDeque,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

use super::{BoxAsyncRead, EntryStream, ObjectMetadata, Storage, StorageRef};
use crate::ConversionError;

#[derive(Clone)]
struct NfsStorage {
    path: PathBuf,
}

pub(super) fn open(uri: &str) -> Result<StorageRef, ConversionError> {
    if uri.is_empty() {
        return Err(ConversionError::InvalidSource(
            "NFS path must not be empty".to_owned(),
        ));
    }
    Ok(Arc::new(NfsStorage {
        path: PathBuf::from(uri),
    }))
}

#[async_trait]
impl Storage for NfsStorage {
    fn path(&self) -> &str {
        self.path.to_str().unwrap_or("")
    }

    fn logical_uri(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }

    fn child(&self, path: String) -> StorageRef {
        Arc::new(Self {
            path: PathBuf::from(path),
        })
    }

    async fn metadata(&self) -> Result<ObjectMetadata, ConversionError> {
        let metadata = tokio::fs::metadata(&self.path).await.map_err(read_error)?;
        Ok(ObjectMetadata {
            path: self.logical_uri(),
            size: metadata.len(),
            is_file: metadata.is_file(),
        })
    }

    async fn list(&self) -> Result<EntryStream, ConversionError> {
        let entries = list_recursive(&self.path).await?;
        Ok(Box::pin(stream::iter(entries.into_iter().map(Ok))))
    }

    async fn reader(&self) -> Result<BoxAsyncRead, ConversionError> {
        let file = tokio::fs::File::open(&self.path)
            .await
            .map_err(read_error)?;
        Ok(Box::new(file))
    }

    async fn read(&self, range: Range<u64>) -> Result<Bytes, ConversionError> {
        let length = usize::try_from(range.end.saturating_sub(range.start))
            .map_err(|error| ConversionError::Read(error.to_string()))?;
        let mut file = tokio::fs::File::open(&self.path)
            .await
            .map_err(read_error)?;
        file.seek(SeekFrom::Start(range.start))
            .await
            .map_err(read_error)?;
        let mut bytes = vec![0; length];
        file.read_exact(&mut bytes).await.map_err(read_error)?;
        Ok(Bytes::from(bytes))
    }
}

async fn list_recursive(root: &Path) -> Result<Vec<ObjectMetadata>, ConversionError> {
    let metadata = tokio::fs::metadata(root).await.map_err(read_error)?;
    if !metadata.is_dir() {
        return Err(ConversionError::InvalidSource(
            "source is not a directory".to_owned(),
        ));
    }

    let mut directories = VecDeque::from([root.to_owned()]);
    let mut entries = Vec::new();
    while let Some(directory) = directories.pop_front() {
        let mut children = tokio::fs::read_dir(directory).await.map_err(read_error)?;
        while let Some(child) = children.next_entry().await.map_err(read_error)? {
            let file_type = child.file_type().await.map_err(read_error)?;
            if file_type.is_dir() {
                directories.push_back(child.path());
            } else if file_type.is_file() {
                let metadata = child.metadata().await.map_err(read_error)?;
                entries.push(ObjectMetadata {
                    path: child.path().to_string_lossy().into_owned(),
                    size: metadata.len(),
                    is_file: true,
                });
            }
        }
    }
    Ok(entries)
}

fn read_error(error: impl std::fmt::Display) -> ConversionError {
    ConversionError::Read(error.to_string())
}
