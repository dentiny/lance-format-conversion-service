use std::path::{Path, PathBuf};

use async_trait::async_trait;
use lance::io::ObjectStoreParams;
use lance_conversion_core::location::DatasetLocation;

use super::{PreparedParquetFile, StorageBackend};
use crate::ConversionError;

pub(super) struct NfsBackend {
    location: DatasetLocation,
}

impl NfsBackend {
    pub(super) const fn new(location: DatasetLocation) -> Self {
        Self { location }
    }
}

#[async_trait]
impl StorageBackend for NfsBackend {
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
    let source = PathBuf::from(source_uri);
    let metadata = tokio::fs::metadata(&source)
        .await
        .map_err(|error| read_error(&error))?;
    if !metadata.is_dir() {
        return Err(ConversionError::InvalidSource(
            "NFS source must be a directory of Parquet files".to_owned(),
        ));
    }

    let mut parquet_files = limit.map_or_else(Vec::new, Vec::with_capacity);
    let mut entries = tokio::fs::read_dir(&source)
        .await
        .map_err(|error| read_error(&error))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|error| read_error(&error))?
    {
        if reached_limit(parquet_files.len(), limit) {
            break;
        }
        let path = entry.path();
        let file_type = entry
            .file_type()
            .await
            .map_err(|error| read_error(&error))?;
        if file_type.is_file() && is_parquet(&path) {
            parquet_files.push(PreparedParquetFile::local(path)?);
        }
    }
    Ok(parquet_files)
}

fn reached_limit(count: usize, limit: Option<usize>) -> bool {
    limit.is_some_and(|limit| count >= limit)
}

fn is_parquet(path: &Path) -> bool {
    path.extension()
        .is_some_and(|extension| extension == "parquet")
}

fn read_error(error: &std::io::Error) -> ConversionError {
    ConversionError::Read(error.to_string())
}

#[cfg(test)]
mod tests {
    use lance_conversion_core::location::DatasetLocation;
    use lance_test_support::{get_test_schema, write_test_parquet};
    use tempfile::TempDir;

    use super::{NfsBackend, StorageBackend};

    #[tokio::test]
    async fn reads_schema_from_first_local_parquet_file() {
        let temp_dir = TempDir::new().unwrap();
        write_test_parquet(temp_dir.path()).await;

        let backend = NfsBackend::new(
            DatasetLocation::parse_location(temp_dir.path().to_str().unwrap()).unwrap(),
        );
        let inspected = backend.get_schema().await.unwrap();
        let expected = get_test_schema();
        assert_eq!(inspected.fields().len(), expected.fields().len());
        for (inspected_field, expected_field) in inspected.fields().iter().zip(expected.fields()) {
            assert_eq!(inspected_field.name(), expected_field.name());
            assert_eq!(inspected_field.data_type(), expected_field.data_type());
            assert_eq!(inspected_field.is_nullable(), expected_field.is_nullable());
        }
    }
}
