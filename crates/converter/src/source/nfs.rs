use std::path::{Path, PathBuf};

use crate::ConversionError;

use super::{PreparedParquetFile, PreparedSource};

pub(super) async fn prepare(source_uri: &str) -> Result<PreparedSource, ConversionError> {
    let source = PathBuf::from(source_uri);
    let metadata = tokio::fs::metadata(&source)
        .await
        .map_err(|error| read_error(&error))?;
    let mut directories = Vec::new();
    let mut parquet_files = Vec::new();

    if metadata.is_dir() {
        directories.push(source);
    } else if is_parquet(&source) {
        parquet_files.push(PreparedParquetFile::local(source)?);
    }

    while let Some(directory) = directories.pop() {
        let mut entries = tokio::fs::read_dir(directory)
            .await
            .map_err(|error| read_error(&error))?;
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|error| read_error(&error))?
        {
            let file_type = entry
                .file_type()
                .await
                .map_err(|error| read_error(&error))?;
            if file_type.is_dir() {
                directories.push(entry.path());
            } else if file_type.is_file() && is_parquet(&entry.path()) {
                parquet_files.push(PreparedParquetFile::local(entry.path())?);
            }
        }
    }

    PreparedSource::new(parquet_files).await
}

fn is_parquet(path: &Path) -> bool {
    path.extension()
        .is_some_and(|extension| extension == "parquet")
}

fn read_error(error: &std::io::Error) -> ConversionError {
    ConversionError::Read(error.to_string())
}
