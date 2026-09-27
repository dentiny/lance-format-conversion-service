use std::{ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture, stream};
use lance::deps::datafusion::{
    error::DataFusionError,
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
};
use parquet::{
    arrow::{
        ParquetRecordBatchStreamBuilder, arrow_reader::ArrowReaderOptions,
        async_reader::AsyncFileReader,
    },
    errors::{ParquetError, Result as ParquetResult},
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};

use super::Source;
use crate::{
    ConversionError,
    storage::{self, StorageRef},
    validation,
};

pub(super) struct ParquetSource {
    files: Vec<PreparedParquetFile>,
}

impl ParquetSource {
    pub(super) async fn open(uri: &str) -> Result<Self, ConversionError> {
        Ok(Self::from_files(list_files(storage::open(uri)?).await?))
    }

    pub(super) fn from_files(files: Vec<PreparedParquetFile>) -> Self {
        Self { files }
    }

    pub(super) async fn schema(uri: &str) -> Result<arrow::datatypes::SchemaRef, ConversionError> {
        let files = list_files(storage::open(uri)?).await?;
        let file = files.first().ok_or_else(no_parquet_files)?;
        let schema = file.read_schema().await?;
        validation::validate_schema(schema.fields())?;
        Ok(schema)
    }
}

#[async_trait]
impl Source for ParquetSource {
    async fn into_stream(self: Box<Self>) -> Result<SendableRecordBatchStream, ConversionError> {
        let mut files = self.files;
        if files.is_empty() {
            return Err(no_parquet_files());
        }
        files.sort_unstable_by(|left, right| left.location.cmp(&right.location));

        let mut schema: Option<arrow::datatypes::SchemaRef> = None;
        for file in &files {
            let file_schema = file.read_schema().await?;
            if let Some(expected) = &schema {
                if expected.as_ref() != file_schema.as_ref() {
                    return Err(ConversionError::Validation(format!(
                        "Parquet file '{}' has schema {file_schema:?}, which does not match the source schema {expected:?}",
                        file.location
                    )));
                }
            } else {
                schema = Some(file_schema);
            }
        }
        let schema = schema.expect("non-empty Parquet file list has a schema");
        validation::validate_schema(schema.fields())?;
        let batches = stream::iter(files)
            .then(PreparedParquetFile::into_stream)
            .map_err(|error| DataFusionError::Execution(error.to_string()))
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
    }
}

async fn list_files(storage: StorageRef) -> Result<Vec<PreparedParquetFile>, ConversionError> {
    if is_parquet_path(storage.path()) {
        let metadata = storage.metadata().await?;
        if !metadata.is_file {
            return Err(ConversionError::InvalidSource(
                "Parquet source is not a file".to_owned(),
            ));
        }
        return Ok(vec![PreparedParquetFile::new(storage, metadata.size)]);
    }

    let mut entries = storage.list().await?;
    let mut files = Vec::new();
    while let Some(entry) = entries.try_next().await? {
        if entry.is_file && is_parquet_path(&entry.path) {
            files.push(PreparedParquetFile::new(
                storage.child(entry.path),
                entry.size,
            ));
        }
    }
    Ok(files)
}

pub(super) struct PreparedParquetFile {
    storage: StorageRef,
    size: u64,
    location: String,
}

impl PreparedParquetFile {
    pub(super) fn new(storage: StorageRef, size: u64) -> Self {
        let location = storage.logical_uri();
        Self {
            storage,
            size,
            location,
        }
    }

    async fn read_schema(&self) -> Result<arrow::datatypes::SchemaRef, ConversionError> {
        let builder = ParquetRecordBatchStreamBuilder::new(ParquetReader::new(
            Arc::clone(&self.storage),
            self.size,
        ))
        .await
        .map_err(|error| ConversionError::Read(error.to_string()))?;
        Ok(Arc::clone(builder.schema()))
    }

    async fn into_stream(self) -> Result<SendableRecordBatchStream, ConversionError> {
        let builder =
            ParquetRecordBatchStreamBuilder::new(ParquetReader::new(self.storage, self.size))
                .await
                .map_err(|error| ConversionError::Read(error.to_string()))?;
        let schema = Arc::clone(builder.schema());
        let batches = builder
            .build()
            .map_err(|error| ConversionError::Read(error.to_string()))?
            .map_err(|error| DataFusionError::Execution(error.to_string()));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
    }
}

struct ParquetReader {
    storage: StorageRef,
    size: u64,
}

impl ParquetReader {
    fn new(storage: StorageRef, size: u64) -> Self {
        Self { storage, size }
    }
}

impl AsyncFileReader for ParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        async move {
            self.storage
                .read(range)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        async move {
            let size = self.size;
            let metadata = ParquetMetaDataReader::new()
                .with_metadata_options(options.map(|value| value.metadata_options().clone()))
                .load_and_finish(self, size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

fn is_parquet_path(path: &str) -> bool {
    std::path::Path::new(path)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("parquet"))
}

fn no_parquet_files() -> ConversionError {
    ConversionError::InvalidSource("source contains no Parquet files".to_owned())
}
