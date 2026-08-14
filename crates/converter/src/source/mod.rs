mod directory;
mod hugging_face;
mod nfs;
mod s3;

use std::{path::PathBuf, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};
use lance::deps::datafusion::{
    error::DataFusionError,
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
};
use lance_conversion_core::location::{DatasetLocation, LocationKind};
use object_store::{ObjectStore, path::Path as ObjectPath};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, async_reader::ParquetObjectReader};

use crate::{ConversionError, validation};

pub(crate) struct PreparedSource {
    files: Vec<PreparedParquetFile>,
    schema: SchemaRef,
}

impl PreparedSource {
    async fn new(mut files: Vec<PreparedParquetFile>) -> Result<Self, ConversionError> {
        if files.is_empty() {
            return Err(ConversionError::InvalidSource(
                "source contains no Parquet files".to_owned(),
            ));
        }
        files.sort_unstable_by(|left, right| left.location().cmp(right.location()));

        let mut schema: Option<SchemaRef> = None;
        for file in &files {
            let file_schema = file.read_schema().await?;
            if let Some(expected) = &schema {
                if !schemas_compatible(expected, &file_schema) {
                    return Err(ConversionError::Validation(format!(
                        "Parquet file '{}' has schema {file_schema:?}, which does not match the source schema {expected:?}",
                        file.location()
                    )));
                }
            } else {
                schema = Some(file_schema);
            }
        }
        let schema = schema.expect("non-empty file list has a schema");
        validation::validate_schema(schema.fields())?;
        Ok(Self { files, schema })
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn into_stream(self) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);
        let batches = stream::iter(self.files)
            .then(PreparedParquetFile::into_stream)
            .map_err(|error| DataFusionError::Execution(error.to_string()))
            .try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(schema, batches))
    }
}

enum PreparedParquetFile {
    Local {
        path: PathBuf,
        location: String,
    },
    Object {
        store: Arc<dyn ObjectStore>,
        path: ObjectPath,
        size: u64,
        location: String,
    },
}

impl PreparedParquetFile {
    fn local(path: PathBuf) -> Result<Self, ConversionError> {
        let location = path
            .to_str()
            .ok_or_else(|| {
                ConversionError::InvalidSource("Parquet path is not valid UTF-8".to_owned())
            })?
            .to_owned();
        Ok(Self::Local { path, location })
    }

    fn object(store: Arc<dyn ObjectStore>, path: ObjectPath, size: u64, location: String) -> Self {
        Self::Object {
            store,
            path,
            size,
            location,
        }
    }

    fn location(&self) -> &str {
        match self {
            Self::Local { location, .. } | Self::Object { location, .. } => location,
        }
    }

    async fn read_schema(&self) -> Result<SchemaRef, ConversionError> {
        match self {
            Self::Local { path, .. } => {
                let file = tokio::fs::File::open(path)
                    .await
                    .map_err(|error| ConversionError::Read(error.to_string()))?;
                let builder = ParquetRecordBatchStreamBuilder::new(file)
                    .await
                    .map_err(|error| ConversionError::Read(error.to_string()))?;
                Ok(Arc::clone(builder.schema()))
            }
            Self::Object {
                store, path, size, ..
            } => {
                let reader =
                    ParquetObjectReader::new(Arc::clone(store), path.clone()).with_file_size(*size);
                let builder = ParquetRecordBatchStreamBuilder::new(reader)
                    .await
                    .map_err(|error| ConversionError::Read(error.to_string()))?;
                Ok(Arc::clone(builder.schema()))
            }
        }
    }

    async fn into_stream(self) -> Result<SendableRecordBatchStream, ConversionError> {
        match self {
            Self::Local { path, .. } => {
                let file = tokio::fs::File::open(path)
                    .await
                    .map_err(|error| ConversionError::Read(error.to_string()))?;
                parquet_stream(file).await
            }
            Self::Object {
                store, path, size, ..
            } => {
                let reader = ParquetObjectReader::new(store, path).with_file_size(size);
                parquet_stream(reader).await
            }
        }
    }
}

async fn parquet_stream<T>(reader: T) -> Result<SendableRecordBatchStream, ConversionError>
where
    T: parquet::arrow::async_reader::AsyncFileReader + Unpin + Send + 'static,
{
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|error| ConversionError::Read(error.to_string()))?;
    let schema = Arc::clone(builder.schema());
    let batches = builder
        .build()
        .map_err(|error| ConversionError::Read(error.to_string()))?
        .map_err(|error| DataFusionError::Execution(error.to_string()));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
}

fn schemas_compatible(expected: &Schema, actual: &Schema) -> bool {
    expected == actual
}

/// Provides a uniform interface for preparing source datasets.
///
/// Implementations resolve native locations into directly readable Parquet
/// files.
#[async_trait]
pub(crate) trait SourceDataset: Send + Sync {
    /// Makes the source's Parquet files available to the conversion reader.
    ///
    /// Resolves the source dataset into individual Parquet file locations.
    ///
    /// Implementations list directory or prefix sources and retain any object
    /// stores required to stream the returned files.
    ///
    /// # Errors
    ///
    /// Returns an error if the location is invalid or its Parquet files cannot
    /// be made available.
    async fn prepare(&self) -> Result<PreparedSource, ConversionError>;
}

impl dyn SourceDataset {
    pub(crate) fn open(source: DatasetLocation) -> Box<Self> {
        match source.kind() {
            LocationKind::Nfs | LocationKind::S3 => {
                Box::new(directory::DirectorySource::new(source))
            }
            LocationKind::HuggingFace => Box::new(hugging_face::HuggingFaceDataset::new(source)),
        }
    }
}

/// Opens, prepares, and validates one source for inspection or conversion.
pub(crate) async fn open_validated_source(
    source_uri: &str,
) -> Result<PreparedSource, ConversionError> {
    let location = DatasetLocation::parse_location(source_uri)
        .map_err(|error| ConversionError::InvalidSource(error.to_string()))?;
    let source = <dyn SourceDataset>::open(location);
    source.prepare().await
}
