mod directory;
mod hugging_face;
mod nfs;
mod s3;

use async_trait::async_trait;
use datafusion::{
    dataframe::DataFrame,
    prelude::{ParquetReadOptions, SessionContext},
};
use lance_conversion_core::location::{DatasetLocation, LocationKind};

use crate::ConversionError;

pub(crate) struct PreparedSource {
    /// Fully resolved Parquet file locations, never directories or prefixes.
    pub(crate) parquet_files: Vec<String>,
}

impl PreparedSource {
    fn new(mut parquet_files: Vec<String>) -> Result<Self, ConversionError> {
        if parquet_files.is_empty() {
            return Err(ConversionError::InvalidSource(
                "source contains no Parquet files".to_owned(),
            ));
        }
        parquet_files.sort_unstable();
        Ok(Self { parquet_files })
    }
}

/// Provides a uniform interface for preparing and deleting source datasets.
///
/// Implementations translate their native location into a Parquet URI that
/// `DataFusion` can read and own source-specific deletion behavior for move jobs.
#[async_trait]
pub(crate) trait SourceDataset: Send + Sync {
    /// Returns whether this source supports copy jobs only.
    ///
    /// Sources that return `true` must not have [`Self::delete`] called.
    fn copy_only(&self) -> bool;

    /// Makes the source's Parquet files available to the conversion reader.
    ///
    /// Resolves the source dataset into individual Parquet file locations.
    ///
    /// Implementations list directory or prefix sources and register any
    /// object stores required to stream the returned files through `context`.
    ///
    /// # Errors
    ///
    /// Returns an error if the location is invalid or its Parquet files cannot
    /// be made available.
    async fn prepare(&self, context: &SessionContext) -> Result<PreparedSource, ConversionError>;

    /// Deletes the source after a move conversion commits successfully.
    ///
    /// # Errors
    ///
    /// Returns an error if any source file or object cannot be deleted.
    async fn delete(&self) -> Result<(), ConversionError>;
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

/// Prepares a source and reads all of its Parquet files as one `DataFusion` frame.
pub(crate) async fn prepare_dataframe(
    source: &dyn SourceDataset,
    context: &SessionContext,
) -> Result<DataFrame, ConversionError> {
    let prepared = source.prepare(context).await?;
    let mut locations = prepared.parquet_files.into_iter();
    let first_location = locations.next().ok_or_else(|| {
        ConversionError::InvalidSource("source contains no Parquet locations".to_owned())
    })?;
    let mut dataframe = context
        .read_parquet(first_location, ParquetReadOptions::default())
        .await
        .map_err(|error| ConversionError::Read(error.to_string()))?;
    for location in locations {
        let next = context
            .read_parquet(location, ParquetReadOptions::default())
            .await
            .map_err(|error| ConversionError::Read(error.to_string()))?;
        dataframe = dataframe
            .union(next)
            .map_err(|error| ConversionError::Read(error.to_string()))?;
    }
    Ok(dataframe)
}
